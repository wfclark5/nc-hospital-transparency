
import os
import requests
import json
import datetime
import shutil
from bs4 import BeautifulSoup
import pandas as pd
from random import choice
from selenium.common.exceptions import TimeoutException
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse
from time import sleep, time
from random import uniform, randint
import json
from urllib.parse import urlsplit
import urllib3
from glob import glob 
import requests
from pathlib import Path
import io
import boto3
from functools import reduce
import numpy as np
import sys
import datetime
import os
import sys
from glob import glob
import pandas as pd
from dataprep.eda import create_report
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import zipfile
import s3fs

# from prefect import task, Flow

def combine_related(list_of_cols):
	"""Combine related df's"""
	return reduce(lambda x,y: x.combine_first(y), list_of_cols)


def get_patient_type_df(df:pd.DataFrame, main_cols:list, type_dict:dict) -> pd.DataFrame:

	"""Get the payer specific dataframe"""

	# make all columns uppercase 

	df.columns = [x.upper() for x in df.columns]

	# make all columns in main calls uppercase 

	main_cols = [x.upper() for x in main_cols]


	dfs_list = []

	for patient_type, type_cols in type_dict.items():

		payer_dict = {'BCBS': [], 'BCBS MEDICARE': [], 'CIGNA': [], 'CIGNA MEDICARE': [],  'MEDCOST': [], 'AETNA': [], 'AETNA MEDICARE': [], 'UNITED': [], 'UNITED MEDICARE': [], 'DEIDENTIFIED MAXIMUM':[], 'DEIDENTIFIED MINIMUM':[]}

		# for each key in payer_dict insert cols found in list to store in dictionary where key is in list
		for col in type_cols:
			col = col.upper()
			if 'BCBS' in col:
				if 'MEDICARE' in col:
					payer_dict['BCBS MEDICARE'].append(df[col])
				else: 
					payer_dict['BCBS'].append(df[col])
			elif 'CIGNA' in col:
				if 'MEDICARE' in col:
					payer_dict['CIGNA MEDICARE'].append(df[col])
				else:
					payer_dict['CIGNA'].append(df[col])
			elif 'MEDCOST' in col:
				payer_dict['MEDCOST'].append(df[col])
			elif 'AETNA' in col:
				if 'MEDICARE' in col:
					payer_dict['AETNA MEDICARE'].append(df[col])
				else:
					payer_dict['AETNA'].append(df[col])
			elif 'UNITED' in col:
				if 'MEDICARE' in col:
					payer_dict['UNITED MEDICARE'].append(df[col])
				else:
					payer_dict['UNITED'].append(df[col])
			elif 'DE-IDENTIFIED' in col:
				if 'MAXIMUM' in col:
					payer_dict['DEIDENTIFIED MAXIMUM'].append(df[col])
				else:
					payer_dict['DEIDENTIFIED MINIMUM'].append(df[col])
		

		out_df = pd.DataFrame([])
		# combine key values into one column of key 

		out_df['BCBS'] = combine_related(payer_dict['BCBS'])

		out_df['BCBS MEDICARE'] = combine_related(payer_dict['BCBS MEDICARE'])

		out_df['AETNA'] = combine_related(payer_dict['AETNA'])

		out_df['AETNA MEDICARE'] = combine_related(payer_dict['AETNA MEDICARE'])

		out_df['CIGNA'] = combine_related(payer_dict['CIGNA'])

		out_df['CIGNA MEDICARE'] = None

		out_df['UHC'] = combine_related(payer_dict['UNITED'])

		out_df['UHC MEDICARE'] = combine_related(payer_dict['UNITED MEDICARE'])

		out_df['MEDCOST'] = combine_related(payer_dict['MEDCOST'])

		out_df['De-identified Minimum'] = combine_related(payer_dict['DEIDENTIFIED MINIMUM'])

		out_df['De-identified Maximum'] = combine_related(payer_dict['DEIDENTIFIED MAXIMUM'])

		out_df = pd.concat([out_df, df[main_cols]], axis=1).astype(str)

		for col in out_df.columns.values:
			out_df[col] = out_df[col].astype(str).apply(lambda x: x.replace('$', '').replace('PER DIEM', ''))

		out_df['Patient Type'] = patient_type

		dfs_list.append(out_df)
	
	return pd.concat(dfs_list)


# function to convert json to csv

def json_to_csv(json, filepath, lines):
	
	df = pd.read_json(json, lines=lines)

	df.to_csv(filepath, index=False)


def list_duplicates(seq):

	"""List all of the duplicate file names in the download path d"""

	seen = set()
	seen_add = seen.add
	# adds all elements it doesn't know yet to seen and all other to seen_twice
	seen_twice = set( x for x in seq if x in seen or seen_add(x) )
	# # turn the set into a list (as requested)
	return list(seen_twice)



def upload_to_s3(aws_profile:str, bucket_name:str, filepath:str, file_name:str) -> None:

	"""Write a folder to an an S3 bucket"""

	s3 = s3fs.S3FileSystem(anon=False, profile=aws_profile)
	s3_path = f"{bucket_name}/{file_name}"
	s3.put(filepath, s3_path, recursive=True)

	return None


# a function to 

def make_zip(file_dir_in:str, file_name_out:str) -> None:

	"""Write all files in a directory and/or it's subdirectories make it a zipfile"""

	with zipfile.ZipFile(file_name_out, 'w', zipfile.ZIP_DEFLATED) as myzip:
		for root, dirs, files in os.walk(file_dir_in):
			for file in files:
				myzip.write(os.path.join(root, file))
				
	return None



def create_directory(directory):

	"""
	Create a directory if it doesn't exist.

	Args
	----
	directory : str
		The path to the directory.

	"""

	if not os.path.exists(directory):
		os.makedirs(directory)

def create_driver(download_path, driver_path):

	"""
	Create selenium chrome browser so we can pull the page source passed in url
	"""

	# set up Chrome browser for selenium

	options = webdriver.ChromeOptions()

	# add headless option 

	options.add_argument("headless")

	# simulate maxing out the browser window

	options.add_argument("start-maximized")

	# remove selenium log level 

	options.add_argument("--log-level=3")

	# disable blink features to get around captcha

	options.add_argument("--disable-blink-features")

	options.add_argument("--disable-blink-features=AutomationControlled")

	options.add_experimental_option("excludeSwitches", ["enable-automation"])

	# add browser notifications

	options.add_experimental_option("prefs", { 
		"profile.default_content_setting_values.notifications": 1 
	})

	# set download path

	chrome_prefs = {"download.default_directory": download_path}

	options.experimental_options["prefs"] = chrome_prefs

	options.add_experimental_option('useAutomationExtension', False)

	driver = webdriver.Chrome(executable_path=driver_path, options=options)

	# set user agent to avoid being blocked by websites

	driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

	driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36'})

	# get generic headers

	driver.get('https://www.httpbin.org/headers')

	return driver


def get_url_data(url, driver=None, is_download=False, is_download_request=False, is_request=False, wait=False):

	"""Use driver to get page source or download data"""

	# create headers for user agent for requests 

	headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


	# if is_download is true, get page or download data
	if is_download:

		if wait == True:
			driver.get(url)
			sleep(10)
		else:
			driver.get(url)
	
	if is_request:

		http = urllib3.PoolManager()

		response = http.request('GET', url, retries = 100)
	
		return response
	
	if is_download_request:

		response = requests.get(url, headers=headers)

		return response

		
	else:
		try:
			driver.get(url)
		except TimeoutException:
			print("Loading took too much time!")
		

	return driver



def download_wait(directory, timeout, nfiles=None):
	"""
	Wait for downloads to finish with a specified timeout.

	Args
	----
	directory : str
		The path to the folder where the files will be downloaded.
	timeout : int
		How many seconds to wait until timing out.
	nfiles : int, defaults to None
		If provided, also wait for the expected number of files.

	"""

	seconds = 0
	dl_wait = True
	while dl_wait and seconds < timeout:
		sleep(1)
		dl_wait = False
		files = os.listdir(directory)
		if nfiles and len(files) != nfiles:
			dl_wait = True

		for fname in files:
			if fname.endswith('.crdownload'):
				dl_wait = True

		seconds += 1
	return seconds
