
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
import wget 
import requests
from pathlib import Path
import io
import boto3

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


# a function to write a get from urllib requests.context to an S3 bucket


def write_to_s3(bucket_name, filepath, response):

    """
    Write file to S3 bucket

    Args
    ----
    filepath : str
        The path to the file to be uploaded.
    bucket_name : str
        The name of the S3 bucket.
    s3_key : str
        The key to be used for the file in the S3 bucket.

    """

    # create a session and connect to S3

    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )

    s3 = session.resource('s3')

    # open file and upload to S3

    s3.Bucket(bucket_name).put_object(Key=filepath, Body=response.content)

    # remove the file from the local directory

    # os.remove(filepath)


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


def get_url_data(url, driver=None, is_download=False, is_request=False, wait=False):

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

    print("Waiting for downloads to finish")

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

def excel_to_csv(response, filename):

    """Convert excel file to csv file"""

    df = pd.read_excel(response)

    df.to_csv(filename.replace('.xlsx', '.csv'))