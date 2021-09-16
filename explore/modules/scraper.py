
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


def get_unc(driver, url):

    """Create drivers to bypass captcha for UNC data"""

    def _wait_between(a,b):
        rand=uniform(a, b) 
        sleep(rand)


    try:
        driver.get(url)
    except TimeoutException:
        print("Loading took too much time!")
    
    driver.find_element_by_xpath('/html/body/div[1]/div/div/div[2]/div/div[1]/div[1]/div[2]/div/a[2]').click()

    sleep(10)

    mainWin = driver.current_window_handle  

    # move the driver to the first iFrame 
    driver.switch_to_frame(driver.find_elements_by_tag_name("iframe")[0])

    # *************  locate CheckBox  **************
    CheckBox = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID ,"recaptcha-anchor"))
            ) 

    # *************  click CheckBox  ***************
    _wait_between(0.5, 0.7)  
    
    # making click on captcha CheckBox 
    CheckBox.click()

    # switch back to main window

    driver.switch_to.window(mainWin)

    driver.find_element_by_xpath('/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/div/div[2]/div/div/div/div/fieldset/div/div/div/div[4]/div/div/div/div/div/span/input').click()

    driver.find_element_by_xpath('/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/div/div[2]/div/div/div/div/fieldset/div/div/div/a').click()

    return driver



def get_duke(url_list):

    """Get Duke data and download the csv files"""

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(raw_download_path, filename), 'wb') as f:
            f.write(response.content)
        


def get_north_carolina_bapitist(url_list):

    """Get Wake-Forest Baptist data and download the CSV file"""

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(raw_download_path, filename.replace('?la=en', '')), 'wb') as f:
            f.write(response.content)
        

def get_app(url_list):

    """Get Applachain Regional Data and download only the CSV data"""

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        filename = response.headers['Content-Disposition'].strip('"').replace('inline; filename="', '').replace('"', '')

        # if filename ends with .csv write it out

        if filename.endswith('.csv'):
            with open(os.path.join(raw_download_path, filename), 'wb') as f:
                f.write(response.content)
        else:
            continue


def get_catawba(url_list):

    """Get Catawba Valley Regional data and download the CSV file"""

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        if filename.endswith('.csv'):
            with open(os.path.join(raw_download_path, filename), 'wb') as f:
                f.write(response.content)
        else:
            continue


def get_cateret(url_list):
    
    """Get Cateret Health data and download only the CSV file"""

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        if filename.endswith('.csv'):
            with open(os.path.join(raw_download_path, filename), 'wb') as f:
                f.write(response.content)
        else:
            excel_to_csv(response.content, os.path.join(raw_download_path, filename))


def get_cone(url_list):

    """Get Cone Health data and download only the CSV file"""

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        print(filename)
        if filename.endswith('.csv'):
            with open(os.path.join(raw_download_path, filename), 'wb') as f:
                f.write(response.content)
        else:
            continue

def get_first(url_list, filename):

    """Get First Health data and download only the CSV file"""


    meta = ['id' , 'hospital' , 'code' , 'description' , 'codeType' , 'cmsShoppable', 'cranewareShoppable' , 
            'shoppable' , 'level' , 'grossCharge' , 'minAllowable' , 'maxAllowable' , 'avgAllowable' , 
            'nationalPercentile50' , 'nationalPercentile75' , 'nationalPercentile90' , 'totalVol835' , 
            'totalVol837' , 'published' , 'selfPay' , ['name', 'id', 'hospital', 'minAllowable', 'maxAllowable', 
            'avgAllowable', 'exclude']]
            
    headers = ['payor.name', 'payor.id', 'payor.hospital', 'payor.minAllowable', 'payor.maxAllowable', 'payor.avgAllowable', 
                'payor.exclude', 'id', 'hospital', 'code', 'description', 'codeType', 'cmsShoppable', 'cranewareShoppable', 
                'shoppable', 'level', 'grossCharge', 'minAllowable', 'maxAllowable', 'avgAllowable', 'nationalPercentile50', 
                'nationalPercentile75', 'nationalPercentile90', 'totalVol835', 'totalVol837', 'published', 'selfPay', 
                'name.id.hospital.minAllowable.maxAllowable.avgAllowable.exclude']

    values = ['payor.name', 'payor.id', 'payor.hospital', 'payor.minAllowable', 'payor.maxAllowable', 'payor.avgAllowable', 
                'payor.exclude', 'id', 'hospital', 'codeType', 'cmsShoppable', 'cranewareShoppable', 'shoppable', 'level', 
                'grossCharge', 'minAllowable', 'maxAllowable', 'avgAllowable', 'nationalPercentile50', 'nationalPercentile75', 
                'nationalPercentile90', 'totalVol835', 'totalVol837', 'published', 'selfPay']
    for url in url_list:
        # infer filename from url 
        # filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        # create pandas dataframe from json 
        # df = pd.read_json()
        json_data =  json.loads(response.content)['response']
        df = pd.json_normalize(json_data, record_path='payors', meta=meta, errors='ignore', record_prefix='payor.')
        filepath = os.path.join(raw_download_path, filename)
        # if file does not exist write header 
        if not os.path.isfile(filepath):
            df.to_csv(filepath, header=headers, index=False)
        else: # else it exists so append without writing the header
            df.to_csv(filepath, mode='a', header=False, index=False)
    # pivot pandas dataframe on codeeType and description
    # df = pd.read_csv(filepath)
    # df.pivot(index='codeType', columns='description', values=values).to_csv(os.path.join(raw_download_path, 'pivot.csv'))

        
def get_iredell(url_list):

    """Get Iredell Health data and download only the CSV file"""
    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(raw_download_path, filename.replace('?la=en', '')), 'wb') as f:
            f.write(response.content)
    

def get_mission(url_list):

    """Get Mission Health data and download only the CSV file"""

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(raw_download_path, filename), 'wb') as f:
            f.write(response.content)

def get_nhrmc(url_list):

    """Get New Hanover Regional Medical Center data and download only the CSV file"""
    
    for url in url_list:
         
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file from excel
        excel_to_csv(response.content, os.path.join(raw_download_path, filename))

def get_northern(url_list):

    """Get Northern Regional data and download only the CSV file"""

    for url in url_list:
        # infer filename from url and replace .json with .csv
        filename = url.split('/')[-1].replace('.json', '.csv')

        # download the file
        response = get_url_data(url, is_request=True)

        # create pandas dataframe from response
        df = pd.read_json(response.content, lines=True)

        # drop columns
        df_all = df.drop(columns=['PACKAGE_TYPE', 'PERCENT_OCCURRENCE_WITHIN_PRIMARY_CODE','SUPPORTING_SERVICE_CODE' ,'SUPPORTING_SERVICE_CODE_DESCRIPTION'])

        # reformat column types
        for column in df_all:
            if df_all[column].dtype == 'float64':
                df_all[column]=pd.to_numeric(df_all[column], downcast='float')
            if df_all[column].dtype == 'int64':
                df_all[column]=pd.to_numeric(df_all[column], downcast='integer')

        # write to csv
        df.to_csv(os.path.join(raw_download_path, filename), index=False)

def get_novant(url_list):

    """Get Novant Health data and download only the CSV file"""

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(raw_download_path, filename), 'wb') as f:
            f.write(response.content)

def get_vidant(url_list):

    """Get Vidant Health data and download only the CSV file"""

    for url in url_list:
         
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file from excel
        excel_to_csv(response.content, os.path.join(raw_download_path, filename))


def get_atrium(url_list):

    """Get Atrium Health data from url"""

    for url in url_list:

        filename = url.split('/')[-1].replace('.json', '.csv')

        # download the file
        response = get_url_data(url, is_request=True)

        # create pandas dataframe from response
        df = pd.read_json(response.content, lines=True)

        df.to_csv(os.path.join(raw_download_path, filename), index=False)



def get_wakemed(driver, url_list):

    """Get wakemed data from url"""

    for url in url_list:
    # infee filename from url
    
        driver.get(url)

        xpath = '/html/body/app-root/app-allservices/div[1]/div/div[3]/div/app-paginator/div[2]/div/div/button'

        driver.execute_script("arguments[0].click();", WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, xpath))))

        driver.switch_to.alert.accept()

        sleep(20)

