
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

# function to convert xlsx to csv

def xlsx_to_csv(file_in, file_out):

    """Convert xlsx to csv"""

    df = pd.read_excel(file_in)

    df.to_csv(file_out)

# function to convert json to csv

def json_to_csv(json, filepath, lines):
    
    df = pd.read_json(json, lines=lines)

    df.to_csv(filepath, index=False)


def list_duplicates(seq):
    seen = set()
    seen_add = seen.add
    # adds all elements it doesn't know yet to seen and all other to seen_twice
    seen_twice = set( x for x in seq if x in seen or seen_add(x) )
    # # turn the set into a list (as requested)
    return list(seen_twice)

def iterate_urls(url_list):

    for index, url in enumerate(url_list):

        print(f'{index}: {url}')

    return url_list



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


def get_url_data(driver, url, is_download=False, wait=False):

    """Use driver to get page source or download data"""

    # if is_download is true, get page or download data
    if is_download:

        if wait == True:
            driver.get(url)
            sleep(10)
        else:
            driver.get(url)
            sleep(60)
        
    else:
        try:
            driver.get(url)
        except TimeoutException:
            print("Loading took too much time!")
        

    return driver


def get_wakemed_data(driver, url):

    """Get wakemed data from url"""

    driver.get(url)

    xpath = '/html/body/app-root/app-allservices/div[1]/div/div[3]/div/app-paginator/div[2]/div/div/button'

    driver.execute_script("arguments[0].click();", WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, xpath))))

    driver.switch_to.alert.accept()

    sleep(20)


def get_atrium_data(url, download_path, lines):

    """Get atrium data from url"""
    
    url_split = urlsplit(url)

    url_path = url_split.path

    url_path_split = url_path.split('/')

    url_path_split = url_path_split[1:]

    filename = url_path_split[1].replace('.json', '')

    filepath = os.path.join(download_path, f"{filename}.csv")

    http = urllib3.PoolManager()
    
    r = http.request('GET', url)

    json_to_csv(r.data, filepath, lines)


def get_northern_data(file_in, file_out):

    df = pd.read_json(file_in, lines=True)

    df_all = df.drop(columns=['PACKAGE_TYPE', 'PERCENT_OCCURRENCE_WITHIN_PRIMARY_CODE','SUPPORTING_SERVICE_CODE' ,'SUPPORTING_SERVICE_CODE_DESCRIPTION'])

    for column in df_all:
        if df_all[column].dtype == 'float64':
            df_all[column]=pd.to_numeric(df_all[column], downcast='float')
        if df_all[column].dtype == 'int64':
            df_all[column]=pd.to_numeric(df_all[column], downcast='integer')

    df.to_csv(file_out, index=False)

def get_unc_data(driver, url):

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