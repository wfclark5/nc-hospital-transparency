
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


def get_url_data(driver, url, is_download=False, download_path=None, wait=False):

    """Use driver to get page source or download data"""

    # if is_download is true, get page or download data

    def _download_file(driver, url, download_path, wait):
    
        """Download file from url"""

        # download file and wait for download to complete

        if wait == True:
            driver.get(url)
            sleep(15)
        else:
            driver.get(url)

    if is_download:
        
        _download_file(driver, url, download_path, wait)
        
    else:
        try:
            driver.get(url)
        except TimeoutException:
            print("Loading took too much time!")
        

    return driver



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


def wait_for_downloads(download_path):

    """Wait for downloads to complete"""

    print("Waiting for downloads..", end="")

    
    timeout = 600   # [seconds]

    timeout_start = time()

    # while runtime is greater than 10 minutes then exit
    
    while any([filename.endswith(".crdownload") for filename in os.listdir(download_path)]):
        if time() > timeout_start + timeout:
            sleep(1)
            print("..", end="")
            break

    print("done!")
