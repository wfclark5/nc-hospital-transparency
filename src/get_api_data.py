#!/usr/bin/env python
# -*- coding: utf-8 -*-

from modules.scraper import *

# create hospital, download, and driver paths

abspath = os.path.normpath(os.path.dirname(os.path.dirname(__file__)))

hospitals = os.path.normpath(os.path.join(abspath, 'hospitals.csv'))

download_path = os.path.normpath(os.path.join(abspath, 'data', 'raw'))

driver_path = os.path.join(abspath, 'drivers', 'chromedriver.exe')

# read in hospitals csv

df = pd.read_csv(hospitals)

# loop through each hospital and ping standard charges page

driver = create_driver(download_path, driver_path)

hospital_data_urls = {}

unc_urls = []

ext = ['.json','.csv', 'wpfb_dl', '.xlsx', 'ptapp']

for index, row in df.iterrows():

    records = []
    
    browser = get_url_data(driver, row['hospital_url'])

    # get the page source and parse itS

    source = browser.page_source

    soup = BeautifulSoup(source, 'lxml')
    
    for entry in soup.find_all(['a'], href=True): 

        download_url = entry.get('href')

        if any(file_type in download_url for file_type in ext):
            # determine if the base hospital_url is in download_url or not

            if row['hospital_id'] in 'vidant-health' and '.csv' in download_url:
                continue

            if download_url.startswith('/'):
                # if download_url starts with '/' then add the base hospital_url to the download_url

                data_url = urlparse(row['hospital_url']).scheme + "://" + urlparse(row['hospital_url']).netloc + '/'  + download_url

                records.append(data_url)

            else:
                # if download_url does not start with '/' then add the base hospital_url to the download_url
                records.append(download_url)
            
    if 'first-health' in row['hospital_id']:

        limit = 250

        page = 0

        first_health_url = row['hospital_url'].format(0)

        output = requests.get(first_health_url).json()

        output = requests.get(row['hospital_url'].format(0)).json()

        page_limit_max = int((output['count'] / limit) + 1)

        first_health_urls = [records.append(row['hospital_url'].format(page)) for page in range(1, page_limit_max)]
        
    if 'wakemed' in row['hospital_id']:

        records.append(row['hospital_url'])
        
    hospital_data_urls[row['hospital_id']] = list(set(records))

# write to a json file

open('hospital_data_urls.json', 'w').write(json.dumps(hospital_data_urls))

# remove hospital_data_urls keys where values are empty

hospital_empty_urls = {k: v for k, v in hospital_data_urls.items() if v == []}

hospital_contain_urls = {k: v for k, v in hospital_data_urls.items() if v != []}

hospitals_collected = list(hospital_contain_urls.keys())

hospital_not_collected = list(hospital_empty_urls.keys())

# url_dictionary: {'hospital_id': [url1, url2, url3]} 

# loop through each hospital_id in url dictionary and iterate through url list and download data

export_data = {}

structured_ext = ['.csv', 'wpfb_dl', '.xlsx', '.json']

skip_hospital = ['university-of-north-carolina-hospital']

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'} # This is chrome, you can set whatever browser you like

# remove all files in download_path

for file in os.listdir(download_path):
    file_path = os.path.join(download_path, file)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
    except Exception as e:
        print(e)

for hospital_id, url_list in hospital_data_urls.items():

    # if hospital_id is atrium then go to next item in hospital_data_urls dictionary

    print(hospital_id)

    if any(ids in hospital_id for ids in skip_hospital): 

        export_data[hospital_id] = url_list

        continue

    # iterate through lists in url_list and download data

    for index, url in enumerate(url_list):

        # if url ends with .csv, .xlsx, .json, or .wpfb_dl then download data

        if index == 0:

            wait = True
        
        else:

            wait = False

        if any(file_type in url for file_type in structured_ext):                

            if 'wakemed' in hospital_id:

                get_wakemed_data(driver,url)

            if 'atrium-health' in hospital_id:

                get_atrium_data(driver, url, download_path)
                
            else:
                
                get_url_data(driver, url, is_download=True, download_path=download_path, wait=wait)


print(download_path)

# wait for download to complete in selenium driver

wait_for_downloads(download_path)

# create a list of files names that end in .crdownload from download_path directory 
not_complete = [os.path.join(download_path, filename) for filename in os.listdir(download_path) if filename.endswith(".crdownload")]

if not_complete:
    print("Downloads not complete")
    print(not_complete)


print("Downloads complete and exported rest of urls")

open('additional_exports.json', 'w').write(json.dumps(export_data))


# write a function that takes in two arguments a json file and a filepath to convert a json file to a csv file with pandas 







