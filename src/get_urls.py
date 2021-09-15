#!/usr/bin/env python
# -*- coding: utf-8 -*-

from modules.scraper import *

# create hospital, download, and driver paths

abspath = os.path.normpath(os.path.dirname(os.path.dirname(__file__)))

hospitals = os.path.normpath(os.path.join(abspath, 'hospitals.csv'))

raw_download_path = os.path.normpath(os.path.join(abspath, 'data', 'raw'))

url_download_path = os.path.normpath(os.path.join(abspath, 'data', 'urls'))

driver_path = os.path.join(abspath, 'drivers', 'chromedriver.exe')

# read in hospitals csv

df = pd.read_csv(hospitals)

# loop through each hospital and ping standard charges page

driver = create_driver(raw_download_path, driver_path)

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

# remove hospital_data_urls keys where values are empty

hospital_empty_urls = {k: v for k, v in hospital_data_urls.items() if v == []}

hospital_contain_urls = {k: v for k, v in hospital_data_urls.items() if v != []}

hospitals_collected = list(hospital_contain_urls.keys())

hospital_not_collected = list(hospital_empty_urls.keys())

# url_dictionary: {'hospital_id': [url1, url2, url3]} 


# write to a json file

open(os.path.join(url_download_path, 'hospital_data_urls.json'), 'w').write(json.dumps(hospital_data_urls))


