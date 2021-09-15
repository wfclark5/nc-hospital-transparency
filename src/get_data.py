from modules.scraper import *


abspath = os.path.normpath(os.path.dirname(os.path.dirname(__file__)))

raw_download_path = os.path.normpath(os.path.join(abspath, 'data', 'raw'))

url_download_path = os.path.normpath(os.path.join(abspath, 'data', 'urls'))

data_urls_path = os.path.join(url_download_path, 'hospital_data_urls.json')

driver_path = os.path.join(abspath, 'drivers', 'chromedriver.exe')

# load the json file
driver = create_driver(raw_download_path, driver_path)

# loop through each hospital_id in url dictionary and iterate through url list and download data

export_data = {}

structured_ext = ['.csv', 'wpfb_dl', '.xlsx', '.json']

skip_hospital = ['university-of-north-carolina-hospital', 'atrium-health', 'wakemed']

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'} # This is chrome, you can set whatever browser you like

hospital_data_urls = json.loads(open(data_urls_path))

# remove all files in download_path

for file in os.listdir(raw_download_path):
    file_path = os.path.join(raw_download_path, file)
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

        print(url)

        # if url ends with .csv, .xlsx, .json, or .wpfb_dl then download data

        if index == 0:

            wait = True
        
        else:

            wait = False

        if any(file_type in url for file_type in structured_ext):                
                
            get_url_data(driver, url, is_download=True, download_path=raw_download_path, wait=wait)

# wait for download to complete in selenium driver

download_wait(raw_download_path, 600)

# create a list of files names that end in .crdownload from download_path directory 
not_complete = [os.path.join(raw_download_path, filename) for filename in os.listdir(url_download_path) if filename.endswith(".crdownload")]

if not_complete:
    print("Downloads not complete")
    print(not_complete)

driver = create_driver(raw_download_path, driver_path)

print("Downloads complete and exported rest of urls")

for hospital_id, urls in export_data.items():
    print(hospital_id)
    for url in urls:
        print(url)
        if 'atrium' in hospital_id:
            get_atrium_data(url, raw_download_path, True)
        
        if 'wakemed' in hospital_id:
            get_wakemed_data(driver,url)



# create a list of all .csv and .json files in the raw_download_path

convert_files =  [f for f in os.listdir(raw_download_path) if f.endswith('.xlsx') or f.endswith('.json')]

# convert json and xlsx files to csv and download_path directory, convert to csv and then delete json file

for file in convert_files:

    if '.json' in file:

        file_in_path = os.path.join(raw_download_path, file)

        file_out_path = os.path.join(raw_download_path, file.replace('.json', '') + '.csv')

        if 'northern-regional' in file:
            get_northern_data(file_in_path, file_out_path)

            

    if '.xlsx' in file:

        print(file)

        file_in_path = os.path.join(raw_download_path, file)

        file_out_path = os.path.join(raw_download_path, file.replace('.xlsx', '') + '.csv')

        xlsx_to_csv(file_in_path, file_out_path)


# find duplicates in duplicates list 

duplicates = list_duplicates([f.replace('.json', '').replace('.csv', '').replace('.xlsx', '') for f in os.listdir(raw_download_path)])

convert_files =  [f for f in os.listdir(raw_download_path) if f.endswith('.xlsx') or f.endswith('.json')]


# remove duplicates
for file in convert_files:
    os.remove(os.path.join(raw_download_path, file))

open(os.path.join(url_download_path, 'additional_exports.json'), 'w').write(json.dumps(export_data))
