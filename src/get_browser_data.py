from modules.scraper import *

import urllib3

driver = create_driver()

additional_exports = json.load(open('data/data.json'))

for hospital_id, url_list in additional_exports.items():

    for index, url in enumerate(url_list):

        if 'university-of-north-carolina-hospital' in hospital_id:
            
            driver = get_unc_data(driver, url)

            source = driver.page_source

            soup = BeautifulSoup(source, 'lxml')

            for entry in soup.find_all(['iframe'], href=True):

                download_url = entry.get('src')

                if 'CSV' in download_url:

                    base_url = 'https://portalapprev.com/ptapp/#'

                    download_url = base_url + '/' + download_url

                    driver.get(download_url)
