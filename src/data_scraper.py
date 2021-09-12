from modules.scraper import *

import urllib3

driver = create_driver()

additional_exports = json.load(open('data/data.json'))

for hospital_id, url_list in additional_exports.items():

    for index, url in enumerate(url_list):

        if 'wakemed' in hospital_id:

            xpath = '/html/body/app-root/app-allservices/div[1]/div/div[3]/div/app-paginator/div[2]/div/div/button'

            driver.execute_script("arguments[0].click();", WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, xpath))))

            driver.switch_to.alert.accept()

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

            if 'atrium-health' in hospital_id:

                # download and save json file from url with urllib3

                http = urllib3.PoolManager()

                r = http.request('GET', url)

                with open('data/' + hospital_id + '_' + str(index) + '.json', 'wb') as f:

                    f.write(r.data)
                



