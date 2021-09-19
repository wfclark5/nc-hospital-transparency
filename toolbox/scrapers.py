from .utils import *


def get_source_urls(driver_path: str, raw_download_path: str) -> dict:

    # print(csv_path)

    hospital_csv = os.path.join(r'C:\Users\remot\OneDrive\Desktop\Personal\nc-hospital-transparency', 'hospitals.csv')


    print(hospital_csv)
    # read in hospitals csv

    df = pd.read_csv(hospital_csv)

    print(df.head())

    driver = create_driver(raw_download_path, driver_path)

    hospital_data_urls = {}

    unc_urls = []

    ext = ['.json','.csv', 'wpfb_dl', '.xlsx', 'ptapp']

    for index, row in df.iterrows():

        records = []
        
        browser = get_url_data(row['hospital_url'], driver)

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

    return hospital_data_urls


def get_unc(driver_path: str, hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Create drivers to bypass captcha for UNC data"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    driver = create_driver(download_path, driver_path)


    def _wait_between(a,b):
        rand=uniform(a, b) 
        sleep(rand)

    for url in url_list:
        
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



def get_duke(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Duke data and download the csv files"""

    url_list = hospital_urls[hospital_id]
    
    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    df_cdm = pd.DataFrame()

    df_drg = pd.DataFrame()

    for url in url_list:
        print(url)
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')), error_bad_lines=False)
        # df = pd.read_csv(response.content)
        
        with open(os.path.join(download_path, filename), 'wb') as f:
            f.write(response.content)

def get_north_carolina_baptist(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Wake-Forest Baptist data and download the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(download_path, filename.replace('?la=en', '')), 'wb') as f:
            f.write(response.content)
        

def get_app(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Applachain Regional Data and download only the CSV data"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        filename = response.headers['Content-Disposition'].strip('"').replace('inline; filename="', '').replace('"', '')

        # if filename ends with .csv write it out

        if filename.endswith('.csv'):
            with open(os.path.join(download_path, filename), 'wb') as f:
                f.write(response.content)
        else:
            continue


def get_catawba(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Catawba Valley Regional data and download the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        if filename.endswith('.csv'):
            with open(os.path.join(download_path, filename), 'wb') as f:
                f.write(response.content)
        else:
            continue


def get_cateret(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:
    
    """Get Cateret Health data and download only the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        if filename.endswith('.csv'):
            with open(os.path.join(download_path, filename), 'wb') as f:
                f.write(response.content)
        else:
            excel_to_csv(response.content, os.path.join(download_path, filename))


def get_cone(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Cone Health data and download only the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        print(filename)
        if filename.endswith('.csv'):
            with open(os.path.join(download_path, filename), 'wb') as f:
                f.write(response.content)
        else:
            continue

def get_first(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

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


    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)
    
    for url in url_list:
        # infer filename from url 
        # filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        # create pandas dataframe from json 
        # df = pd.read_json()
        json_data =  json.loads(response.content)['response']
        df = pd.json_normalize(json_data, record_path='payors', meta=meta, errors='ignore', record_prefix='payor.')
        filepath = os.path.join(download_path, f'{hospital_id}_standardcharges.csv')
        # if file does not exist write header 
        if not os.path.isfile(filepath):
            df.to_csv(filepath, header=headers, index=False)
        else: # else it exists so append without writing the header
            df.to_csv(filepath, mode='a', header=False, index=False)
    # pivot pandas dataframe on codeeType and description
    # df = pd.read_csv(filepath)
    # df.pivot(index='codeType', columns='description', values=values).to_csv(os.path.join(download_path, 'pivot.csv'))

        
def get_iredell(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:


    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)
    
    """Get Iredell Health data and download only the CSV file"""
    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(download_path, filename.replace('?la=en', '')), 'wb') as f:
            f.write(response.content)
    

def get_mission(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Mission Health data and download only the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)
    
    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(download_path, filename), 'wb') as f:
            f.write(response.content)

def get_nhrmc(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get New Hanover Regional Medical Center data and download only the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:
         
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file from excel
        excel_to_csv(response.content, os.path.join(download_path, filename))

def get_northern(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Northern Regional data and download only the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

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
        df.to_csv(os.path.join(download_path, filename), index=False)

def get_novant(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Novant Health data and download only the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        response = get_url_data(url, is_request=True)
        # write reponse to csv file 
        with open(os.path.join(download_path, filename), 'wb') as f:
            f.write(response.content)

def get_vidant(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Vidant Health data and download only the CSV file"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:
         
        # infer filename from url 
        filename = url.split('/')[-1]
        # download the file
        print(filename)
        response = get_url_data(url, is_request=True)
        # write reponse to csv file from excel

        excel_to_csv(response.content, os.path.join(download_path, filename))


def get_atrium(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get Atrium Health data from url"""

    url_list = hospital_urls[hospital_id]

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    for url in url_list:

        filename = url.split('/')[-1].replace('.json', '.csv')

        print(filename)

        # download the file

        http = urllib3.PoolManager()
        
        r = http.request('GET', url)

        # create pandas dataframe from response
        df = pd.read_json(r.data)

        df.to_csv(os.path.join(download_path, filename), index=False)



def get_wakemed(hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

    """Get wakemed data from url"""

    url_list = hospital_urls[hospital_id]

    print(url_list)

    download_path = os.path.join(raw_download_path, hospital_id)
    
    create_directory(download_path)

    driver = create_driver(download_path, driver_path)

    for url in url_list:
    # infee filename from url
    
        driver.get(url)

        xpath = '/html/body/app-root/app-allservices/div[1]/div/div[3]/div/app-paginator/div[2]/div/div/button'

        driver.execute_script("arguments[0].click();", WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, xpath))))

        driver.switch_to.alert.accept()

    download_wait(directory=download_path, timeout=150, nfiles=1)

    json_file = os.path.join(download_path, [pos_json for pos_json in os.listdir(download_path)][0])

    wakemed_json = open(json_file, 'r').read()

    wakemed_data = wakemed_json.replace('\x00', '')

    df = pd.read_json(wakemed_data)

    df.to_csv(os.path.join(download_path, f'{hospital_id}_standardcharges.csv'), index=False)

    os.remove(json_file)