import sys
from toolbox.scrapers import *
from prefect import task, Flow

# abspath = os.path.normpath(os.path.dirname(os.path.dirname(__file__)))


@task
def get_source_urls() -> dict:


    hospital_csv = os.path.normpath(os.path.join(abspath, 'hospitals.csv'))

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

# get_unc('university-of-north-carolina-hospital')

# define Prefect flow
with Flow("hospital-price-transparency") as flow:
    hospital_urls = get_source_urls()
    # houston_realtor_data = transform(realtor_data)
    # load_to_database = load(houston_realtor_data)

flow.register(project_name="hospital-price-transparency")

flow.run_agent()

flow.run()
# get_duke('duke-university-hospital', hospital_urls)

# get_north_carolina_baptist('north-carolina-baptist-hospital', hospital_urls)

# get_app('app-regional-health-system', hospital_urls)

# get_catawba('catawba-valley-medical-center', hospital_urls)

# get_cateret('cateret-health-care', hospital_urls)

# get_cone('cone-health', hospital_urls)

# get_first('first-health-moore', hospital_urls)

# get_first('first-health-montgomery', hospital_urls)

# get_iredell('iredell-health', hospital_urls)

# get_mission('mission-health', hospital_urls)

# get_nhrmc('nhrmc-health', hospital_urls)

# get_northern('northern-regional', hospital_urls)

# get_novant('novant-health', hospital_urls)

# get_wakemed('wakemed-raleigh', hospital_urls)

# get_wakemed('wakemed-cary', hospital_urls)

# get_vidant('vidant-health', hospital_urls)

# get_atrium('atrium-health', hospital_urls)