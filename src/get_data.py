from toolbox.scrapers import *

hospital_urls = os.path.join(url_download_path, 'hospital_data_urls.json')

urls_json = json.load(open(hospital_urls))


# get_unc('university-of-north-carolina-hospital')

get_duke('duke-university-hospital')

get_north_carolina_baptist('north-carolina-baptist-hospital')

get_app('app-regional-health-system')

get_catawba('catawba-valley-medical-center')

get_cateret('cateret-health-care')

get_cone('cone-health')

get_first('first-health-moore')

get_first('first-health-montgomery')

get_iredell('iredell-health')

get_mission('mission-health')

get_nhrmc('nhrmc-health')

get_northern('northern-regional')

get_novant('novant-health')

get_wakemed('wakemed-raleigh')

get_wakemed('wakemed-cary')

get_vidant('vidant-health')

get_atrium('atrium-health')