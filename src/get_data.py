import sys
import os
from prefect import task, Flow


abspath = os.path.dirname(os.path.normpath(os.path.abspath(os.path.dirname(__file__))))

sys.path.append(abspath)

from toolbox.scrapers import *

print(abspath)

driver_path = os.path.join(abspath, 'drivers', 'chromedriver.exe')

raw_download_path = os.path.normpath(os.path.join(abspath, 'data', 'raw'))


hospital_urls = get_source_urls(driver_path, raw_download_path) 

# get_unc('university-of-north-carolina-hospital')


# df_cdm, df_drg = get_duke('duke-university-hospital', hospital_urls, raw_download_path)

# df_ncb = get_north_carolina_baptist('north-carolina-baptist-hospital', hospital_urls, raw_download_path)

# df_cdm_app, df_drg_app, df_shop_app = get_app('app-regional-health-system', hospital_urls, raw_download_path)

# df_catawba = get_catawba('catawba-valley-medical-center', hospital_urls,  raw_download_path)

# df_comp_cateret, df_desc_cateret = get_cateret('cateret-health-care', hospital_urls,  raw_download_path)

# df_cone = get_cone('cone-health', hospital_urls, raw_download_path)

# df_first_moore =  get_first('first-health-moore', hospital_urls, raw_download_path)

# df_first_montgomery =  get_first('first-health-montgomery', hospital_urls, raw_download_path)

# df_cdm_iredell, df_drg_iredell, df_drg_internet =  get_iredell('iredell-health', hospital_urls, raw_download_path)

# df_mission = get_mission('mission-health', hospital_urls, raw_download_path)

# df_nhrmc_op, df_nhrmc_ip = get_nhrmc('nhrmc-health', hospital_urls, raw_download_path)

df_northern = get_northern('northern-regional', hospital_urls, raw_download_path)

# df_novant = get_novant('novant-health', hospital_urls, raw_download_path)

# df_wakemed_raleigh = get_wakemed('wakemed-raleigh', hospital_urls, raw_download_path, driver_path)

# get_wakemed('wakemed-cary', hospital_urls, raw_download_path, driver_path)

df_vidant = get_vidant('vidant-health', hospital_urls, raw_download_path)

# get_atrium('atrium-health', hospital_urls, raw_download_path)