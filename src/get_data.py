import sys
import os
from prefect import task, Flow


from toolbox.scrapers import *




@task
hospital_urls = get_source_urls(driver_path, raw_download_path) 

# get_unc('university-of-north-carolina-hospital')

# define Prefect flow
with Flow("hospital-price-transparency") as flow:
    hospital_urls = get_source_urls()
    # houston_realtor_data = transform(realtor_data)
    # load_to_database = load(houston_realtor_data)

flow.register(project_name="hospital-price-transparency")

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