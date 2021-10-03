
from process.raw import *

from process.curate import *

from process.presentation import *


abspath = os.path.dirname(os.path.normpath(os.path.abspath(os.path.dirname(__file__))))

driver_path = os.path.join(abspath, 'drivers', 'chromedriver.exe')

raw_download_path = os.path.normpath(os.path.join(abspath, 'data', 'raw'))

curate_download_path = os.path.normpath(os.path.join(abspath, 'data', 'curated'))

presentation_download_path = os.path.normpath(os.path.join(abspath, 'data', 'presentation'))

# Get Hospital URLS
hospital_urls = get_source_urls(driver_path, abspath)

""" Curated Standard Charge Data """
## Duke
df_raw_duke = get_duke(hospital_urls, raw_download_path)

df_curated_duke = curate_duke(df_raw_duke, curate_download_path)

print('Duke Downloaded')

## Cone
df_raw_cone = get_cone(hospital_urls, raw_download_path)

df_curated_cone = curate_cone(df_raw_cone, curate_download_path)

print('Cone Downloaded')

## North Carolina Baptist
df_raw_ncb = get_ncb(hospital_urls, raw_download_path)

df_curated_ncb = curate_ncb(df_raw_ncb, curate_download_path)

print("Baptist Download")

## Novant
df_raw_novant = get_novant(hospital_urls, raw_download_path)

df_curated_novant = curate_novant(df_raw_novant, curate_download_path)

print("Novant Downloaded")

## App Regional
df_raw_app = get_app(hospital_urls, raw_download_path)

df_curated_app = curate_app(df_raw_app, curate_download_path)

print("App Regional Downloaded")

## NHRMC
df_raw_nhrmc = get_nhrmc(hospital_urls, raw_download_path)

df_curated_nhrmc = curate_nhrmc(df_raw_nhrmc, curate_download_path)

print("NHRMC Downloaded")

## Catawba
df_raw_catawba = get_catawba(hospital_urls,  raw_download_path)

df_curated_catawba = curate_catawba(df_raw_catawba, curate_download_path)

print("Catawba Downloaded")

## Northern 
df_raw_northern = get_northern(hospital_urls, raw_download_path)

df_curated_north = curate_northern(df_raw_northern, curate_download_path)

print("Northern Downloaded")


## WakeMed 
df_raw_wakemed = get_wakemed(hospital_urls, raw_download_path, driver_path)

df_curated_wakemed = curate_wakemed(df_raw_wakemed, curate_download_path)

print("Wakemed Downloaded")

## First Health 
df_raw_first =  get_first(hospital_urls, raw_download_path) 

df_curated_first = curate_first(df_raw_first, curate_download_path)

print("First Health Downloaded")

## Atrium 
df_raw_atrium = get_atrium(hospital_urls, raw_download_path)

df_curated_atrium = curate_atrium(df_raw_atrium, curate_download_path)

print("Atrium Downloaded")


##  Vidant
df_vidant = get_vidant(hospital_urls, raw_download_path)

df_vidant_curated = curate_vidant(df_vidant, curate_download_path)

print("Vidant Downloaded")


""" Below only contain Standard Charges per Payor so we will only curate the raw """
# Iredell
df_raw_cdm_iredell, df_raw_drg_iredell, df_raw_drg_internet =  get_iredell(hospital_urls, raw_download_path) 

# Mission
df_raw_mission = get_mission(hospital_urls, raw_download_path) 

# Cateret 
df_raw_comp_cateret, df_raw_desc_cateret = get_cateret(hospital_urls,  raw_download_path)

print("CDM Downloaded Only")


# create a list of all curated dataframes to pass to presentation layer function

dfs_lst = [df_curated_duke, df_curated_cone, df_curated_ncb, df_curated_novant, 
            df_curated_app, df_curated_nhrmc, df_curated_catawba, df_curated_north, 
            df_curated_wakemed, df_curated_first, df_curated_atrium, df_vidant_curated]

presentation_df = get_presentation(dfs_lst, presentation_download_path)

print("Downloaded Presentation Data")