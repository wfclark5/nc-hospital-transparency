import sys
import os
from prefect import task, Flow


abspath = os.path.dirname(os.path.normpath(os.path.abspath(os.path.dirname(__file__))))

sys.path.append(abspath)

from process.raw import *

from process.curate import *


# get_unc('university-of-north-carolina-hospital')

driver_path = os.path.join(abspath, 'drivers', 'chromedriver.exe')

raw_download_path = os.path.normpath(os.path.join(abspath, 'data', 'raw'))

hospital_urls = get_source_urls(driver_path, raw_download_path) 

curate_download_path = os.path.normpath(os.path.join(abspath, 'data', 'curated'))


## Get date for today to pass into filename 

## Northern (!!! Need Curate)
df_northern = get_northern(hospital_urls, raw_download_path)

## WakeMed Cary (!!! Need Curate) and Wakemed Raleigh (!!! Need Curate)
df_wakemed_raleigh, df_wakemed_cary = get_wakemed(hospital_urls, raw_download_path, driver_path)

## Atrium (!!! Need Curate)
df_atrium = get_atrium(hospital_urls, raw_download_path)

##  Vidant (!!! Need Curate)
df_vidant = get_vidant(hospital_urls, raw_download_path)

## First Health Montegomery (!!! Need Curate)
df_first_montgomery =  get_first(hospital_urls, raw_download_path) #need to pivot data on payor

## First Health Moore (!!! Need Curate)
df_first_moore =  get_first(hospital_urls, raw_download_path) #need to pivot data on payor

## Below only contain Standard Charges per Payor so we will only curate the raw ##

## Iredell
df_cdm_iredell, df_drg_iredell, df_drg_internet =  get_iredell('iredell-health', hospital_urls, raw_download_path) # Get's charge master but not standard charges

## Mission
df_mission = get_mission('mission-health', hospital_urls, raw_download_path) #Get's charge master but not standard charges

## Cateret 
df_comp_cateret, df_desc_cateret = get_cateret('cateret-health-care', hospital_urls,  raw_download_path) #Get's charge master not standard charges

## Have Curated Standard Charge Data ####

## Duke
df_cdm, df_drg = get_duke(hospital_urls, raw_download_path)

curate_duke_df = curate_duke(df_cdm, df_drg, curate_download_path)

## Cone
df_cone = get_cone(hospital_urls, raw_download_path)

curate_cone_df = curate_cone(df_cone, curate_download_path)

## North Carolina Baptist
df_ncb = get_north_carolina_baptist(hospital_urls, raw_download_path)

curate_ncb_df = curate_ncb(df_ncb, curate_download_path)

## Novant
df_novant = get_novant(hospital_urls, raw_download_path)

curate_novant_df = curate_novant(df_novant, curate_download_path)


## App Regional
df_cdm_app, df_drg_app, df_shop_app = get_app(hospital_urls, raw_download_path)

curate_app_df = curate_app_shoppable(df_shop_app, curate_download_path)


## NHRMC
df_nhrmc_op, df_nhrmc_ip = get_nhrmc(hospital_urls, raw_download_path)

curate_nhrmc_df = curate_nhrmc(df_nhrmc_ip, curate_download_path)


## Catawba
df_catawba = get_catawba(hospital_urls,  raw_download_path)

curate_catawba_df = curate_catawba(df_catawba, curate_download_path)

 
## App 
df_cdm_app, df_drg_app, df_shop_app = get_app(hospital_urls, raw_download_path) 

curate_app_shoppable_df = curate_app_shoppable(df_shop_app, curate_download_path)



