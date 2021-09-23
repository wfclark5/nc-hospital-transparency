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


df_cdm, df_drg = get_duke('duke-university-hospital', hospital_urls, raw_download_path)

def curate_duke(duke_df: pd.DataFrame, cdm=True) -> pd.DataFrame:
    duke_df = duke_df.astype(str)

    cols = duke_df.columns.to_list()
    for col in cols:
        if cdm==True:
            duke_df[col] = duke_df[col].apply(lambda s: s.replace(' $0.00 for routine inpatient services and up to $', '')).apply(lambda s: s.replace(' for all other services, depending on the circumstances ', '')).apply(lambda s: s.replace(' $0.00 for routine inpatient services; negotiated from ', '')).apply(lambda s: s.replace('$', ''))
            try:
                duke_df[col] = duke_df[col].str.split(' to ', expand=True)[1]
            except:
                pass
        else:
            duke_df = duke_df.replace('Payment data is not available for this DRG for this entity for this payer', '')
            try:
                duke_df[col] = duke_df[col].str.split(', ', expand=True)[0].apply(lambda s: s.replace('Average payment of ', ''))
            except:
                pass
    
    

    return duke_df


df_cone = get_cone('cone-health', hospital_urls, raw_download_path)

def curate_cone(cone_df: pd.DataFrame) -> pd.DataFrame:
	cone_out_df = df_cone[['Procedure',
		'Code (CPT/HCPCS/MS-DRG)',
		'NDC (for medications/drugs)',
		'Rev Code',
		'Procedure Description',
		' Gross Charge ',
		' Aetna Negotiated Outpatient Rate ',
		' AETNA MEDICARE Negotiated Outpatient Rate ',
		' BLUE CROSS BLUE SHIELD Negotiated Outpatient Rate ',
		' BLUE CROSS BLUE SHIELD MEDICARE Negotiated Outpatient Rate ',
		' CIGNA Negotiated Outpatient Rate ',
		' CIGNA MEDICARE ADVANTAGE Negotiated Outpatient Rate ',
		' HUMANA Negotiated Outpatient Rate ',
		' HUMANA MEDICARE Negotiated Outpatient Rate ',
		' UNITED HEALTHCARE Negotiated Outpatient Rate ',
		' UNITED HEALTHCARE MEDICARE Negotiated Outpatient Rate ',
		' MEDCOST Negotiated Outpatient Rate ',
		' TRICARE Negotiated Outpatient Rate ',
		' Uninsured Outpatient Rate ',
		' De-identified Min Outpatient Negotiated Rate Across All Payers ',
		' De-identified Max Outpatient Negotiated Rate Across All Payers ']]

	cone_out_df['Patient Type'] = 'Outpatient'


	cone_in_df = df_cone[['Procedure',
	'Code (CPT/HCPCS/MS-DRG)',
	'NDC (for medications/drugs)',
	'Rev Code',
	'Procedure Description',
	' Gross Charge ',
	' Aetna Negotiated Inpatient Rate ',
	' AETNA MEDICARE Negotiated Inpatient Rate ',
	' BLUE CROSS BLUE SHIELD Negotiated Inpatient Rate ',
	' BLUE CROSS BLUE SHIELD MEDICARE Negotiated Inpatient Rate ',
	' CIGNA Negotiated Inpatient Rate ',
	' CIGNA MEDICARE ADVANTAGE Negotiated Inpatient Rate ',
	' HUMANA Negotiated Inpatient Rate ',
	' HUMANA MEDICARE Negotiated Inpatient Rate ',
	' UNITED HEALTHCARE Negotiated Inpatient Rate ',
	' UNITED HEALTHCARE MEDICARE Negotiated Inpatient Rate ',
	' MEDCOST Negotiated Inpatient Rate ',
	' TRICARE Negotiated Inpatient Rate ',
	' Uninsured Inpatient Rate ',
	' De-identified Min Inpatient Negotiated Rate Across All Payers ',
	' De-identified Max Inpatient Negotiated Rate Across All Payers ']]


	cone_in_df['Patient Type'] = 'Inpatient'

	cone_headers = ['Procedure', 'CPT/HCPCS/MS-DRG','NDC Code', 'Rev Code', 'Procedure Description','Gross Charge', 'Aetna','AETNA MEDICARE', 'BCBS','BCBS MEDICARE','CIGNA','CIGNA MEDICARE','HUMANA','HUMANA MEDICARE','UHC','UHC MEDICARE','MEDCOST','TRICARE','Uninsured Rate','De-identified Minimum','De-identified Maximum','Patient Type']

	cone_in_df.columns = cone_headers

	cone_out_df.columns = cone_headers

	cone_df = pd.concat([cone_out_df, cone_in_df])

	cols = cone_df.columns.to_list()

	cone_df = cone_df.astype(str)

	for col in cols:
		cone_df[col] = cone_df[col].apply(lambda s: s.replace('$', '')).apply(lambda s: s.replace(' Not paid separately ', '')).apply(lambda s: s.replace('N/A', ''))
		try:
			# split columns in two and then average them together
			cone_df[col] = cone_df[col].str.split(' to ', expand=True)[1] 
		except Exception as e:
			pass
	
	return cone_df

df_ncb = get_north_carolina_baptist('north-carolina-baptist-hospital', hospital_urls, raw_download_path)

def curate_ncb(ncb_df: pd.DataFrame) -> pd.DataFrame:
    
    ncb_df.columns = [s.replace(' Managed Care', '') for s in ncb_df.columns.to_list()]
    
    return ncb_df


df_cdm_app, df_drg_app, df_shop_app = get_app('app-regional-health-system', hospital_urls, raw_download_path)

def curate_app(app_df: pd.DataFrame) -> pd.DataFrame:
    # drop column in app_df dataframe that is not needed
    app_df = app_df.drop(['BCBS State'], axis=1)

    return app_df

df_catawba = get_catawba('catawba-valley-medical-center', hospital_urls,  raw_download_path)

df_comp_cateret, df_desc_cateret = get_cateret('cateret-health-care', hospital_urls,  raw_download_path)

df_first_moore =  get_first('first-health-moore', hospital_urls, raw_download_path)

df_first_montgomery =  get_first('first-health-montgomery', hospital_urls, raw_download_path)

df_cdm_iredell, df_drg_iredell, df_drg_internet =  get_iredell('iredell-health', hospital_urls, raw_download_path)

df_mission = get_mission('mission-health', hospital_urls, raw_download_path)

df_nhrmc_op, df_nhrmc_ip = get_nhrmc('nhrmc-health', hospital_urls, raw_download_path)

df_northern = get_northern('northern-regional', hospital_urls, raw_download_path)

df_novant = get_novant('novant-health', hospital_urls, raw_download_path)

df_wakemed_raleigh = get_wakemed('wakemed-raleigh', hospital_urls, raw_download_path, driver_path)

# get_wakemed('wakemed-cary', hospital_urls, raw_download_path, driver_path)

# df_vidant = get_vidant('vidant-health', hospital_urls, raw_download_path)

# get_atrium('atrium-health', hospital_urls, raw_download_path)