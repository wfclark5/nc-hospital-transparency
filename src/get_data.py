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


curate_cone_df = curate_cone(df_cone)



df_ncb = get_north_carolina_baptist('north-carolina-baptist-hospital', hospital_urls, raw_download_path)

def curate_ncb(ncb_df: pd.DataFrame) -> pd.DataFrame:
    
    ncb_df.columns = [s.replace(' Managed Care', '') for s in ncb_df.columns.to_list()]
    
    return ncb_df

curate_ncb_df = curate_ncb(df_ncb)




df_cdm_app, df_drg_app, df_shop_app = get_app('app-regional-health-system', hospital_urls, raw_download_path)

def curate_app_shoppable(app_df: pd.DataFrame) -> pd.DataFrame:
    # drop column in app_df dataframe that is not needed
    app_df = app_df.drop(['BCBS State'], axis=1)

    return app_df


curate_app_df = curate_app_shoppable(df_shop_app)



df_novant = get_novant('novant-health', hospital_urls, raw_download_path)

def curate_novant(novant_df: pd.DataFrame) -> pd.DataFrame:

    novant_curated_df = df_novant[['Code Description', 
        'CPT/DRG', 
        'Gross Charge',
        'Discounted cash price',
        'Aetna', 
        'Aetna Medicare', 
        'Blue Medicare',
        'Blue Cross NC', 
        'Cigna Healthcare', 
        'Cigna Medicare',
        'Humana ChoiceCare', 
        'Humana Medicare', 
        'UHC',
        'UHC MCR ADV',
        'Tricare', 
        'MedCost - MBS', 
        'De-identified minimum negotiated charge',
        'De-identified maximum negotiated charge', 
        'Filename']]

    novant_curated_df.columns = ['Code Description', 
        'CPT/DRG', 
        'Gross Charge',
        'Discounted Cash Price',
        'Aetna', 
        'Aetna Medicare', 
        'BCBS Medicare',
        'BCBS', 
        'Cigna', 
        'Cigna Medicare',
        'Humana', 
        'Humana Medicare', 
        'UHC',
        'UHC Medicare',
        'Tricare', 
        'MedCost', 
        'De-identified minimum negotiated charge',
        'De-identified maximum negotiated charge',
        'Filename']

    return novant_curated_df

curate_novant_df = curate_novant(df_novant)

df_nhrmc_op, df_nhrmc_ip = get_nhrmc('nhrmc-health', hospital_urls, raw_download_path)

def curate_nhrmc(df_nhrmc_op: pd.DataFrame, df_nhrmc_ip: pd.DataFrame) -> pd.DataFrame:
    
    df_nhrmc_op['Cigna'].fillna(df_nhrmc_op['Cigna HMO/PPO'], inplace =True)
    
    df_nhrmc_op['Humana'].fillna(df_nhrmc_op['Humana PPO'], inplace =True)
    
    df_nhrmc_op['Medcost'].fillna(df_nhrmc_op['MedCost'], inplace =True)
    
    df_nhrmc_op = df_nhrmc_op[['MS-DRG/APC','Average Charge', 'Description', 'Aetna',  'BCBS', 'Cigna', 'UHC',  'Cigna HMO', 'Medcost',  'Humana',  'De-Identified Minimum Negotiated Charge', 'De-Identified Maximum Negotiated Charge', 'Filename']]
    
    df_nhrmc_op['Patient Type'] = 'Outpatient'
    
    df_nhrmc_ip['Cigna'].fillna(df_nhrmc_ip['Cigna HMO/PPO'], inplace =True)
    
    df_nhrmc_ip['Humana'].fillna(df_nhrmc_ip['Humana PPO'], inplace =True)
    
    df_nhrmc_ip['Medcost'].fillna(df_nhrmc_ip['MedCost'], inplace =True)
    
    df_nhrmc_ip = df_nhrmc_ip[['MS-DRG/APC','Average Charge', 'Description', 'Aetna',  'BCBS', 'Cigna', 'UHC',  'Cigna HMO', 'Medcost',  'Humana',  'De-Identified Minimum Negotiated Charge', 'De-Identified Maximum Negotiated Charge', 'Filename']]
    
    df_nhrmc_ip['Patient Type'] = 'Inpatient'
    
    nhrmc_curate_df = pd.concat([df_nhrmc_op, df_nhrmc_ip])

    return nhrmc_curate_df

curate_nhrmc_df = curate_nhrmc(df_nhrmc_op, df_nhrmc_ip)

df_catawba = get_catawba('catawba-valley-medical-center', hospital_urls,  raw_download_path)

def curate_catawba(df_catawba: pd.DataFrame) -> pd.DataFrame:
    
  df_catawba['CIGNA HEALTHSPRING_Max_Allowable'] = df_catawba['CIGNA HEALTHSPRING_Max_Allowable'].fillna(df_catawba['CIGNA HEALTH AND LIFE INSURANCE COMPANY_Max_Allowable']).fillna(df_catawba['CIGNA_Max_Allowable'])
  
  df = df_catawba[['HospCode',
   'Code',
   'Description',
   'Code_Type',
   'CMS Shoppable',
   'Package/Line_Level',
   'Min_Allowable_835',
   'Max_Allowable_835',
   'AETNA_Max_Allowable',
   'BCBS OF NC_Max_Allowable',
   'CIGNA HEALTHSPRING_Max_Allowable',
   'HUMANA INC._Max_Allowable',
   'UNITED HEALTHCARE INSURANCE COMPANY_Max_Allowable', 
   'Filename']]

  df.columns = ['Rev Code',
   'DRG/CPT/NDC Code',
   'Description',
   'Code_Type',
   'CMS Shoppable',
   'Package/Line_Level',
   'De-identified Minimum',
   'De-identified Maximum',
   'Aetna',
   'BCBS',
   'Cigna',
   'Humana',
   'UHC',
   'Filename']

  return df

curate_catawba_df = curate_catawba(df_catawba)




df_first_moore =  get_first('first-health-moore', hospital_urls, raw_download_path)

df_first_montgomery =  get_first('first-health-montgomery', hospital_urls, raw_download_path)

df_cdm_iredell, df_drg_iredell, df_drg_internet =  get_iredell('iredell-health', hospital_urls, raw_download_path)

df_northern = get_northern('northern-regional', hospital_urls, raw_download_path)

df_wakemed_cary = get_wakemed('wakemed-cary', hospital_urls, raw_download_path, driver_path)

df_wakemed_raleigh = get_wakemed('wakemed-raleigh', hospital_urls, raw_download_path, driver_path)

df_vidant = get_vidant('vidant-health', hospital_urls, raw_download_path)

df_atrium = get_atrium('atrium-health', hospital_urls, raw_download_path)




df_comp_cateret, df_desc_cateret = get_cateret('cateret-health-care', hospital_urls,  raw_download_path) #Get's charge master not standard charges

df_mission = get_mission('mission-health', hospital_urls, raw_download_path) #Get's charge master not standard charges