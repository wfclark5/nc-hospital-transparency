from process.utils import *

# pass global date variable 


global date 

date =  datetime.datetime.now().strftime("%Y-%m-%d")


def curate_duke(duke_df:pd.DataFrame, curated_path:str) -> pd.DataFrame:

	duke_df = duke_df.astype(str)
	
	duke_df =  duke_df[['CPT/MS-DRG' ,'Procedure Description' ,'Gross Charge' ,'AETNA MEDICARE' ,'BCBS MEDICARE' ,
						'HUMANA MEDICARE' ,'UHC MEDICARE' ,'AETNA' ,'BCBS' ,'CIGNA' ,'Medcost' ,'UHC' ,'Tricare' ,
						'Self Pay' ,'De-identified Minimum' ,'De-identified Maximum' ,'Filename', 'Type']]

	cols = duke_df.columns.to_list()

	for col in cols:

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



	system = 'DUKE'

	duke_df['system'] = system

	filename = f'{system}_curated_{date}.csv'
	
	duke_df.to_csv(os.path.join(curated_path, filename), index=False)
	
	return duke_df

	
def curate_cone(cone_df: pd.DataFrame, curated_path:str) -> pd.DataFrame:

	cone_df = cone_df.astype(str)

	# strip trailing and leading white space in column names

	cone_df.columns = cone_df.columns.str.strip()


	cone_out_df = cone_df[['Procedure',
		'Code (CPT/HCPCS/MS-DRG)',
		'NDC (for medications/drugs)',
		'Rev Code',
		'Procedure Description',
		'Gross Charge',
		'Aetna Negotiated Outpatient Rate',
		'AETNA MEDICARE Negotiated Outpatient Rate',
		'BLUE CROSS BLUE SHIELD Negotiated Outpatient Rate',
		'BLUE CROSS BLUE SHIELD MEDICARE Negotiated Outpatient Rate',
		'CIGNA Negotiated Outpatient Rate',
		'CIGNA MEDICARE ADVANTAGE Negotiated Outpatient Rate',
		'HUMANA Negotiated Outpatient Rate',
		'HUMANA MEDICARE Negotiated Outpatient Rate',
		'UNITED HEALTHCARE Negotiated Outpatient Rate',
		'UNITED HEALTHCARE MEDICARE Negotiated Outpatient Rate',
		'MEDCOST Negotiated Outpatient Rate',
		'TRICARE Negotiated Outpatient Rate',
		'Uninsured Outpatient Rate',
		'De-identified Min Outpatient Negotiated Rate Across All Payers',
		'De-identified Max Outpatient Negotiated Rate Across All Payers'
		,'Filename']].copy()

	cone_out_df['Patient Type'] = 'Outpatient'


	cone_in_df = cone_df[['Procedure',
	'Code (CPT/HCPCS/MS-DRG)',
	'NDC (for medications/drugs)',
	'Rev Code',
	'Procedure Description',
	'Gross Charge',
	'Aetna Negotiated Inpatient Rate',
	'AETNA MEDICARE Negotiated Inpatient Rate',
	'BLUE CROSS BLUE SHIELD Negotiated Inpatient Rate',
	'BLUE CROSS BLUE SHIELD MEDICARE Negotiated Inpatient Rate',
	'CIGNA Negotiated Inpatient Rate',
	'CIGNA MEDICARE ADVANTAGE Negotiated Inpatient Rate',
	'HUMANA Negotiated Inpatient Rate',
	'HUMANA MEDICARE Negotiated Inpatient Rate',
	'UNITED HEALTHCARE Negotiated Inpatient Rate',
	'UNITED HEALTHCARE MEDICARE Negotiated Inpatient Rate',
	'MEDCOST Negotiated Inpatient Rate',
	'TRICARE Negotiated Inpatient Rate',
	'Uninsured Inpatient Rate',
	'De-identified Min Inpatient Negotiated Rate Across All Payers',
	'De-identified Max Inpatient Negotiated Rate Across All Payers'
	,'Filename']].copy()


	cone_in_df['Patient Type'] = 'Inpatient'

	cone_headers = ['Procedure', 'CPT/MS-DRG', 'NDC Code', 'Rev Code', 'Procedure Description','Gross Charge', 'AETNA',
						'AETNA MEDICARE', 'BCBS','BCBS MEDICARE','CIGNA','CIGNA MEDICARE','HUMANA','HUMANA MEDICARE','UHC','UHC MEDICARE',
						'Medcost','Tricare','Uninsured Rate','De-identified Minimum','De-identified Maximum','Patient Type', 'Filename']



	cone_in_df.columns, cone_out_df.columns = cone_headers, cone_headers

	cone_curated_df = pd.concat([cone_out_df, cone_in_df], axis=0)

	cols = cone_curated_df.columns.to_list()

	for col in cols:
		cone_curated_df[col] = cone_curated_df[col].apply(lambda s: s.replace('Ãƒâ€šÃ‚Â®', '')).apply(lambda s: s.replace('$', '')).apply(lambda s: s.replace(' Not paid separately ', '')).apply(lambda s: s.replace('N/A', ''))
		# try:
		# 	# split columns in two and then average them together
		# 	cone_curated_df[col] = cone_curated_df[col].str.split(' to ', expand=True)[1]
		# except Exception as e:
		# 	pass

	system = 'CONE'

	cone_curated_df['system'] = system

	filename = f'{system}_curated_{date}.csv'

	cone_curated_df.to_csv(os.path.join(curated_path, filename), index=False)

	return cone_curated_df


def curate_ncb(ncb_df: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	
	ncb_df.columns = [s.replace(' Managed Care', '') for s in ncb_df.columns.to_list()]

	system = 'NCB'

	ncb_df['system'] = system
	
	filename= f'{system}_curated_{date}.csv'

	ncb_df.to_csv(os.path.join(curated_path, filename), index=False)

	return ncb_df



def curate_app_shoppable(app_df: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	# drop column in app_df dataframe that is not needed
	app_df = app_df.drop(['BCBS State', 'CMS Required DRG/CPT/HCPCS', 'CMH FY21 Chg (estimate)'], axis=1)


	app_df.columns = ['CPT/MS-DRUG', 'Procedure Description', 'Self-Pay', 'Medicare',
	   					'Medicaid', 'Aetna', 'BCBS', 'Cigna', 'Medcost', 'UHC', 'Filename']

	system = 'APP'

	app_df['system'] = system

	filename= f'{system}_curated_{date}.csv'

	app_df.to_csv(os.path.join(curated_path, filename), index=False)

	return app_df


def curate_novant(novant_df: pd.DataFrame, curated_path:str) -> pd.DataFrame:

	novant_curated_df = novant_df[['Code Description', 
		'CPT/DRG', 
		'Gross Charge',
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
		'Filename']].copy()

	novant_curated_df.columns = ['Procedure Description', 
		'CPT/DRG', 
		'Gross Charge',
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
		'De-identified Minimum',
		'De-identified Maximum',
		'Filename']

	system = 'NOVANT'

	novant_curated_df['system'] = system

	filename= f'{system}_curated_{date}.csv'

	novant_curated_df.to_csv(os.path.join(curated_path, filename), index=False)

	return novant_curated_df




def curate_nhrmc(df_nhrmc_op: pd.DataFrame, df_nhrmc_ip: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	
	df_nhrmc_op['Cigna'].fillna(df_nhrmc_op['Cigna HMO/PPO'], inplace =True)

	df_nhrmc_op['Cigna'].fillna(df_nhrmc_op['Cigna HMO'], inplace =True)

	df_nhrmc_op['Humana'].fillna(df_nhrmc_op['Humana PPO'], inplace =True)
	
	df_nhrmc_op['Medcost'].fillna(df_nhrmc_op['MedCost'], inplace =True)
	
	df_nhrmc_op = df_nhrmc_op[['MS-DRG/APC','Average Charge', 'Description', 'Aetna',  
								'BCBS', 'Cigna', 'UHC',  'Medcost',  'Humana',  
								'De-Identified Minimum Negotiated Charge', 'De-Identified Maximum Negotiated Charge', 
								'Filename']].copy()
	
	df_nhrmc_op['Patient Type'] = 'Outpatient'

	print(df_nhrmc_ip.columns)
	
	df_nhrmc_ip['Cigna'].fillna(df_nhrmc_ip['Cigna HMO/PPO'], inplace =True)

	df_nhrmc_ip['Cigna'].fillna(df_nhrmc_ip['Cigna HMO'], inplace =True)
	
	df_nhrmc_ip['Medcost'].fillna(df_nhrmc_ip['MedCost'], inplace =True)
	
	df_nhrmc_ip = df_nhrmc_ip[['MS-DRG/APC','Average Charge', 'Description', 'Aetna',  'BCBS', 'Cigna', 'UHC',  
							   'Medcost',  'Humana',  'De-Identified Minimum Negotiated Charge', 
							   	'De-Identified Maximum Negotiated Charge', 'Filename']].copy()
	
	df_nhrmc_ip['Patient Type'] = 'Inpatient'
	
	nhrmc_curate_df = pd.concat([df_nhrmc_op, df_nhrmc_ip])

	system = 'NHRMC'

	nhrmc_curate_df['system'] = system

	filename= f'{system}_curated_{date}.csv'

	nhrmc_curate_df.to_csv(os.path.join(curated_path, filename), index=False)

	nhrmc_curate_df.columns = ['CPT/MS-DRG', 'Gross Charge', 'Procedure Description', 'Aetna', 'BCBS', 'Cigna',
	   							'UHC', 'Medcost', 'Humana', 'De-Identified Minimum', 'De-Identified Maximum', 
								'Filename', 'Patient Type', 'system']

	return nhrmc_curate_df



def curate_catawba(df_catawba: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	

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
   'Filename']].copy()
	
	df.columns = ['Rev Code',
   'CPT/MS-DRG',
   'Procedure Description',
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

	system = 'CATAWBA'

	df['system'] = 'CATAWBA'

	filename = f'{system}_curated_{date}.csv'

	df.to_csv(os.path.join(curated_path, filename), index=False)

	return df


def curate_northern(df_northern: pd.DataFrame, curated_path:str) -> pd.DataFrame:

	"""Pivot table on PAYER_NAME, PRIMARY_CODE and export out"""

	curated_north_df = df_northern[['PT_SUMMARY', 'PAYER_NAME', 'PRIMARY_CODE', 'PRIMARY_CODE_DESCRIPTION', 'GROSS_CHARGES', 'CASH_PRICE', 'MAX_NEGOTIATED_RATE', 'Filename']].copy()

	curated_north_df = curated_north_df.drop_duplicates(subset=['PAYER_NAME', 'PRIMARY_CODE'], keep='first')

	curated_north_df = curated_north_df.pivot(index=['PRIMARY_CODE', 'PRIMARY_CODE_DESCRIPTION',  'CASH_PRICE', 'GROSS_CHARGES', 'PT_SUMMARY', 'Filename'], columns=['PAYER_NAME'], values=['MAX_NEGOTIATED_RATE']).stack(level=[0]).reset_index()

	curated_north_df['BCBS'] = curated_north_df['BCBS OF NC'].fillna(curated_north_df['BLUE CROSS OF NORTH CAROLINA'])

	curated_north_df.reset_index(drop=True, inplace=True)

	curated_north_df.rename(columns={'PRIMARY_CODE': 'CPT/MS-DRG', 
									'PT_SUMMARY': 'Patient Type', 
									'CASH_PRICE':'Self Pay', 
									'GROSS_CHARGES' : 'Gross Charges',
									'PRIMARY_CODE_DESCRIPTION': 'Procedure Description', 
									'BLUE MEDICARE/PARTNERS NHP' : 'BCBS MEDICARE', 
									'UNITED HEALTHCARE' : 'UHC', 
									'UNITED HEALTHCARE MEDICARE' : 'UHC MEDICARE', 
									'CIGNA MEDICARE ACCESS' : 'CIGNA MEDICARE', 
									'HUMANA GOLD CHOICE MEDICARE': 'HUMANA MEDICARE', 
									'TRICARE FOR LIFE': 'TRICARE'},  inplace=True)

	
	system = 'NORTHERN'

	curated_north_df['system'] = system

	curated_north_df = curated_north_df[['CPT/MS-DRG', 'Patient Type', 'Procedure Description', 'Gross Charges', 'AETNA', 
											 'AETNA MEDICARE',  'BCBS', 'BCBS MEDICARE', 'CIGNA', 'CIGNA MEDICARE', 'UHC', 
											'UHC MEDICARE', 'HUMANA MEDICARE', 'MEDCOST', 'TRICARE', 'Self Pay', 'Filename', 'system']].copy()

	filename= f'{system}_curated_{date}.csv'

	curated_north_df.to_csv(os.path.join(curated_path, filename), index=False)

	return curated_north_df
