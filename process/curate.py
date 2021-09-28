from process.utils import *

# pass global date variable 


global date 

date =  datetime.datetime.now().strftime("%Y-%m")


def curate_duke(df_duke:pd.DataFrame, curated_path:str) -> pd.DataFrame:

	df_duke = df_duke.astype(str)
	
	df_duke =  df_duke[['CPT/MS-DRG' ,'Procedure Description' ,'Gross Charge' ,'AETNA MEDICARE' ,'BCBS MEDICARE' ,
						'HUMANA MEDICARE' ,'UHC MEDICARE' ,'AETNA' ,'BCBS' ,'CIGNA' ,'Medcost' ,'UHC' ,'Tricare' ,
						'Self Pay' ,'De-identified Minimum' ,'De-identified Maximum' ,'Filename', 'Type']]

	cols = df_duke.columns.to_list()

	for col in cols:

		df_duke[col] = df_duke[col].apply(lambda s: s.replace(' $0.00 for routine inpatient services and up to $', '')).apply(lambda s: s.replace(' for all other services, depending on the circumstances ', '')).apply(lambda s: s.replace(' $0.00 for routine inpatient services; negotiated from ', '')).apply(lambda s: s.replace('$', ''))
		
		try:
				df_duke[col] = df_duke[col].str.split(' to ', expand=True)[1]
		except:
			pass

		else:
			df_duke = df_duke.replace('Payment data is not available for this DRG for this entity for this payer', '')
			try:
				df_duke[col] = df_duke[col].str.split(', ', expand=True)[0].apply(lambda s: s.replace('Average payment of ', ''))
			except:
				pass



	system = 'DUKE'

	df_duke['system'] = system

	filename = f'{system}_curated_{date}.csv'
	
	df_duke.to_csv(os.path.join(curated_path, filename), index=False)
	
	return df_duke

	
def curate_cone(df_cone: pd.DataFrame, curated_path:str) -> pd.DataFrame:

	df_cone = df_cone.astype(str)

	# strip trailing and leading white space in column names

	df_cone.columns = df_cone.columns.str.strip()


	cone_out_df = df_cone[['Procedure',
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


	cone_in_df = df_cone[['Procedure',
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


def curate_ncb(df_ncb: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	
	df_ncb.columns = [s.replace(' Managed Care', '') for s in df_ncb.columns.to_list()]

	system = 'BAPTIST'

	df_ncb['system'] = system
	
	filename= f'{system}_curated_{date}.csv'

	df_ncb.to_csv(os.path.join(curated_path, filename), index=False)

	return df_ncb



def curate_app_shoppable(df_app: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	# drop column in df_app dataframe that is not needed
	df_app = df_app.drop(['BCBS State', 'CMS Required DRG/CPT/HCPCS', 'CMH FY21 Chg (estimate)'], axis=1)


	df_app.columns = ['CPT/MS-DRUG', 'Procedure Description', 'Self-Pay', 'Medicare',
	   					'Medicaid', 'Aetna', 'BCBS', 'Cigna', 'Medcost', 'UHC', 'Filename']

	system = 'APP'

	df_app['system'] = system

	filename= f'{system}_curated_{date}.csv'

	df_app.to_csv(os.path.join(curated_path, filename), index=False)

	return df_app


def curate_novant(df_novant: pd.DataFrame, curated_path:str) -> pd.DataFrame:

	novant_curated_df = df_novant[['Code Description', 
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

def curate_wakemed(df_wakemed:pd.DataFrame, curated_path:str) -> pd.DataFrame:
	"""Combine payors into one series"""

	df = df_wakemed[df_wakemed['payer'].notna()]

	df = df.drop_duplicates(subset=['CPT/MS-DRG', 'payer'])

	df = df.pivot(index=['CPT/MS-DRG', 'Patient Type', 'Self Pay', 'Gross Charge', 'De-identified Maximum', 
		'De-identified Minimum', 'Filename'], columns='payer', values='Payer_Allowed_Amount')

	df.reset_index(inplace=True)

	df['AETNA'] = combine_related([df["AETNA COMMERCIAL"], df["AETNA PPO"], df["AETNA HMO"]])
	df['UHC'] = combine_related([df["UNITED HEALTHCARE COMMERCIAL"], df["UNITED HEALTHCARE MEDICARE ADVANTAGE"], df["UNITED HEALTHCARE PPO"], df["UNITED HEALTHCARE HMO"]])
	df['BCBS'] = combine_related([df["BCBS HMO"], df["BCBS COMMERCIAL"], df["BCBS PPO"]])
	df['HUMANA'] = df["HUMANA PPO"]
	df['HUMANA MEDICARE'] = df["HUMANA MEDICARE ADVANTAGE"]
	df['CIGNA'] = df["CIGNA COMMERCIAL"]

	df.drop(columns=['AETNA COMMERCIAL', 'AETNA PPO', 'AETNA HMO', 'UNITED HEALTHCARE COMMERCIAL', 
					'UNITED HEALTHCARE MEDICARE ADVANTAGE', 'UNITED HEALTHCARE PPO', 'BCBS HMO', 
					'BCBS COMMERCIAL', 'BCBS PPO', 'HUMANA PPO', 'HUMANA MEDICARE ADVANTAGE', 
					'CIGNA COMMERCIAL', 'CONNECTICUT GENERAL COMMERCIAL', 'GEISINGER HEALTHPLAN HMO',
       				'GEISINGER HEALTHPLAN PPO', 'KAISER FOUNDATION HEALTH PLAN HMO',
       				'NC STATE HEALTH PLAN PPO', 'UNITED HEALTHCARE HMO'], inplace=True)
	

	system = 'WAKEMED'
	
	df['system'] = system
	
	filename= f'{system}_curated_{date}.csv'

	df.to_csv(os.path.join(curated_path, filename), index=False)

	return df

	
def curate_first(df_first:pd.DataFrame, curated_path:str) -> pd.DataFrame:
	"""Curates first health into standard format"""

	df = df_first.rename(columns={'payor.name': 'payer',
									'payor.maxAllowable': 'rate', 
									'code': 'CPT/MS-DRG', 
									'description': 'Procedure Description', 
									'grossCharge': 'Gross Charge'})

	df = df[['payer', 'rate', 'CPT/MS-DRG', 'Procedure Description', 'Gross Charge', 'Filename']]

	df.drop_duplicates(subset=['payer', 'CPT/MS-DRG'], inplace=True)

	df = df.pivot(index=['CPT/MS-DRG', 'Procedure Description', 'Gross Charge', 'Filename'], columns='payer', values='rate')

	df.reset_index(inplace=True)

	bcbs = [df["BCBSNC HOST"], df["BCBS OF NC"], df["BLUE CROSS BLUE SHIELD OF NORTH CAROLINA"], df["BCBSNC-BLUE CROSS BLUE SHIELD"]] 
	humana_medicare = [df["HUMANA INC. MEDICARE ADVANTAGE PPO"], df["HUMANA INC. MEDICARE ADVANTAGE HMO"]] 
	tricare = [df["TRICARE EAST"], df["TRICARE TDEFIC"], df["TRIWEST HEALTHCARE ALLIANCE"]] 
	aetna = [df["AETNA"], df["AETNA HEALTH AND LIFE INSURANCE COMPANY"], df["AETNA AMERICAN CONTINENTAL INSURANCE COMPANY"], df["AETNA HEALTH INS COMPANY"], df["AETNA SEHBP EDUCATORS MEDICARE PLAN NET 04259"]] 
	uhc = df["UNITED HEALTHCARE INSURANCE COMPANY"]
	uhc_medicare = [df["UNITED HEALTHCARE INSURANCE COMPANY AARP MEDICARE ADVANTAGE CHOICE"], df["UNITED HEALTHCARE INSURANCE COMPANY AARP MEDICARE ADVANTAGE PLAN 1"], df["UNITED HEALTHCARE INSURANCE COMPANY AARP MEDICARE ADVANTAGE PLAN 2"]] 
	cigna = df["CIGNA HEALTH AND LIFE INSURANCE COMPANY"] 
	humana = df["HUMANA INC."]



	df['AETNA'] = combine_related(aetna)
	df['AETNA MEDICARE'] = None
	df['UHC'] = uhc
	df['UHC MEDICARE'] = combine_related(uhc_medicare)
	df['BCBS'] = combine_related(bcbs)
	df['BCBS MEDICARE'] = None
	df['HUMANA'] = humana
	df['HUMANA MEDICARE'] = combine_related(humana_medicare)
	df['CIGNA'] = cigna
	df['CIGNA MEDICARE'] = None
	df['TRICARE'] = combine_related(tricare)


	df = df[['CPT/MS-DRG', 'Procedure Description', 'Gross Charge', 'AETNA', 
			'AETNA MEDICARE', 'UHC', 'UHC MEDICARE', 'BCBS', 'BCBS MEDICARE', 
			'HUMANA', 'HUMANA MEDICARE', 'CIGNA', 'CIGNA MEDICARE', 'TRICARE', 'MEDCOST', 'Filename']]
	

	system = 'FIRST'
	
	df['system'] = system
	
	filename= f'{system}_curated_{date}.csv'

	df.to_csv(os.path.join(curated_path, filename), index=False)

	return df

