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

	df_cone = pd.concat([cone_out_df, cone_in_df], axis=0)

	del cone_in_df, cone_out_df

	cols = df_cone.columns.to_list()

	for col in cols:
		df_cone[col] = df_cone[col].apply(lambda s: s.replace('Ãƒâ€šÃ‚Â®', '')).apply(lambda s: s.replace('$', '')).apply(lambda s: s.replace(' Not paid separately ', '')).apply(lambda s: s.replace('N/A', ''))
		# try:
		# 	# split columns in two and then average them together
		# 	df_cone[col] = df_cone[col].str.split(' to ', expand=True)[1]
		# except Exception as e:
		# 	pass

	system = 'CONE'

	df_cone['system'] = system

	filename = f'{system}_curated_{date}.csv'

	df_cone.to_csv(os.path.join(curated_path, filename), index=False)

	return df_cone


def curate_ncb(df_ncb: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	
	df_ncb.columns = [s.replace(' Managed Care', '') for s in df_ncb.columns.to_list()]

	system = 'BAPTIST'

	df_ncb['system'] = system
	
	filename= f'{system}_curated_{date}.csv'

	df_ncb.to_csv(os.path.join(curated_path, filename), index=False)

	return df_ncb



def curate_app(df_app: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	# drop column in df_app dataframe that is not needed
	df_app = df_app.drop(['BCBS State', 'CMS Required DRG/CPT/HCPCS', 'CMH FY21 Chg (estimate)'], axis=1)


	df_app.columns = ['CPT/MS-DRUG', 'Procedure Description', 'Self Pay', 'Medicare',
	   					'Medicaid', 'Aetna', 'BCBS', 'Cigna', 'Medcost', 'UHC', 'Filename']

	system = 'APP'

	df_app['system'] = system

	filename= f'{system}_curated_{date}.csv'

	df_app.to_csv(os.path.join(curated_path, filename), index=False)

	return df_app


def curate_novant(df_novant: pd.DataFrame, curated_path:str) -> pd.DataFrame:

	df_novant = df_novant[['Code Description', 
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

	df_novant.columns = ['Procedure Description', 
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

	df_novant['system'] = system

	filename= f'{system}_curated_{date}.csv'

	df_novant.to_csv(os.path.join(curated_path, filename), index=False)

	return df_novant




def curate_nhrmc(df_nhrmc: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	
	df_nhrmc['Cigna'].fillna(df_nhrmc['Cigna HMO/PPO'], inplace =True)

	df_nhrmc['Cigna'].fillna(df_nhrmc['Cigna HMO'], inplace =True)

	df_nhrmc['Humana'].fillna(df_nhrmc['Humana PPO'], inplace =True)
	
	df_nhrmc['Medcost'].fillna(df_nhrmc['MedCost'], inplace =True)
	
	system = 'NHRMC'

	df_nhrmc['system'] = system

	df_nhrmc = df_nhrmc[['MS-DRG/APC', 'Patient Type', 'Average Charge', 'Description', 'Aetna', 'BCBS', 'Cigna', 'UHC',  
								'Medcost', 'Humana', 'De-Identified Minimum Negotiated Charge', 
								'De-Identified Maximum Negotiated Charge', 'Filename', 'system']].copy()

	filename= f'{system}_curated_{date}.csv'


	df_nhrmc.columns = ['CPT/MS-DRG', 'Patient Type', 'Gross Charge', 'Procedure Description', 'Aetna', 'BCBS', 'Cigna',
	   							'UHC', 'Medcost', 'Humana', 'De-Identified Minimum', 'De-Identified Maximum', 
								'Filename',  'system']

	df_nhrmc.to_csv(os.path.join(curated_path, filename), index=False)

	return df_nhrmc



def curate_catawba(df_catawba: pd.DataFrame, curated_path:str) -> pd.DataFrame:
	

	df_catawba['CIGNA HEALTHSPRING_Max_Allowable'] = df_catawba['CIGNA HEALTHSPRING_Max_Allowable'].fillna(df_catawba['CIGNA HEALTH AND LIFE INSURANCE COMPANY_Max_Allowable']).fillna(df_catawba['CIGNA_Max_Allowable'])
	
	df_catawba = df_catawba[['HospCode',
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
	
	df_catawba.columns = ['Rev Code',
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

	df_catawba['system'] = 'CATAWBA'

	filename = f'{system}_curated_{date}.csv'

	df_catawba.to_csv(os.path.join(curated_path, filename), index=False)

	return df_catawba


def curate_northern(df_northern: pd.DataFrame, curated_path:str) -> pd.DataFrame:

	"""Pivot table on PAYER_NAME, PRIMARY_CODE and export out"""

	df_northern = df_northern[['PT_SUMMARY', 'PAYER_NAME', 'PRIMARY_CODE', 'PRIMARY_CODE_DESCRIPTION', 'GROSS_CHARGES', 'CASH_PRICE', 'MAX_NEGOTIATED_RATE', 'Filename']].copy()

	df_northern = df_northern.drop_duplicates(subset=['PAYER_NAME', 'PRIMARY_CODE'], keep='first')

	df_northern = df_northern.pivot(index=['PRIMARY_CODE', 'PRIMARY_CODE_DESCRIPTION',  'CASH_PRICE', 'GROSS_CHARGES', 'PT_SUMMARY', 'Filename'], columns=['PAYER_NAME'], values=['MAX_NEGOTIATED_RATE']).stack(level=[0]).reset_index()

	df_northern['BCBS'] = df_northern['BCBS OF NC'].fillna(df_northern['BLUE CROSS OF NORTH CAROLINA'])

	df_northern.reset_index(drop=True, inplace=True)

	df_northern.rename(columns={'PRIMARY_CODE': 'CPT/MS-DRG', 
									'PT_SUMMARY': 'Patient Type', 
									'CASH_PRICE':'Self Pay', 
									'GROSS_CHARGES' : 'Gross Charge',
									'PRIMARY_CODE_DESCRIPTION': 'Procedure Description', 
									'BLUE MEDICARE/PARTNERS NHP' : 'BCBS MEDICARE', 
									'UNITED HEALTHCARE' : 'UHC', 
									'UNITED HEALTHCARE MEDICARE' : 'UHC MEDICARE', 
									'CIGNA MEDICARE ACCESS' : 'CIGNA MEDICARE', 
									'HUMANA GOLD CHOICE MEDICARE': 'HUMANA MEDICARE', 
									'TRICARE FOR LIFE': 'TRICARE'},  inplace=True)

	
	system = 'NORTHERN'

	df_northern['system'] = system

	df_northern = df_northern[['CPT/MS-DRG', 'Patient Type', 'Procedure Description', 'Gross Charge', 'AETNA', 
											 'AETNA MEDICARE',  'BCBS', 'BCBS MEDICARE', 'CIGNA', 'CIGNA MEDICARE', 'UHC', 
											'UHC MEDICARE', 'HUMANA MEDICARE', 'MEDCOST', 'TRICARE', 'Self Pay', 'Filename', 'system']].copy()

	filename= f'{system}_curated_{date}.csv'

	df_northern.to_csv(os.path.join(curated_path, filename), index=False)

	return df_northern

def curate_wakemed(df_wakemed:pd.DataFrame, curated_path:str) -> pd.DataFrame:
	"""Combine payors into one series"""

	df_wakemed = df_wakemed[df_wakemed['payer'].notna()]

	df_wakemed = df_wakemed.drop_duplicates(subset=['CPT/MS-DRG', 'payer'])

	df_wakemed = df_wakemed.pivot(index=['CPT/MS-DRG', 'Patient Type', 'Self Pay', 'Gross Charge', 'De-identified Maximum', 
		'De-identified Minimum', 'Filename'], columns='payer', values='Payer_Allowed_Amount')

	df_wakemed.reset_index(inplace=True)

	df_wakemed['AETNA'] = combine_related([df_wakemed["AETNA COMMERCIAL"], df_wakemed["AETNA PPO"], df_wakemed["AETNA HMO"]])
	df_wakemed['UHC'] = combine_related([df_wakemed["UNITED HEALTHCARE COMMERCIAL"], df_wakemed["UNITED HEALTHCARE MEDICARE ADVANTAGE"], df_wakemed["UNITED HEALTHCARE PPO"], df_wakemed["UNITED HEALTHCARE HMO"]])
	df_wakemed['BCBS'] = combine_related([df_wakemed["BCBS HMO"], df_wakemed["BCBS COMMERCIAL"], df_wakemed["BCBS PPO"]])
	df_wakemed['HUMANA'] = df_wakemed["HUMANA PPO"]
	df_wakemed['HUMANA MEDICARE'] = df_wakemed["HUMANA MEDICARE ADVANTAGE"]
	df_wakemed['CIGNA'] = df_wakemed["CIGNA COMMERCIAL"]

	df_wakemed.drop(columns=['AETNA COMMERCIAL', 'AETNA PPO', 'AETNA HMO', 'UNITED HEALTHCARE COMMERCIAL', 
					'UNITED HEALTHCARE MEDICARE ADVANTAGE', 'UNITED HEALTHCARE PPO', 'BCBS HMO', 
					'BCBS COMMERCIAL', 'BCBS PPO', 'HUMANA PPO', 'HUMANA MEDICARE ADVANTAGE', 
					'CIGNA COMMERCIAL', 'CONNECTICUT GENERAL COMMERCIAL', 'GEISINGER HEALTHPLAN HMO',
       				'GEISINGER HEALTHPLAN PPO', 'KAISER FOUNDATION HEALTH PLAN HMO',
       				'NC STATE HEALTH PLAN PPO', 'UNITED HEALTHCARE HMO'], inplace=True)
	

	system = 'WAKEMED'
	
	df_wakemed['system'] = system
	
	filename= f'{system}_curated_{date}.csv'

	df_wakemed.to_csv(os.path.join(curated_path, filename), index=False)

	return df_wakemed

	
def curate_first(df_first:pd.DataFrame, curated_path:str) -> pd.DataFrame:
	"""Curates first health into standard format"""

	df_first = df_first.rename(columns={'payor.name': 'payer',
									'payor.maxAllowable': 'rate', 
									'code': 'CPT/MS-DRG', 
									'description': 'Procedure Description', 
									'grossCharge': 'Gross Charge'})

	df_first = df_first[['payer', 'rate', 'CPT/MS-DRG', 'Procedure Description', 'Gross Charge', 'Filename']]

	df_first.drop_duplicates(subset=['payer', 'CPT/MS-DRG'], inplace=True)

	df_first = df_first.pivot(index=['CPT/MS-DRG', 'Procedure Description', 'Gross Charge', 'Filename'], columns='payer', values='rate')

	df_first.reset_index(inplace=True)

	bcbs = [df_first["BCBSNC HOST"], df_first["BCBS OF NC"], df_first["BLUE CROSS BLUE SHIELD OF NORTH CAROLINA"], df_first["BCBSNC-BLUE CROSS BLUE SHIELD"]] 
	humana_medicare = [df_first["HUMANA INC. MEDICARE ADVANTAGE PPO"], df_first["HUMANA INC. MEDICARE ADVANTAGE HMO"]] 
	tricare = [df_first["TRICARE EAST"], df_first["TRICARE TDEFIC"]] 
	aetna = [df_first["AETNA"], df_first["AETNA HEALTH AND LIFE INSURANCE COMPANY"], df_first["AETNA AMERICAN CONTINENTAL INSURANCE COMPANY"], df_first["AETNA HEALTH INS COMPANY"], df_first["AETNA SEHBP EDUCATORS MEDICARE PLAN NET 04259"]] 
	uhc = df_first["UNITED HEALTHCARE INSURANCE COMPANY"]
	uhc_medicare = [df_first["UNITED HEALTHCARE INSURANCE COMPANY AARP MEDICARE ADVANTAGE CHOICE"], df_first["UNITED HEALTHCARE INSURANCE COMPANY AARP MEDICARE ADVANTAGE PLAN 1"], df_first["UNITED HEALTHCARE INSURANCE COMPANY AARP MEDICARE ADVANTAGE PLAN 2"]] 
	cigna = df_first["CIGNA HEALTH AND LIFE INSURANCE COMPANY"] 
	humana = df_first["HUMANA INC."]



	df_first['AETNA'] = combine_related(aetna)
	df_first['AETNA MEDICARE'] = None
	df_first['UHC'] = uhc
	df_first['UHC MEDICARE'] = combine_related(uhc_medicare)
	df_first['BCBS'] = combine_related(bcbs)
	df_first['BCBS MEDICARE'] = None
	df_first['HUMANA'] = humana
	df_first['HUMANA MEDICARE'] = combine_related(humana_medicare)
	df_first['CIGNA'] = cigna
	df_first['CIGNA MEDICARE'] = None
	df_first['TRICARE'] = combine_related(tricare)


	df_first = df_first[['CPT/MS-DRG', 'Procedure Description', 'Gross Charge', 'AETNA', 
			'AETNA MEDICARE', 'UHC', 'UHC MEDICARE', 'BCBS', 'BCBS MEDICARE', 
			'HUMANA', 'HUMANA MEDICARE', 'CIGNA', 'CIGNA MEDICARE', 'TRICARE', 'MEDCOST', 'Filename']]
	

	system = 'FIRST'
	
	df_first['system'] = system
	
	filename= f'{system}_curated_{date}.csv'

	df_first.to_csv(os.path.join(curated_path, filename), index=False)

	return df_first

def curate_atrium(df_atrium:pd.DataFrame, curated_path:str) -> pd.DataFrame:
	
	df_atrium_ip = df_atrium[['Code',  'Procedure Description', 'Payer', 'Inpatient Gross Charge', 'Inpatient Negotiated Charge', 'Filename']].copy()

	df_atrium_ip['Patient Type'] = 'Inpatient'

	df_atrium_op = df_atrium[['Code',  'Procedure Description', 'Payer', 'Outpatient Gross Charge', 'Outpatient Negotiated Charge', 'Filename']].copy()

	df_atrium_op['Patient Type'] = 'Outpatient'

	df_atrium_fac = df_atrium[['Code',  'Procedure Description', 'Payer',   'Gross Charge - Facility', 'Filename']].copy()

	df_atrium_fac['Patient Type'] = 'Facility'
		
	df_atrium_nonfac = df_atrium[['Code',  'Procedure Description', 'Payer',   'Gross Charge - Non-Facility', 'Filename']].copy()

	df_atrium_nonfac['Patient Type'] = 'Non-Facility'

	del df_atrium

	replace = {"Code": "CPT/MS-DRG", 
				"Inpatient Gross Charge" : "Gross Charge", 
				"Inpatient Negotiated Charge": "rate",
				"Outpatient Gross Charge" : "Gross Charge", 
				"Outpatient Negotiated Charge": "rate", 
				"Gross Charge - Facility" : "Gross Charge", 
				"Negotiated Charge - Facility": "rate", 
				"Gross Charge - Non-Facility": "Gross Charge", 
				"Negotiated Charge - Non-Facility": "rate"}


	df_atrium_ip.rename(columns=replace, inplace=True)

	df_atrium_op.rename(columns=replace, inplace=True)

	df_atrium_fac.rename(columns=replace, inplace=True)

	df_atrium_nonfac.rename(columns=replace, inplace=True)

	df_list = [df_atrium_ip, df_atrium_op, df_atrium_fac, df_atrium_nonfac]

	del df_atrium_ip, df_atrium_op, df_atrium_fac, df_atrium_nonfac 

	df_atrium = pd.concat(df_list)

	df_atrium = df_atrium[(df_atrium['CPT/MS-DRG'].notnull()) & (df_atrium['CPT/MS-DRG'] != 'All Codes') & (df_atrium['Payer'] != 'nan')]

	df_atrium = df_atrium.astype(str)

	df_atrium['rate'] = df_atrium['rate'].apply(lambda s: s.replace('$', ''))

	df_atrium = df_atrium[["Procedure Description", "CPT/MS-DRG", "Payer", "Patient Type", "Gross Charge", "rate"]]

	df_atrium = df_atrium.pivot_table(index=["Procedure Description", "CPT/MS-DRG", 'Patient Type', 'Gross Charge'], columns="Payer", values=["rate"], aggfunc='max')

	df_atrium = df_atrium.stack(level=[0]).reset_index()

	df_atrium['UHC MEDICARE'] = combine_related([df_atrium['UHC AARP MEDICARE COMPLETE MA [373]'], df_atrium['UHC MEDICARE COMPLETE CHOICE MA [388]']])

	df_atrium = df_atrium[['Procedure Description', 'CPT/MS-DRG', 'Patient Type', 'Gross Charge', 'BCBS NC', 'BLUEMEDICARE [362]', 'AETNA [100]', 'AETNA MEDICARE ADV [113]', 'CIGNA [102]',
		'CIGNA MEDICARE ADV [115]', 'HUMANA [103]', 'HUMANA MEDICARE ADV [117]', 
		'MEDCOST [196]', 'UNITED HEALTHCARE [101]', 'UHC MEDICARE']]

	df_atrium.columns = ['Procedure Description', 'CPT/MS-DRG', 'Patient Type', 'Gross Charge', 'BCBS NC', 'BCBS MEDICARE', 'AETNA', 'AETNA MEDICARE', 'CIGNA',
		'CIGNA MEDICARE', 'HUMANA', 'HUMANA MEDICARE', 
		'MEDCOST', 'UNITED HEALTHCARE', 'UHC MEDICARE']
		
	df_atrium = df_atrium.replace({'nan': np.nan})

	df_atrium.dropna(subset=['Gross Charge','BCBS NC', 'BCBS MEDICARE', 'AETNA', 'AETNA MEDICARE', 'CIGNA',
		'CIGNA MEDICARE', 'HUMANA', 'HUMANA MEDICARE', 
		'MEDCOST', 'UNITED HEALTHCARE', 'UHC MEDICARE'], how='all', inplace=True)

	system = 'ATRIUM'

	df_atrium['system'] = system

	filename= f'{system}_curated_{date}.csv'

	df_atrium.to_csv(os.path.join(curated_path, filename), index=False)

	return df_atrium




def curate_vidant(df_vidant: pd.DataFrame, curated_path:str) -> pd.DataFrame:
# map and join multindex in test.columns to be the same join blank if column name is unnamed

	cols = df_vidant.columns.map('|'.join).str.strip('|').values.tolist()

	cols_lst = []

	for col in cols:
		split = col.split('|')
		if 'Unnamed' in split[0]:
			cols_lst.append(split[1])
		elif 'Filename' in split[0]:
			cols_lst.append(split[0])
		else:
			cols_lst.append(split[0] + ' ' + split[1])

	df_vidant.columns = cols_lst

	# set main cols

	main_cols = ['CPT/HCPCS/DRG CODE',   'SERVICE CODE', 'DESCRIPTION', 'CHARGE', 'SELF PAY DISCOUNT RATE']

	# get all of the drug columns from cols_lst

	drg_cols = [col for col in cols_lst if 'DRG' in col or 'DRUG' in col]

	# get all of the inpatient columns from cols_lst

	inpatient_cols =  main_cols + drg_cols + [col for col in cols_lst if 'INPATIENT' in col]

	# get all outpatient columns from cols_lst

	outpatient_cols = main_cols + drg_cols + [col for col in cols_lst if 'OUTPATIENT' in col]

	patient_type_dict = {'INPATIENT': inpatient_cols, 'OUTPATIENT': outpatient_cols}

	df_vidant = get_patient_type_df(df_vidant, main_cols, patient_type_dict)

	df_vidant = df_vidant.replace('nan', np.nan)

	df_vidant.columns = ['BCBS', 'BCBS MEDICARE', 'AETNA', 'AETNA MEDICARE', 'CIGNA',
	   'CIGNA MEDICARE', 'UHC', 'UHC MEDICARE', 'MEDCOST',
	   'De-identified Minimum', 'De-identified Maximum', 'CPT/MS-DRG',
	   'Rev Code', 'Procedure Description', 'Gross Charge',
	   'Self-Pay', 'Patient Type']

	
	system = 'VIDANT'

	df_vidant['system'] = system

	filename= f'{system}_curated_{date}.csv'

	df_vidant.to_csv(os.path.join(curated_path, filename), index=False)

	return df_vidant
