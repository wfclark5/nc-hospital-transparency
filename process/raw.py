from .utils import *


def get_source_urls(driver_path: str, raw_download_path: str) -> dict:

	hospital_csv = os.path.join(raw_download_path, 'hospitals.csv')

	# read in hospitals csv

	df = pd.read_csv(hospital_csv)

	driver = create_driver(raw_download_path, driver_path)

	hospital_data_urls = {}

	wakemed_urls = []

	ext = ['.json','.csv', 'wpfb_dl', '.xlsx', 'ptapp']

	for index, row in df.iterrows():

		records = []
		
		browser = get_url_data(row['hospital_url'], driver)

		# get the page source and parse itS

		source = browser.page_source

		soup = BeautifulSoup(source, 'lxml')
		
		for entry in soup.find_all(['a'], href=True): 

			download_url = entry.get('href')

			if any(file_type in download_url for file_type in ext):
				# determine if the base hospital_url is in download_url or not

				if row['hospital_id'] in 'vidant-health' and '.csv' in download_url:
					continue

				if download_url.startswith('/'):
					# if download_url starts with '/' then add the base hospital_url to the download_url

					data_url = urlparse(row['hospital_url']).scheme + "://" + urlparse(row['hospital_url']).netloc + '/'  + download_url

					records.append(data_url)

				else:
					# if download_url does not start with '/' then add the base hospital_url to the download_url
					records.append(download_url)
				
		if 'first-health' in row['hospital_id']:

			limit = 250

			page = 0

			first_health_url = row['hospital_url'].format(0)

			output = requests.get(first_health_url).json()

			output = requests.get(row['hospital_url'].format(0)).json()

			page_limit_max = int((output['count'] / limit) + 1)

			first_health_urls = [records.append(row['hospital_url'].format(page)) for page in range(1, page_limit_max)]
			
		if 'wakemed' in row['hospital_id']:
			
			wakemed_urls.append(row['hospital_url'])
			
		hospital_data_urls[row['hospital_id']] = list(set(records)) 

	hospital_data_urls['wakemed'] = wakemed_urls

	return hospital_data_urls


def get_unc(driver_path: str, hospital_id: str, hospital_urls: dict, raw_download_path: str) -> None:

	"""Create drivers to bypass captcha for UNC data"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	driver = create_driver(download_path, driver_path)


	def _wait_between(a,b):
		rand=uniform(a, b) 
		sleep(rand)

	for url in url_list:
		
		try:
			driver.get(url)
		except TimeoutException:
			print("Loading took too much time!")
		
		driver.find_element_by_xpath('/html/body/div[1]/div/div/div[2]/div/div[1]/div[1]/div[2]/div/a[2]').click()

		sleep(10)

		mainWin = driver.current_window_handle  

		# move the driver to the first iFrame 
		driver.switch_to_frame(driver.find_elements_by_tag_name("iframe")[0])

		# *************  locate CheckBox  **************
		CheckBox = WebDriverWait(driver, 10).until(
				EC.presence_of_element_located((By.ID ,"recaptcha-anchor"))
				) 

		# *************  click CheckBox  ***************
		_wait_between(0.5, 0.7)  
		
		# making click on captcha CheckBox 
		CheckBox.click()

		# switch back to main window

		driver.switch_to.window(mainWin)

		driver.find_element_by_xpath('/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/div/div[2]/div/div/div/div/fieldset/div/div/div/div[4]/div/div/div/div/div/span/input').click()

		driver.find_element_by_xpath('/html/body/div[1]/div/div/div[2]/div/div[2]/div[2]/div/div/div[2]/div/div/div/div/div[2]/div/div/div/div/fieldset/div/div/div/a').click()






def get_duke(hospital_urls: dict, raw_download_path: str, hospital_id='duke-university-hospital') -> pd.DataFrame:

	"""Get Duke data, download the csv files, and get dataframes"""


	# get urls from hospital_urls using hospital_id parameter
	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)

	create_directory(download_path)

	df_list = []


	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file

		response = get_url_data(url, is_request=True)

		# write reponse to csv file 
		df = pd.read_csv(io.StringIO(response.data.decode('ANSI')), engine='python')

		df['Filename'] = filename
		
		df.to_csv(os.path.join(download_path, filename), index=False)

		df.columns = df.columns.str.strip()
		
		df.rename(columns={'HCPCS/ CPT/NDC Code' : 'CPT/MS-DRG', 
						   'Procedure Name' : 'Procedure Description', 
						   'Charge' : 'Gross Charge', 
						   'Medicare Adv - Aetna' : 'AETNA MEDICARE', 
						   'Medicare Adv - BCBS' : 'BCBS MEDICARE', 
						   'Medicare Adv - Humana' : 'HUMANA MEDICARE', 
						   'Medicare Adv - United' : 'UHC MEDICARE', 
						   'Aetna':'AETNA',  
						   'Cigna':'CIGNA', 
						   'Medcost':'Medcost', 
						   'United':'UHC', 
						   'Tricare':'Tricare', 
						   'De-identified Minimum' : 'De-identified Minimum', 
						   'De-identified Maximum' : 'De-identified Maximum',  
						   'MS DRG' : 'CPT/MS-DRG', 
						   'MS DRG w Description' :	'Procedure Description'}, inplace=True)

		df_list.append(df)
		
	return pd.concat(df_list)


def get_ncb(hospital_urls: dict, raw_download_path: str, hospital_id='north-carolina-baptist-hospital') -> pd.DataFrame:

	"""Get Wake-Forest Baptist data and download the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	df_list = []

	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file
		response = get_url_data(url, is_request=True)
		# write reponse to csv file 
		df = pd.read_csv(io.BytesIO(response.data), engine='python')

		df.to_csv(os.path.join(download_path, filename.replace('?la=en', '')), index=False)


		df['Filename'] = filename


		# check to see if column name is in dataframe and if it is replace it 
		
		df.rename(columns={'Inpatient/Outpatient': 'Patient Type', 
								'CPT/HCPC Code': 'CPT', 
								'Revenue Code': 'Rev Code', 
								'DRG Code': 'DRG', 
								'NDC Code': 'NDC',
								'Description': 'Procedure Description',
								'Gross Charges': 'Gross Charge',
								'Min': 'De-identified Minimum',
								'Max': 'De-identified Maximum',
								'Minimum Negotiated Charge': 'De-identified Minimum', 
								'Maximum Negotiated Charge': 'De-identified Maximum',
								'Aetna Coventry FirstHealth Wellpath': 'Aetna Managed Care',
								'Aetna\nCoventry FirstHealth Wellpath': 'Aetna Managed Care',
								'Humana Choicecare': 'Humana',
								'Blue Medicare': 'BCBS Medicare',
								'BCBS MEDICARE': 'BCBS Medicare',
								'Discounted Cash Price': 'Self Pay',
								'Uninsured Discount': 'Self Pay',
								'BCBS\n(PPO, State Health Plan, Federal Employees, Blue Select)': 'BCBS Managed Care',
								'BCBS\n(PPO, State Health, Federal Employees, Blue Select, Blue Value)': 'BCBS Managed Care',
								'BCBS\n(PPO, State Health, Federal Employees, Blue Select)': 'BCBS Managed Care', 
								'BCBS\\n(PPO, State Health, Federal Employees, Blue Select)': 'BCBS Managed Care',
								'BCBS\n(PPO, State Health, Federal Employee, Blue Select)': 'BCBS Managed Care',
								'BCBS\n(PPO,State Health, Federal Employees, Blue Select)': 'BCBS Managed Care'}, inplace=True)

		if 'Wilkes-Regional' in filename:
			df['Humana'] = None

		if 'North-Carolina-Baptist-Hospital' in filename:
			df['Medcost'] = None


		df['CPT/MS-DRG'] = combine_related([df['CPT'], df['DRG'], df['NDC']])
		
		print(df.columns)

		df=df[["Patient Type", "DRG", "Rev Code",  "CPT", 
				"NDC",	"Procedure Description", "Gross Charge", "Self Pay", 'De-identified Minimum', 
				'De-identified Maximum',	"Aetna Managed Care" ,"Aetna Medicare",	"BCBS Managed Care", "BCBS Medicare", "Cigna Managed Care" , 
				"Humana", "Humana Medicare"	, "UHC Managed Care", "UHC Medicare", 'Medcost', "Filename"]]																								

		# append to list
		df_list.append(df)
	
	return pd.concat(df_list)
		

def get_app(hospital_urls: dict, raw_download_path: str, hospital_id='app-regional-health-system') -> pd.DataFrame:

	"""Get Applachain Regional Data and download only the CSV data"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)

	create_directory(download_path)

	cdm_lst, drg_lst, shop_df = [], [], []


	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file
		response = get_url_data(url, is_request=True)
		
		filename = response.headers['Content-Disposition'].strip('"').replace('inline; filename="', '').replace('"', '')
		
		headers = ['CMS Required DRG/CPT/HCPCS', 'DRG/CPT/ HCPCS','Description','CMH FY21 Chg (estimate)','Self-Pay (estimate)', 'Medicare', 'Medicaid', 'Aetna','BCBS',	'BCBS State', 'Cigna', 'Medcost','UHC']

		# if filename ends with .csv write it out

		if filename.endswith('.csv'):

			if 'CDM' in filename:
				df = pd.read_csv(io.StringIO(response.data.decode('utf-8')), skiprows=5, engine='python')
				df['Filename'] = filename

				cdm_lst.append(df)
				df.to_csv(os.path.join(download_path, filename), index=False)

			if 'DRG' in filename:
				df = pd.read_csv(io.StringIO(response.data.decode('utf-8')), skiprows=5, engine='python')
				df['Filename'] = filename
				drg_lst.append(df)
				df.to_csv(os.path.join(download_path, filename), index=False)

			if 'Shop' in filename: 
				df = pd.read_csv(io.StringIO(response.data.decode('utf-8')), skiprows=5, header = 0, names=headers, engine='python')
				df['Filename'] = filename
				shop_df.append(df)
				df.to_csv(os.path.join(download_path, filename), index=False)
		
		else:
			continue
			
	return pd.concat(shop_df)


def get_catawba(hospital_urls: dict, raw_download_path: str, hospital_id='catawba-valley-medical-center') -> pd.DataFrame:

	"""Get Catawba Valley Regional data and download the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	df_list = []

	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file
		response = get_url_data(url, is_request=True)
		if filename.endswith('.csv'):
			if 'StandardCharges' in filename:
				df = pd.read_csv(io.StringIO(response.data.decode('utf-8')), engine='python')
				df['Filename'] = filename
				df.to_csv(os.path.join(download_path, filename), index=False)
				df_list.append(df)
			else: 
				continue

	return pd.concat(df_list)


def get_cateret(hospital_urls: dict, raw_download_path: str, hospital_id='cateret-health-care') -> pd.DataFrame:
	
	"""Get Cateret Health data and download only the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	comp_list = []

	desc_list = []

	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file
		response = get_url_data(url, is_request=True)

		if 'Comparison' in filename:
			df = pd.read_excel(io.BytesIO(response.data), skiprows=5)
			df['Filename'] = filename
			df.to_csv(os.path.join(download_path, filename.replace('.xlsx', '.csv')), index=False)
			comp_list.append(df)
		if 'Description' in filename:                 
			df = pd.read_csv(io.BytesIO(response.data), engine='python')
			df['Filename'] = filename
			df.to_csv(os.path.join(download_path, filename), index=False)
			desc_list.append(df)
		else: 
			continue

	return pd.concat(comp_list), pd.concat(desc_list)

def get_cone(hospital_urls: dict, raw_download_path: str, hospital_id='cone-health') -> pd.DataFrame:

	"""Get Cone Health data and download only the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	df_list = []

	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file
		response = get_url_data(url, is_request=True)

		if filename.endswith('.csv'):
			df = pd.read_csv(io.BytesIO(response.data), skiprows=3, engine='python')
			df['Filename'] = filename
			df.to_csv(os.path.join(download_path, filename), index=False)
			df_list.append(df)
		else:
			continue
	
	return pd.concat(df_list)

def get_first(hospital_urls: dict, raw_download_path: str) -> pd.DataFrame:

	"""Get First Health data and download only the CSV file"""

	ids = ['first-health-moore', 'first-health-montgomery']

	meta = ['id' , 'hospital' , 'code' , 'description' , 'codeType' , 'cmsShoppable', 'cranewareShoppable' , 
			'shoppable' , 'level' , 'grossCharge' , 'minAllowable' , 'maxAllowable' , 'avgAllowable' , 
			'nationalPercentile50' , 'nationalPercentile75' , 'nationalPercentile90' , 'totalVol835' , 
			'totalVol837' , 'published' , 'selfPay' , ['name', 'id', 'hospital', 'minAllowable', 'maxAllowable', 
			'avgAllowable', 'exclude']]
			
	headers = ['payor.name', 'payor.id', 'payor.hospital', 'payor.minAllowable', 'payor.maxAllowable', 'payor.avgAllowable', 
				'payor.exclude', 'id', 'hospital', 'code', 'description', 'codeType', 'cmsShoppable', 'cranewareShoppable', 
				'shoppable', 'level', 'grossCharge', 'minAllowable', 'maxAllowable', 'avgAllowable', 'nationalPercentile50', 
				'nationalPercentile75', 'nationalPercentile90', 'totalVol835', 'totalVol837', 'published', 'selfPay', 
				'name.id.hospital.minAllowable.maxAllowable.avgAllowable.exclude']
	df_moore_list = []
	
	df_mont_list = []

	for hospital_id in ids:

		url_list = hospital_urls[hospital_id]

		for url in url_list:
			# infer filename from url 
			# filename = url.split('/')[-1]
			# download the file
			response = get_url_data(url, is_download_request=True)
			# create pandas dataframe from json 
			# df = pd.read_json()
			json_data =  json.loads(response.content)['response']
			df = pd.json_normalize(json_data, record_path='payors', meta=meta, errors='ignore', record_prefix='payor.')
			df['Filename'] = hospital_id
			# if file does not exist write header 
		
			if 'moore' in hospital_id:
				df_moore_list.append(df)
			else:
				df_mont_list.append(df)


	hospital_id = 'first-health'

	download_path = os.path.join(raw_download_path, hospital_id)
		
	create_directory(download_path)

	dfs_list = df_moore_list + df_mont_list

	df = pd.concat(dfs_list)

	df.to_csv(os.path.join(download_path, f'{hospital_id}_standard_charges.csv'), index=False)

	return df

		
def get_iredell(hospital_urls: dict, raw_download_path: str, hospital_id='iredell-health') -> pd.DataFrame:


	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	cdm_list, drg_inter_list, drg_list = [], [], []
	
	"""Get Iredell Health data and download only the CSV file"""
	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file

		response = get_url_data(url, is_request=True)

		if 'cdm' in filename:
			df = pd.read_csv(io.BytesIO(response.data), engine='python')
			df['Filename'] = filename
			df.to_csv(os.path.join(download_path, filename), index=False)
			cdm_list.append(df)
		
		if 'drg_inter' in filename:
			df = pd.read_csv(io.BytesIO(response.data), engine='python')
			df['Filename'] = filename
			df.to_csv(os.path.join(download_path, filename), index=False)
			drg_inter_list.append(df)
		
		else:
			df = pd.read_csv(io.BytesIO(response.data), skiprows = 3, engine='python')
			df['Filename'] = filename
			df.to_csv(os.path.join(download_path, filename), index=False)
			drg_list.append(df)
	
	return pd.concat(cdm_list), pd.concat(drg_inter_list), pd.concat(drg_list)


def get_mission(hospital_urls: dict, raw_download_path: str, hospital_id='mission-health') -> pd.DataFrame:

	"""Get Mission Health data and download only the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)
	
	df_list = []

	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file
		response = get_url_data(url, is_request=True)
		# write reponse to csv file 

		df = pd.read_csv(io.BytesIO(response.data), skiprows = 1, engine='python')

		df['Filename'] = filename
		
		df.to_csv(os.path.join(download_path, filename), index=False)

		df_list.append(df)
	
	return pd.concat(df_list)

def get_nhrmc(hospital_urls: dict, raw_download_path: str, hospital_id='nhrmc-health') -> pd.DataFrame:

	"""Get New Hanover Regional Medical Center data and download only the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	ip_df_list = []

	op_df_list = []

	for url in url_list:
		 
		# infer filename from url 
		filename = url.split('/')[-1].replace('.xlsx', '.csv')
		# download the file


		response = get_url_data(url, is_download_request=True)

		# write reponse to csv file from excel

		if 'new-hanover' in filename:

			df_ip = pd.read_excel(response.content,  sheet_name=3, skiprows=5)

			df_ip['Filename'] = filename

			df_op = pd.read_excel(response.content, sheet_name=4, skiprows=5)

			df_op['Filename'] = filename

			df_ip.rename(columns = {'MS-DRG': 'MS-DRG/APC', 
									'APC': 'MS-DRG/APC', 
									'BCBS HMO PPO': 'BCBS', 
									'Cigna PPO': 'Cigna', 
									'UHC HMO': 'UHC'}, inplace=True)

			df_ip['Patient Type'] = 'Inpatient'

			df_op.rename(columns = {'MS-DRG': 'MS-DRG/APC', 
									'APC': 'MS-DRG/APC', 
									'BCBS HMO PPO': 'BCBS', 
									'Cigna PPO': 'Cigna', 
									'UHC HMO': 'UHC'}, inplace=True)
			
			df_op['Patient Type'] = 'Outpatient'

			ip_df_list.append(df_ip)

			op_df_list.append(df_op)

			df_op.to_csv(os.path.join(download_path, filename), index=False)

			df_ip.to_csv(os.path.join(download_path, filename), index=False)




		if 'pender' in filename:

			df_ip = pd.read_excel(response.content,  sheet_name=3, skiprows=4)

			df_ip['Filename'] = filename

			df_op = pd.read_excel(response.content, sheet_name=4, skiprows=5)

			df_op['Filename'] = filename
	
			df_ip.rename(columns = {'MS-DRG': 'MS-DRG/APC', 
									'APC': 'MS-DRG/APC', 
									'DRG Average Charge': 'Average Charge', 
									'APC Average Charge': 'Average Charge',  
									'Aetna HMO/PPO': 'Aetna', 
									'BCBS PPO/HMO': 'BCBS', 
									'Cigna PPO/HMO': 'Cigna', 
									'UHC HMO/PPO': 'UHC', 
									'MedCost PPO': 'MedCost'}, inplace=True)

			df_ip['Patient Type'] = 'Inpatient'

			df_op.rename(columns ={'MS-DRG': 'MS-DRG/APC', 
									'APC': 'MS-DRG/APC', 
									'DRG Average Charge': 'Average Charge', 
									'APC Average Charge': 'Average Charge',  
									'Aetna HMO/PPO': 'Aetna', 
									'BCBS PPO/HMO': 'BCBS', 
									'Cigna PPO/HMO': 'Cigna', 
									'UHC HMO/PPO': 'UHC', 
									'MedCost PPO': 'MedCost'}, inplace=True)

			df_op['Patient Type'] = 'Outpatient'

			ip_df_list.append(df_ip)

			op_df_list.append(df_op)

			df_op.to_csv(os.path.join(download_path, filename), index=False)

			df_ip.to_csv(os.path.join(download_path, filename), index=False)

		df_list = ip_df_list + op_df_list

		df = pd.concat(df_list)


	return df
	
	


def get_northern(hospital_urls: dict, raw_download_path: str, hospital_id='northern-regional') -> pd.DataFrame:

	"""Get Northern Regional data and download only the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	for url in url_list:
		# infer filename from url and replace .json with .csv
		filename = url.split('/')[-1].replace('.json', '.csv')

		# download the file
		response = get_url_data(url, is_download_request=True)

		# create pandas dataframe from response
		df = pd.read_json(response.content, lines=True)

		df['Filename'] = filename

		# drop columns
		df_all = df.drop(columns=['PACKAGE_TYPE', 'PERCENT_OCCURRENCE_WITHIN_PRIMARY_CODE','SUPPORTING_SERVICE_CODE' ,'SUPPORTING_SERVICE_CODE_DESCRIPTION'])

		# reformat column types
		for column in df_all:
			if df_all[column].dtype == 'float64':
				df_all[column]=pd.to_numeric(df_all[column], downcast='float')
			if df_all[column].dtype == 'int64':
				df_all[column]=pd.to_numeric(df_all[column], downcast='integer')

		# write to csv
		df.to_csv(os.path.join(download_path, filename), index=False)

	return df

def get_novant(hospital_urls: dict, raw_download_path: str, hospital_id='novant-health') -> pd.DataFrame:

	"""Get Novant Health data and download only the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	df_list = []

	for url in url_list:
		# infer filename from url 
		filename = url.split('/')[-1]
		# download the file
		response = get_url_data(url, is_request=True)
		# write reponse to csv file 
		df = pd.read_csv(io.BytesIO(response.data) , engine='python')

		df['Filename'] = filename
		
		df.to_csv(os.path.join(download_path, filename), index=False)

		df.columns = df.columns.str.strip()

		df_list.append(df)
		
	return pd.concat(df_list)



def get_vidant(hospital_urls: dict, raw_download_path: str, hospital_id='vidant-health') -> pd.DataFrame:

	"""Get Vidant Health data and download only the CSV file"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	df_list = []

	for url in url_list:
		 
		# infer filename from url 
		filename = url.split('/')[-1]
		
		# download the file
		
		response = get_url_data(url, is_download_request=True)
		
		# write reponse to csv file from excel

		df = pd.read_excel(response.content, header=[2,3])

		df['Filename'] = filename

		df.to_csv(os.path.join(download_path, filename.replace('.xlsx', '.csv')), index=False)

		df_list.append(df)

	return pd.concat(df_list)



def get_atrium(hospital_urls: dict, raw_download_path: str, hospital_id='atrium-health') -> pd.DataFrame:

	"""Get Atrium Health data from url"""

	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)

	create_directory(download_path)

	df_list = []

	for url in url_list:

		filename = url.split('/')[-1].replace('.json', '.csv')

		# download the file

		response = get_url_data(url, is_request=True)

		# create pandas dataframe from response

		df = pd.read_json(response.data)

		df.replace(r'', np.nan, inplace=True)

		df['Filename'] = filename

		# strip white space

		df.columns = df.columns.str.strip()

		if 'BehavioralHealth' in filename:
			df['Min /Max'] = None
			df['Outpatient Negotiated Charge'] = None
			df['Plan'] = None
			df['Inpatient Negotiated Charge'] = None


		
		df = df[["Procedure", "Code Type", "Code", "Rev Code", "Procedure Description", "Min /Max",
					"Inpatient Gross Charge", "Inpatient Negotiated Charge", "Outpatient Gross Charge",
					"Outpatient Negotiated Charge", "TabName", "Quantity", "Payer", "Plan(s)",  "Inpatient Discounted Charge",
					"Outpatient Discounted Charge", "Plan", "Product", "Gross Charge - Facility", "Negotiated Charge - Facility",
					"Gross Charge - Non-Facility", "Negotiated Charge - Non-Facility", "Filename"]]

		# replace blanks will nan

		df.replace(r'', np.nan, inplace=True)

		df.to_csv(os.path.join(download_path, filename), index=False)

		if 'CarolinasMedicalCenter' in filename or 'AtriumHealthUnion' in filename:

			df = df.groupby(level=0, axis=1).agg(lambda x: ':'.join(x[x.notnull()].astype(str)))
	
		df_list.append(df)


	return pd.concat(df_list)





def get_wakemed(hospital_urls: dict, raw_download_path: str, driver_path: str, hospital_id='wakemed') -> pd.DataFrame:

	"""Get wakemed data from url"""

	## create a list comprehension to get the urls for each hospital
	url_list = hospital_urls[hospital_id]

	download_path = os.path.join(raw_download_path, hospital_id)
	
	create_directory(download_path)

	driver = create_driver(download_path, driver_path)

	dfs_list = []
	
	for url in url_list:
	# infee filename from url
	
		driver.get(url)

		xpath = '/html/body/app-root/app-allservices/div[1]/div/div[3]/div/app-paginator/div[2]/div/div/button'

		driver.execute_script("arguments[0].click();", WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, xpath))))

		driver.switch_to.alert.accept()

		sleep(10)

	download_wait(directory=download_path, timeout=60, nfiles=1)

	json_list = [pos_json for pos_json in os.listdir(download_path)]

	filename = os.path.join(download_path, f'{hospital_id}-standard_charges.csv')

	if os.path.isfile(filename):
		os.remove(filename)

	for file in json_list:

		if file.endswith('.json'):

			json_file = os.path.join(download_path, file)

			wakemed_json = open(json_file, 'r').read()

			wakemed_data = wakemed_json.replace('\x00', '')

			df = pd.read_json(wakemed_data)

			df['Filename'] = hospital_id

			if not os.path.isfile(filename):
				df.to_csv(filename, index=False)
			else: # else it exists so append without writing the header
				df.to_csv(filename, mode='a', header=False, index=False)

			os.remove(json_file)

			df.rename(columns={'Avg_Gross_Charge': 'Gross Charge', 
								'Gross_Charge': 'Gross Charge',
								'description': 'Procedure Description',
								'iobSelection': 'Patient Type',
								'Cash_Discount': 'Self Pay',
								'Associated_Codes': 'CPT/MS-DRG',
								'DeIdentified_Max_Allowed': 'De-identified Maximum',
								'Deidentified_Min_Allowed': 'De-identified Minimum'
								 }, inplace=True)

			dfs_list.append(df)
		else:
			continue

	return pd.concat(dfs_list)

