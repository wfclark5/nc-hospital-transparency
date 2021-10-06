from process.utils import *

abspath = os.path.dirname(os.path.normpath(os.path.abspath(os.path.dirname(''))))

sys.path.insert(0, os.path.abspath('../src'))

raw_row_holder = {}
curated_row_holder = {}
presentation_row_holder = {}

def generate_profile(profile_path, title, key, df, filename, layer=None)  -> None: 
    profile = None
    profile = create_report(df, title=title)
    if layer == 'raw':
        raw_row_holder[f"{key} {title}"] = key + f'_{filename.lower()}.html'
    if layer == 'curated':
        curated_row_holder[f"{key} {title}"] = key + f'_{filename.lower()}.html'
    if layer == 'presentation':
        presentation_row_holder[f"{key} {title}"] = key + f'_{filename.lower()}.html'
    profile.save(filename=key + f'_{filename.lower()}', to=profile_path)

def profile_app(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.split('_')[1:-1]).replace('FY2021', '').replace('FY2022', '').replace('FY2023', '')
    filename = title.replace(' ', '_')
    df = pd.read_csv(path, dtype='unicode')
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_atrium(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').replace('AtriumHealth','').replace('BehavioralHealth', 'Behavioral Health').replace('StandardCharges', 'Standard Charges').replace('Carolinas', 'Carolinas ')
        .replace('Medical','Medical ').split('_')[1:])
    filename = title.replace(' ', '_')
    df = pd.read_csv(path, dtype='unicode')
    df = df.fillna(-1)
    df.dropna(subset=['Code'], inplace=True)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_catawba(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').replace('CatawbaValleyMedicalCenter', 'Catawba Valley Medical Center').replace('StandardCharges', 'Standard Charges').split('_')[1:-1])
    filename = 'valley_medical_center'
    key = 'catwaba'
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename)

def profile_cateret(profile_path, key, path, file)  -> None: 
    title = file.replace('.csv', '').replace('-', ' ')
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_cone(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').replace('AlamanceRegionalMedicalCenterInc', 'Alamance Regional Medical Center').replace('AnniePennHospital', 'Annie Penn Hospital').replace('BehavioralHealthHospital', 'Behavioral Health Hospital').replace('TheMosesHConeMemorialHospital', 'The Moses Cone Memorial Hospital').replace('WesleyLongHospital', 'Wesley Long Hospital').replace('standardcharges', 'Standard Charges').split('_')[1:])
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_duke(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').replace('standardcharges', 'Standard Charges').replace('DRaH', 'Duke Raleigh Hospital').replace('DRH', 'Duke Regional Hospital').replace('DUH', 'Duke University Hospital').split('_')[1:])
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_first(profile_path, key, path, file)  -> None: 
    title = file.replace('.csv', '').repal
    filename = title.replace('first-health', '')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename)

def profile_iredell(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').replace('-', '_').split('_')[0:2])
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_mission(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').replace('standardcharges', 'Standard Charges').split('_')[1:2]).replace('-', ' ').capitalize()
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_nhrmc(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').split('-')[1:])
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_ncb(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').split('Transparency')[0]).replace(' ', '').replace('-', ' ').rstrip()
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename)

def profile_northern(profile_path, key, path, file)  -> None: 
    title = 'Northern Regional Standard Charges'
    filename = title.replace('Northern Regional ', '').replace(' ','_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_novant(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').replace('standardcharges', 'Standard Charges').split('_')[1:]).replace('MedicalCenter', ' Medical Center').replace('NovantHealth', '').replace('OrthopedicHospital', ' Orthopedic Hospital').replace('ParkHospital', ' Park Hospital')
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_vidant(profile_path, key, path, file)  -> None: 
    title = ' '.join(file.replace('.csv', '').split('-')[2:]).replace('VIDANT ', '').lstrip()
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_wakemed(profile_path, key, path, file)  -> None: 
    title = 'hopsital standard charges'
    filename = title.replace(' ', '_')
    df = pd.read_csv(path)
    generate_profile(profile_path, title, key, df, filename, 'raw')

def profile_raw(profile_path: str, csv_file_dict: dict) -> None:
    for key, values in csv_file_dict.items():
        for file, path in values.items():
            print(key, file)
            try: 
                if 'app-regional' in key:
                    profile_app(profile_path, key, path, file)
                elif 'atrium' in key:
                    try:
                        profile_atrium(profile_path, key, path, file)
                    except:
                        pass
                elif 'catawba' in key: 
                    profile_catawba(profile_path, key, path, file)
                elif 'cateret-health-care' in key:
                    profile_cateret(profile_path, key, path, file)
                elif 'cone-health' in key:
                    profile_cone(profile_path, key, path, file)
                elif 'duke' in key:
                    profile_duke(profile_path, key, path, file)
                elif 'first-health' in key:
                    profile_first(profile_path, key, path, file)
                elif 'iredell-health' in key: 
                    profile_iredell(profile_path, key, path, file)
                elif 'mission-health' in key:
                    profile_mission(profile_path, key, path, file)
                elif 'nhrmc' in key:
                    profile_nhrmc(profile_path, key, path, file)
                elif 'north-carolina-baptist-hospital' in key:
                    profile_ncb(profile_path, key, path, file)
                elif 'northern' in key: 
                    profile_northern(profile_path, key, path, file)
                elif 'novant' in key:
                    profile_novant(profile_path, key, path, file)
                elif 'vidant-health' in key:
                    profile_vidant(profile_path, key, path, file)
                elif 'wakemed' in key:
                    profile_wakemed(profile_path, key, path, file)
                else:
                    continue
            except Exception as e:
                print(e)
                print(file)

def get_raw_files(raw_download_path: str) -> dict:
    csv_raw_file_dict = {}
    for path in glob(f'{raw_download_path}/*', recursive=True):
        key = os.path.basename(path)
        csv_raw_file_dict[key] = {}
        for file in glob(f'{path}/*.csv'):
            csv_raw_file_dict[key][os.path.basename(file)] =  file
    return csv_raw_file_dict

def get_curated_files(curate_download_path: str) -> list:
    csv_curated_lst = []
    for path in glob(f'{curate_download_path}/*.csv', recursive=True):
        csv_curated_lst.append(path)
    return csv_curated_lst


def profile_curated(profile_path: str, csv_curated_lst: list) -> None:
    for path in csv_curated_lst:
        title = ' '.join(os.path.basename(path.replace('.csv', '').split('_')[0])).replace(' ', '')
        filename = title
        key = 'curated'
        print(title)
        if 'APP' in filename:
            df = pd.read_csv(path)
        elif 'ATRIUM' in filename:
            df = pd.read_csv(path)
        elif 'BAPTIST' in filename:
            df = pd.read_csv(path)
            df = df.fillna(-1)
        elif 'CATAWBA' in filename:
            df = pd.read_csv(path)
        elif 'CATERET' in filename:
            df = pd.read_csv(path)
        elif 'CONE' in filename:
            df = pd.read_csv(path)
        elif 'DUKE' in filename:
            df = pd.read_csv(path)
            df = df.fillna(-1)
        elif 'FIRST' in filename:
            df = pd.read_csv(path)
        elif 'IREDELL' in filename:
            df = pd.read_csv(path)
        elif 'MISSION' in filename:
            df = pd.read_csv(path)
        elif 'NHRMC' in filename:
            df = pd.read_csv(path)
        elif 'NORTHERN' in filename:
            df = pd.read_csv(path)
        elif 'NOVANT' in filename:
            df = pd.read_csv(path)
        elif 'VIDANT' in filename:
            df = pd.read_csv(path)
        elif 'WAKEMED' in filename:
            df = pd.read_csv(path)
        else:
            continue
        
        generate_profile(profile_path, title, key, df, filename, 'curated')

def get_presentation_files(presentation_download_path: str) -> list:
    presentation_curated_lst = []
    for path in glob(f'{presentation_download_path}/*.csv', recursive=True):
        presentation_curated_lst.append(path)
    return presentation_curated_lst

def profile_presentation(profile_path: str, presentation_curated_lst: list) -> None:
    for path in presentation_curated_lst:
        title = os.path.basename(path).replace('.csv', '').replace('-', ' ').replace('_', ' ')
        filename = title.replace(' ', '_')
        key = 'presentation'
        df = pd.read_csv(path)
        df = df.fillna(-1)
        generate_profile(profile_path, title, key, df, filename, 'presentation')


def generate_table(csv_in_path:str, out_path:str) -> None:
	table_dict = {"Raw": [], "Curated": [], "Presentation": []}

	for key, value in raw_row_holder.items():
		table_dict['Raw'].append(f" <a href='{value}'>{key}</a> ")

	for key, value in curated_row_holder.items():
		table_dict['Curated'].append(f" <a href='{value}'>{key}</a> ")

	for key, value in presentation_row_holder.items():
		table_dict['Presentation'].append(f" <a href='{value}'>{key}</a> ")

	table_df = pd.DataFrame.from_dict(dict([ (k,pd.Series(v)) for k,v in table_dict.items() ]), orient='index')

	table_df = table_df.transpose()

	table_df.to_csv(csv_in_path, index=False)

	filein = open(csv_in_path)

	fileout = open(os.path.join(out_path, "html-table.html"), "w")

	table = ""
	data = filein.readlines()
	# Create the table's column headers
	header = data[0].split(",")
	table += "  <thead>\n"
	for column in header:
		table += "    <th>{0}</th>\n".format(column.strip())
	table += "  <thead>\n"


	# Create the table's row data
	for line in data[1:]:
		row = line.split(",")
		# print(line)
		# break
		table += "  <tr>\n"
		for column in row:
			print(column)
			table += "    <td>{0}</td>\n".format(column.strip())
		table += "  </tr>\n"


	fileout.writelines(table)

	fileout.close()

	filein.close()

	return None

