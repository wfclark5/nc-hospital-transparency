
from process.profiles import *

profile_path = os.path.normpath(os.path.join(abspath, 'docs', 'profiles'))

raw_download_path = os.path.normpath(os.path.join(abspath, 'data', 'raw'))

curate_download_path = os.path.normpath(os.path.join(abspath, 'data', 'curated'))

presentation_download_path = os.path.normpath(os.path.join(abspath, 'data', 'presentation'))

csv_in_path = os.path.join(abspath, 'docs', 'profiles', 'table.csv')

html_out_path =  os.path.join(abspath, 'docs', 'profiles')

raw_files = get_raw_files(raw_download_path)

profile_raw(profile_path, raw_files)

curated_files = get_curated_files(curate_download_path)

profile_curated(profile_path, curated_files)

presentation_files = get_presentation_files(presentation_download_path)

profile_presentation(profile_path, presentation_files)

generate_table(csv_in_path, html_out_path)

