from modules.scraper import *


abspath = os.path.normpath(os.path.dirname(os.path.dirname(__file__)))

raw_download_path = os.path.normpath(os.path.join(abspath, 'data', 'raw'))

url_download_path = os.path.normpath(os.path.join(abspath, 'data', 'urls'))

additional_json_path = os.path.join(url_download_path, 'additional_exports.json')

driver_path = os.path.join(abspath, 'drivers', 'chromedriver.exe')

# load the json file
driver = create_driver(raw_download_path, driver_path)
