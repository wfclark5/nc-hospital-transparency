from process.profiles import *

data_path = os.path.normpath(os.path.join(abspath, 'data'))

data_zip_path = os.path.join(data_path, 'nc_health_cost_all.zip')

profiles_path = os.path.normpath(os.path.join(abspath, 'docs', 'profiles'))

make_zip(data_path, data_zip_path)

upload_to_s3('nc_health_cost', profiles_path, 'profiles')

upload_to_s3('nc_health_cost', data_zip_path, 'nc_health_cost_all.zip')


