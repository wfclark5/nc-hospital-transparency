from process.profiles import *

abspath = os.path.dirname(os.path.normpath(os.path.abspath(os.path.dirname(__file__))))

profile_path = os.path.normpath(os.path.join(abspath, 'docs', 'profiles'))

data_path = os.path.normpath(os.path.join(abspath, 'data'))

data_zip_path = os.path.join(data_path, 'nc_health_cost_all.zip')

profiles_path = os.path.normpath(os.path.join(abspath, 'docs', 'profiles'))

make_zip(data_path, data_zip_path)

s3 = s3fs.core.S3FileSystem(profile="franz")


upload_to_s3('nc_health_cost', profiles_path, 'profiles')

upload_to_s3('nc_health_cost', data_zip_path, 'nc_health_cost_all.zip')


