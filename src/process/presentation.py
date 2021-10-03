
from process.utils import *

def get_presentation(dfs_lst, presentation_download_path):
    """
    This function will create a presentation layer of all the dataframes in the dfs_lst
    """

    dfs_lst_present = []
    for dfs in dfs_lst:
        dfs.columns = dfs.columns.str.upper()
        dfs_lst_present.append(dfs)

    presentation_df = pd.concat(dfs_lst_present)

    presentation_df = presentation_df[['CPT/MS-DRG', 'PROCEDURE DESCRIPTION',  'PATIENT TYPE',  'SYSTEM',  'GROSS CHARGE', 'SELF PAY', 
        'DE-IDENTIFIED MAXIMUM',  'DE-IDENTIFIED MINIMUM', 'AETNA',
        'AETNA MEDICARE',  'BCBS', 'BCBS MEDICARE', 'HUMANA', 'HUMANA MEDICARE',
        'CIGNA', 'CIGNA MEDICARE', 'MEDCOST',  'TRICARE',  'UHC', 'UHC MEDICARE',  'FILENAME']]

    presentation_df.to_csv(os.path.join(presentation_download_path, 'nc_hospitals-price-transparency.csv'), index=False)

    presentation_df