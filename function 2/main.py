import os
import sys
import tempfile
import pandas as pd
from google.cloud import storage

# get a list of in the source system
BUCKET_NAME="aemoarchive"
files_new=[]    
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)
blobs = bucket.list_blobs()
for blob in blobs:
      files_new.append(blob.name)
# get a list of files already loaded
BUCKET_NAME="ameoclean"
filelist_storage=[]    
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)
blobs = bucket.list_blobs()
for blob in blobs:
      filelist_storage.append(blob.name)
#print(filelist_storage)
filelist_storage = [w.replace('REGIOND_PUBLIC_DAILY_', 'PUBLIC_DAILY_') for w in filelist_storage]
filelist_storage = [w.replace('.csv', '.zip') for w in filelist_storage]
# get a list of new files to be loaded
files_new = [fn for fn in files_new if fn.endswith(('.zip'))]
files = sorted(list(set(files_new)-set(filelist_storage)),reverse=True)
print(str(len(files) ) +'  files to upload')
def get_file_path(filename):
    return os.path.join(tempfile.gettempdir(), filename)
def loaddata(requet,dataa):
 for index, x in enumerate(files):
     path_file = 'gs://aemoarchive/'+ x
     print(str(index ) +"  "+ path_file)
     df=None
     df = pd.read_csv(path_file,
     skiprows=1,
     dtype=str,
     engine='python',
     names=range(130),
     usecols=range(11))
     df.columns = df.iloc[0]
     df = df[1:]
     print(sys.getsizeof(df))
      ################ Extract TUNIT Table
     df_unit=df.query('DISPATCH=="TUNIT"')
     df_unit = df_unit.rename(columns={'1': 'version'})
     df_unit = df_unit.query('version=="2"')
     df_unit.columns = df_unit.iloc[0]
     df_unit = df_unit[1:]
     df_unit = df_unit.rename(columns={'2': 'version'})
     df_unit = df_unit.loc[:, df_unit.columns.notnull()]
     df_unit.drop(columns=['I'],inplace=True)
     df_unit.rename(columns={'TUNIT': 'UNIT'}, inplace=True)
     #cols = ['INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE','RAMPUPRATE','LOWER5MIN','LOWER60SEC','LOWER6SEC','RAISE5MIN',
     #'RAISE60SEC','RAISE6SEC','LOWERREG','RAISEREG','AVAILABILITY']
     #df_unit[cols] = df_unit[cols].apply(pd.to_numeric, errors='coerce', axis=1)
     df_unit['SETTLEMENTDATE']=pd.to_datetime(df_unit['SETTLEMENTDATE'])
     unit_file_name = x.replace('PUBLIC_DAILY_','UNITT_PUBLIC_DAILY_')
     unit_file_name = unit_file_name.replace('.zip','.csv')
     df_unit = df_unit.query('INITIALMW!="0"')
     df_unit = pd.DataFrame(df_unit, columns=['UNIT','DUID','SETTLEMENTDATE','INITIALMW'])
     path_to_file = get_file_path(unit_file_name)
     df_unit.to_csv(path_to_file,
     index=False,float_format="%.4f",date_format='%Y-%m-%dT%H:%M:%S.%fZ',compression='gzip')
     blob = bucket.blob(unit_file_name)
     blob.upload_from_filename(path_to_file,content_type='text/csv')
     os.remove(path_to_file)
     print(unit_file_name + str(sys.getsizeof(df_unit)))
     df_unit = None
     
     ################ Extract DUNIT Table
     df_unit=df.query('DISPATCH=="DUNIT"')
     df_unit = df_unit.rename(columns={'1': 'version'})
     df_unit = df_unit.query('version=="3"')
     df_unit.columns = df_unit.iloc[0]
     df_unit = df_unit[1:]
     df_unit = df_unit.rename(columns={'3': 'version'})
     df_unit = df_unit.loc[:, df_unit.columns.notnull()]
     df_unit.drop(columns=['I'],inplace=True)
     df_unit.rename(columns={'DUNIT': 'UNIT'}, inplace=True)
     #cols = ['INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE','RAMPUPRATE','LOWER5MIN','LOWER60SEC','LOWER6SEC','RAISE5MIN',
     #'RAISE60SEC','RAISE6SEC','LOWERREG','RAISEREG','AVAILABILITY']
     #df_unit[cols] = df_unit[cols].apply(pd.to_numeric, errors='coerce', axis=1)
     df_unit = df_unit.astype({"INTERVENTION": int})
     df_unit['SETTLEMENTDATE']=pd.to_datetime(df_unit['SETTLEMENTDATE'])
     unit_file_name = x.replace('PUBLIC_DAILY_','UNITD_PUBLIC_DAILY_')
     unit_file_name = unit_file_name.replace('.zip','.csv')
     df_unit = df_unit.query('INITIALMW!="0"')
     df_unit = pd.DataFrame(df_unit, columns=['UNIT','DUID','SETTLEMENTDATE','INTERVENTION','INITIALMW'])
     path_to_file = get_file_path(unit_file_name)
     df_unit.to_csv(path_to_file,
     index=False,float_format="%.4f",date_format='%Y-%m-%dT%H:%M:%S.%fZ',compression='gzip')
     blob = bucket.blob(unit_file_name)
     blob.upload_from_filename(path_to_file,content_type='text/csv')
     os.remove(path_to_file)
     print(unit_file_name + str(sys.getsizeof(df_unit)))
     df_unit = None

     ################ Extract TREGION Table
     df_unit=df.query('DISPATCH=="TREGION"')
     df_unit = df_unit.rename(columns={'1': 'version'})
     df_unit = df_unit.query('version=="2"')
     df_unit.columns = df_unit.iloc[0]
     df_unit = df_unit[1:]
     df_unit = df_unit.rename(columns={'2': 'version'})
     df_unit = df_unit.loc[:, df_unit.columns.notnull()]
     df_unit.drop(columns=['I'],inplace=True)
     df_unit.rename(columns={'TREGION': 'UNIT'}, inplace=True)
     #cols = ['INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE','RAMPUPRATE','LOWER5MIN','LOWER60SEC','LOWER6SEC','RAISE5MIN',
     #'RAISE60SEC','RAISE6SEC','LOWERREG','RAISEREG','AVAILABILITY']
     #df_unit[cols] = df_unit[cols].apply(pd.to_numeric, errors='coerce', axis=1)
     df_unit['SETTLEMENTDATE']=pd.to_datetime(df_unit['SETTLEMENTDATE'])
     unit_file_name = x.replace('PUBLIC_DAILY_','REGIONT_PUBLIC_DAILY_')
     unit_file_name = unit_file_name.replace('.zip','.csv')
     df_unit = pd.DataFrame(df_unit, columns=['UNIT','REGIONID','SETTLEMENTDATE','RRP'])
     path_to_file = get_file_path(unit_file_name)
     df_unit.to_csv(path_to_file,
     index=False,float_format="%.4f",date_format='%Y-%m-%dT%H:%M:%S.%fZ',compression='gzip')
     blob = bucket.blob(unit_file_name)
     blob.upload_from_filename(path_to_file,content_type='text/csv')
     os.remove(path_to_file)
     print(unit_file_name + str(sys.getsizeof(df_unit)))
     df_unit = None


     ################ Extract DREGION Table
     df_unit=df.query('DISPATCH=="DREGION"')
     df_unit = df_unit.rename(columns={'1': 'version'})
     df_unit = df_unit.query('version=="3"')
     df_unit.columns = df_unit.iloc[0]
     df_unit = df_unit[1:]
     df_unit = df_unit.rename(columns={'3': 'version'})
     df_unit = df_unit.loc[:, df_unit.columns.notnull()]
     df_unit.drop(columns=['I'],inplace=True)
     df_unit.rename(columns={'DREGION': 'UNIT'}, inplace=True)
     #cols = ['INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE','RAMPUPRATE','LOWER5MIN','LOWER60SEC','LOWER6SEC','RAISE5MIN',
     #'RAISE60SEC','RAISE6SEC','LOWERREG','RAISEREG','AVAILABILITY']
     #df_unit[cols] = df_unit[cols].apply(pd.to_numeric, errors='coerce', axis=1)
     df_unit['SETTLEMENTDATE']=pd.to_datetime(df_unit['SETTLEMENTDATE'])
     df_unit = df_unit.astype({"INTERVENTION": int})
     unit_file_name = x.replace('PUBLIC_DAILY_','REGIOND_PUBLIC_DAILY_')
     unit_file_name = unit_file_name.replace('.zip','.csv')
     df_unit = pd.DataFrame(df_unit, columns=['UNIT','REGIONID','SETTLEMENTDATE','INTERVENTION','RRP'])
     path_to_file = get_file_path(unit_file_name)
     df_unit.to_csv(path_to_file,
     index=False,float_format="%.4f",date_format='%Y-%m-%dT%H:%M:%S.%fZ',compression='gzip')
     blob = bucket.blob(unit_file_name)
     blob.upload_from_filename(path_to_file,content_type='text/csv')
     os.remove(path_to_file)
     print(unit_file_name + str(sys.getsizeof(df_unit)))
     df_unit = None