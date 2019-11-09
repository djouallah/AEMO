
import os
import tempfile
import pandas as pd
import gcsfs
from google.cloud import storage
from google.cloud import bigquery
import re ,shutil
from urllib.request import urlopen
def get_file_path(filename):
    return os.path.join(tempfile.gettempdir(), filename)
def load(request,dataa):

    filelist_storage=[]

            
    client = storage.Client()
    BUCKET_NAME = 'aemoscadaprice'
    bucket = client.get_bucket(BUCKET_NAME)

    blobs = bucket.list_blobs()

    for blob in blobs:
       filelist_storage.append(blob.name)
    current = [w.replace('.CSV', '.zip') for w in filelist_storage]
    url = "http://nemweb.com.au/Reports/Current/DispatchIS_Reports/"
    result = urlopen(url).read().decode('utf-8')
    pattern = re.compile(r'[\w.]*.zip')
    filelist = pattern.findall(result)



    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload))
    client = bigquery.Client()

    dataset_ref = client.dataset('aemodataset')
    table_ref = dataset_ref.table('TODAYPRICE')
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.schema = [
    bigquery.SchemaField("SETTLEMENTDATE", "TIMESTAMP"),
    bigquery.SchemaField("REGIONID", "STRING"),
    bigquery.SchemaField("RRP", "NUMERIC"),
                        ]


    for x in files_to_upload:
            with urlopen(url+x) as source, open(get_file_path(x), 'w+b') as target:
                shutil.copyfileobj(source, target)
            df = pd.read_csv(get_file_path(x),skiprows=1,dtype=str,names=range(109),usecols=range(10))
            df.columns = df.iloc[0]
            df = df[1:]
            df=df.query('CASE_SOLUTION=="PRICE"')
            df.columns = df.iloc[0]
            df = df[1:]
            df = df.loc[:, df.columns.notnull()]
            df=df.query('INTERVENTION=="0"')
            df = pd.DataFrame(df, columns=['SETTLEMENTDATE','REGIONID','RRP'])
            df['SETTLEMENTDATE']=pd.to_datetime(df['SETTLEMENTDATE'])
            df['RRP']=pd.to_numeric(df['RRP'])


            y = x.replace('.zip', '.CSV')
            df.to_csv(get_file_path(y),index=False,float_format="%.4f",date_format='%Y-%m-%dT%H:%M:%S.%fZ')
            storage_client = storage.Client()
            buckets = list(storage_client.list_buckets())
            bucket = storage_client.get_bucket(BUCKET_NAME)
            blob = bucket.blob(y)
            blob.upload_from_filename(get_file_path(y))
            with open(get_file_path(y), "rb") as source_file:
                job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

            job.result()  # Waits for table load to complete.
            print("Loaded {} rows into {}:{}.".format(job.output_rows, 'aemodataset', 'TODAYPRICE'))
            os.remove(get_file_path(x))
            os.remove(get_file_path(y))
            print(x)
