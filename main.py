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
def load(request):

    filelist_storage=[]

            
    client = storage.Client()
    BUCKET_NAME = 'aemoscada'
    bucket = client.get_bucket(BUCKET_NAME)

    blobs = bucket.list_blobs()

    for blob in blobs:
       filelist_storage.append(blob.name)
    current = [w.replace('.CSV', '.zip') for w in filelist_storage]
    url = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
    result = urlopen(url).read().decode('utf-8')
    pattern = re.compile(r'[\w.]*.zip')
    filelist = pattern.findall(result)



    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload))
    print(files_to_upload)
    client = bigquery.Client()

    dataset_ref = client.dataset('aemodataset')
    table_ref = dataset_ref.table('TODAYDUID')
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.schema = [
    bigquery.SchemaField("SETTLEMENTDATE", "TIMESTAMP"),
    bigquery.SchemaField("DUID", "STRING"),
    bigquery.SchemaField("SCADAVALUE", "NUMERIC"),
                        ]


    for x in files_to_upload:
            with urlopen(url+x) as source, open(get_file_path(x), 'w+b') as target:
                shutil.copyfileobj(source, target)
            df = pd.read_csv(get_file_path(x),skiprows=1,usecols=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],parse_dates=["SETTLEMENTDATE"])
            df=df.dropna(how='all') #drop na
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
            print("Loaded {} rows into {}:{}.".format(job.output_rows, 'aemodataset', 'TODAYDUID'))
            os.remove(get_file_path(x))
            os.remove(get_file_path(y))
            print(x)
