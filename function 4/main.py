import os
from datetime import datetime, date, timedelta
import urllib.request as urllib2
#import json
import tempfile
import pandas as pd
import gcsfs
#from google.cloud import storage
import re ,shutil
from urllib.request import urlopen
def get_file_path(filename):
    return os.path.join(tempfile.gettempdir(), filename)
def load(request):    
    
    url = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
    result = urlopen(url).read().decode('utf-8')
    pattern = re.compile(r'[\w.]*.zip')
    filelist1 = pattern.findall(result)
    filelist_unique = dict.fromkeys(filelist1)
    filelist_sorted=sorted(filelist_unique, reverse=True)
    filelist = filelist_sorted[:288]

    #client = storage.Client()
    #BUCKET_NAME = 'aemotoday'
    #bucket = client.get_bucket(BUCKET_NAME)
    df = pd.read_csv('gs://aemotoday/log_scada.csv',names=["name"])

    current = df['name'].tolist()
    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload)) 
    print(files_to_upload)
    
    for x in files_to_upload:
            with urlopen(url+x) as source, open(get_file_path(x), 'w+b') as target:
                shutil.copyfileobj(source, target)
            df = pd.read_csv(get_file_path(x),skiprows=1,usecols=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],parse_dates=["SETTLEMENTDATE"])
            df=df.dropna(how='all') #drop na
            df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])
            df['Date'] = df['SETTLEMENTDATE'].dt.date
            df.to_parquet('gs://aemotoday/scada',partition_cols=['Date'],allow_truncated_timestamps=True)
            current.append(x)
            log = pd.DataFrame (current,columns=['name'])
            if len(x) >1:
                 log.to_csv('gs://aemotoday/log_scada.csv',index=False)
            os.remove(get_file_path(x))
