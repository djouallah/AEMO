import os
import re
import urllib.request
from google.cloud import storage
def loadfilesgooglestorage(request):
   url = "http://nemweb.com.au/Reports/Current/Daily_Reports/"
   BUCKET_NAME='aemoarchive'
   # get a list of files from the web site
   result = urllib.request.urlopen(url).read().decode('utf-8')
   pattern = re.compile(r'[\w.]*.zip')
   filelist = pattern.findall(result)
   filelist=list(dict.fromkeys(filelist))
   # get a list of files from google storage account
   filelist_storage=[]    
   client = storage.Client()
   bucket = client.get_bucket(BUCKET_NAME)
   blobs = bucket.list_blobs()
   for blob in blobs:
      filelist_storage.append(blob.name)
   # get a list of files from google storage account
   files_to_upload = list(set(filelist)-set(filelist_storage))
   for x in files_to_upload:
         file = urllib.request.urlopen(url+x)
         storage_client = storage.Client()
         bucket = storage_client.get_bucket(BUCKET_NAME)
         blob = bucket.blob(x)
         blob.upload_from_string(file.read())
         print(x)
