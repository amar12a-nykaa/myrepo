import csv
import json
import sys
import boto3
import pandas as pd
import subprocess
sys.path.append('/home/apis/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils
from pas.v2.utils import UserProfileServiceDynamoDb
from os import listdir
from datetime import datetime, timedelta

yesterday = datetime.now() - timedelta(days=1)
foldername = yesterday.strftime('%Y%m%d')

cmd = "/usr/local/bin/aws s3 sync "+"s3://nykaa-ups/daily_data/"+foldername+" "+"/home/ubuntu/nykaa_scripts/feed_pipeline/data/"+foldername+"/"
out = subprocess.check_output(cmd , shell=True).strip()
if out:
    print(out)

files = [f for f in listdir("/home/ubuntu/nykaa_scripts/feed_pipeline/data/"+foldername)]
print(files)

db = boto3.resource('dynamodb')
table = db.Table('user_profile_service')

for filename in files:
  csvfile = open("/home/ubuntu/nykaa_scripts/feed_pipeline/data/"+foldername+"/"+filename, 'r')
  header = csvfile.readline()
  header = header.rstrip("\n")
  columns = header.split(",")
  fileName = filename
  attribute_name = fileName.split("__")
  attribute = attribute_name[0]
  with open("/home/ubuntu/nykaa_scripts/feed_pipeline/data/"+foldername+"/"+filename) as csv_file:
    for user in csv.DictReader(csv_file):
      new_data = {}
      for column in range(1,len(columns)):
        new_data[columns[column]] = user[columns[column]]
      user_data = table.get_item(Key={'user_id':user['customer_id']})
      if 'Item' in user_data.keys() and attribute in user_data['Item'].keys():
        privy_data = user_data['Item'][attribute]
      else:
        privy_data={}
      for key in new_data.keys():
        privy_data[key]=new_data[key]
      if 'Item' in user_data.keys():
        table.update_item(Key={'user_id':  user_data['Item']['user_id']},AttributeUpdates={attribute:{"Action": "PUT","Value":privy_data}})
      else:
        table.put_item(Item = {'user_id':  user['customer_id'],attribute:privy_data})
