import csv
import json
import sys
import boto3
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils
from pas.v2.utils import UserProfileServiceDynamoDb

csvfile = open('Samplecsv.csv', 'r')

header = csvfile.readline()
header = header.rstrip("\n")
columns = header.split(",")

fieldnames = (columns)
reader = csv.DictReader( csvfile, fieldnames)
db = boto3.resource('dynamodb')
table = db.Table('user_profile_service')
filename = "privy"


with open('Samplecsv.csv') as csv_file:
    csvfile.readline()
    for user in csv.DictReader(csv_file):
        new_data = {}
        for column in range(1,len(columns)):
            new_data[columns[column]] = user[columns[column]]
        user_data = table.get_item(Key={'user_id':user['customer_id']})
        if 'Item' in user_data.keys() and 'privy' in user_data['Item'].keys():
          privy_data = user_data['Item']['privy']
        else:
          privy_data={}
        for key in new_data.keys():
            privy_data[key]=new_data[key]
        if 'Item' in user_data.keys():
          table.update_item(Key={'user_id':  user_data['Item']['user_id']},AttributeUpdates={"privy":{"Action": "PUT","Value":privy_data}})  
        else:
          table.put_item(Item = {'user_id':  user['customer_id'],'privy':privy_data})

