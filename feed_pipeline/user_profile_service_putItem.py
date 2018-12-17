import csv
import json
import sys
import boto3
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils
from pas.v2.utils import UserProfileServiceDynamoDb

csvfile = open('SampleCSVFile.csv', 'r')

header = csvfile.readline()
header = header.rstrip("\n")
columns = header.split(",")

fieldnames = (columns)
reader = csv.DictReader( csvfile, fieldnames)
db = boto3.resource('dynamodb')
table = db.Table('NykaaUserData')
filename = "privy"


with open('SampleCSVFile.csv') as csv_file:
    csvfile.readline()
    for user in csv.DictReader(csv_file):
        new_data = {}
        for column in range(1,len(columns)):
            new_data[columns[column]] = user[columns[column]]
 #       print(new_data)
        user_data = table.get_item(Key={'userId':user['customer_id']})
        if 'privy' in user_data['Item'].keys():
          privy_data = user_data['Item']['privy']
        else:
          privy_data={}
        for key in new_data.keys():
            privy_data[key]=new_data[key]
        table.update_item(Key={'user_id':  user_data['Item']['user_id']},AttributeUpdates={"privy":{"Action": "PUT","Value":privy_data}})  


