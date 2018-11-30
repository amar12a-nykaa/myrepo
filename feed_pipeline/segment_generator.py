import time
import psycopg2
import psycopg2.extras
import sys
import requests
import django
import os
import boto3
sys.path.append('/home/apis/nykaa/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'nykaa.settings')
django.setup()
from pas.v2.utils import Utils
from pas.v2.utils import DyanamoDbTable
from pas.v2.models import UserDataStore 
from contextlib import closing

def updateDyanamodb(row):
  data = {}
  data["userId"] = str(row[0])
  if row[1]:
      data["firstName"] = row[1]
  if row[2]:
      data["lastName"] = row[2]
  if row[3]:
      data["gender"] = row[3]
  if row[4]:
      data["dob"] = str(row[4])
  if row[5]:
      data["email"] = row[5]
  UserDataStore.update_user_data(user_id=data["userId"],new_attributes=data)




db = DyanamoDbTable(table_name='NykaaUserData', region_name = 'ap-southeast-1')
connection = Utils.redshiftConnection()
#cursor= connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
query = """
              select customer_id, firstname, lastname, gender, dob, email FROM customer_info 
        """
rows = []
with closing(connection.cursor()) as cursor:
  cursor.execute(query)
  while True:
    batch_empty = True
    for row in cursor.fetchmany(10):
      batch_empty = False
      rows.append(row)
      updateDyanamodb(row)
    if batch_empty:
      break

connection.close()
                                                                                                         






































