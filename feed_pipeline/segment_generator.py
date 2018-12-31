import time
import psycopg2
import psycopg2.extras
import sys
import requests
import os
import boto3
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils
from pas.v2.utils import UserProfileServiceDynamoDb 
from contextlib import closing
sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter

def updateDyanamodb(row,db):
  data = {}
  data["user_id"] = str(row[0])
  if row[1]:
      data["first_name"] = row[1]
  if row[2]:
      data["last_name"] = row[2]
  if row[3]:
      data["gender"] = row[3]
  if row[4]:
      data["dob"] = str(row[4])
  if row[5]:
      data["email"] = row[5]
  db.put_item(item=data)    




db = UserProfileServiceDynamoDb()
connection = Utils.redshiftConnection()
#cursor= connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
query = """
            select customer_id, firstname, lastname, gender, dob, email FROM customer_info c JOIN customer_view v ON c.customer_id=v.id WHERE v.order_total_at_mrp > 0        """
rows = []
print("Starting the script")
with closing(connection.cursor()) as cursor:
  cursor.execute(query)
  ctr = LoopCounter(name='Segment Generator')
  while True:
    batch_empty = True
    for row in cursor.fetchmany(10000):
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)
      batch_empty = False
      rows.append(row)
      updateDyanamodb(row,db)
    if batch_empty:
      break

connection.close()
                                                                                                         






































