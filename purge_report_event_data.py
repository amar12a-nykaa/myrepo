import pprint
import copy
import os
import os.path
import re
import sys
import time
import json
import socket
import requests
import memcache
import traceback
import urllib.request
from functools import wraps
from contextlib import closing

import mysql.connector

ENV = 'prod'
ENV = 'preprod'

def fetchResults(connection, query, values=None):
  results = []
  with closing(connection.cursor()) as cursor:
    cursor.execute(query, values)
    results = [dict(zip([col[0] for col in cursor.description], row))
                 for row in cursor.fetchall()]
  return results

if ENV == 'prod':
  host = "preprod-11april2017.cjmplqztt198.ap-southeast-1.rds.amazonaws.com"
  user = "anik"
  password = "slATy:2Rl9Me5mR"
  db = "nykaalive1"
elif ENV == 'preprod':
  host = "proddbnaykaa-2016-reports01.ciel4c1bqlwh.ap-southeast-1.rds.amazonaws.com"
  user = "nykaalive"
  password = "oh1ued3phi0uh8ooPh6"
  db = "nykaalive1"
else:
  print("Unknown Environment")
  sys.exit()

mysql_connection = mysql.connector.connect(host=host, user=user, password=password, database=db)

res = fetchResults(mysql_connection, "select min(event_id) as min_event_id from report_event limit 10")
#pprint.pprint(res)
min_event_id = int(res[0]['min_event_id'] )
print(min_event_id)

curr_event_id = min_event_id
for i in range(0,10):
  delete_till_id = curr_event_id + 100
  query = "delete from report_event where event_id < %s and logged_at < '2017-03-01 00:00:00'"  % delete_till_id
  print(query)
  #fetchResults(mysql_connection, query)
  curr_event_id = curr_event_id + 100
  

  


