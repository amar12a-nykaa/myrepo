import datetime
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

def ResultIter(cursor, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

def fetchResults(connection, query, values=None):
  results = []
  with closing(connection.cursor()) as cursor:
    cursor.execute(query, values)
    for x in  ResultIter(cursor):
      yield dict(zip([col[0] for col in cursor.description], x))

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

mysql_conn = mysql.connector.connect(host=host, user=user, password=password, database=db)

results = fetchResults(mysql_conn, "select item_id from sales_flat_quote_item_option  limit 10 ;")
#results = fetchResults(mysql_conn, "select item_id from sales_flat_quote_item_option  where item_id= '64810314';")
#pprint.pprint(res)
#min_event_id = int(res[0]['min_event_id'] )
#print(min_event_id)

for res in results:
  print("----xx")
  print(res)
  item_id = res['item_id']
  res2 = list(fetchResults(mysql_conn, "select item_id, created_at from sales_flat_quote_item where item_id = %s" % item_id))
  to_delete = False
  if not res2:
    to_delete = True
  else:
    created_at = res2[0]['created_at'] 
    print(created_at)
    if created_at < datetime.datetime(2017, 2, 14, 0, 0):
      to_delete = True

  #print(list(res2))
  if to_delete:
    query = "delete from sales_flat_quote_item_option where item_id = %s" % item_id 
    print(query)
    #fetchResults(mysql_conn, query)
    #cursor = mysql_conn.cursor()
    with closing(mysql_conn.cursor()) as cursor:
    #query = "DELETE FROM products WHERE sku LIKE 'test\_%' OR psku LIKE 'parent\_%'"
      res = cursor.execute(query)
      mysql_conn.commit()

  

  


