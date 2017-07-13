#!/usr/bin/python
import IPython
import sys
import csv
import json
import pprint
import operator
import mysql.connector

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils
from contextlib import closing
from pymongo import MongoClient

client = MongoClient("52.221.96.159")

host = "localhost"
user = "root"
password = "nykaa2017"
db = "nykaa"

nykaadb = mysql.connector.connect(host=host, user=user, password=password, database=db)
docs = []
collection='autocomplete'
query = "select brand, brand_popularity, brand_url from nykaa.brand_category_mappings"
with closing(nykaadb.cursor(dictionary=True)) as cursor:
  cursor.execute(query)
  for row in cursor.fetchall():
    print(row)
    docs.append({"_id": "brand-" + row['brand'].replace(" ","_"), 
        "entity": row['brand'], 
        "weight": row['brand_popularity'], 
        "url": row['brand_url'],
        "type": "brand"
      })
    if len(docs) == 10:
      Utils.indexCatalog(docs, collection)
      docs = []

Utils.indexCatalog(docs, collection)
