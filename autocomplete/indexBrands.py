#!/usr/bin/python
import re
import sys
import csv
import json

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils
from pymongo import MongoClient

client = MongoClient("52.221.96.159")


docs = []
collection='autocomplete'

mysql_conn = Utils.mysqlConnection()
query = "select brand, brand_popularity, brand_url from brand_category_mappings"
results = Utils.fetchResults(mysql_conn, query)
for row in results:
  print(row)
  docs.append({"_id": "brand_" + re.sub('[^A-Za-z0-9]+', '_', row['brand']), 
      "entity": row['brand'], 
      "weight": row['brand_popularity'], 
      "url": row['brand_url'],
      "type": "brand"
    })
  if len(docs) == 10:
    Utils.indexCatalog(docs, collection)
    docs = []

Utils.indexCatalog(docs, collection)
