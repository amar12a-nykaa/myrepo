#!/usr/bin/python
import re
import sys
import json

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils


docs = []
collection='autocomplete'

mysql_conn = Utils.mysqlConnection()
query = "select brand, brand_popularity, brand_url from brands"
results = Utils.fetchResults(mysql_conn, query)
for row in results:
  print(row)
  docs.append({"_id": "brand_" + re.sub('[^A-Za-z0-9]+', '_', row['brand']), 
      "entity": row['brand'], 
      "weight": row['brand_popularity'], 
      "data": json.dumps({"url": row['brand_url'], "type": "brand"})
    })
  if len(docs) == 10:
    Utils.indexCatalog(docs, collection)
    docs = []

Utils.indexCatalog(docs, collection)

import requests
print("Building suggester .. ")
base_url = Utils.solrBaseURL(collection=collection)
requests.get(base_url + "suggest?wt=json&suggest.q=la&suggest.build=true")
print("Done")
