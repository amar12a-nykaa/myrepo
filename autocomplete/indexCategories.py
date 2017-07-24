#!/usr/bin/python
import re
import sys
import json

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

docs = []
collection='autocomplete'

mysql_conn = Utils.mysqlConnection()
query = "SELECT id as category_id, name as category_name, url, category_popularity FROM l3_categories"
results = Utils.fetchResults(mysql_conn, query)
for row in results:
  docs.append({"_id": "category_" + re.sub('[^A-Za-z0-9]+', '_', row['category_name']) + '_' + row['category_id'],
      "entity": row['category_name'],
      "weight": row['category_popularity'],
      "data": json.dumps({"url": row['url'], "type": "category"})
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
