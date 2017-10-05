#!/usr/bin/python
import re
import sys
import json
import requests 

sys.path.append('/nykaa/scripts/sharedutils/')
from solrutils import SolrUtils
from loopcounter import LoopCounter

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils


from utils import createId

docs = []
collection='autocomplete'

mysql_conn = Utils.mysqlConnection()
query = "SELECT id as category_id, name as category_name, url, category_popularity FROM l3_categories order by category_popularity desc"
results = Utils.fetchResults(mysql_conn, query)
ctr = LoopCounter(name='Products Indexing')
prev_cat = None
for row in results:
  ctr += 1
  if ctr.should_print():
    print(ctr.summary)

  if prev_cat == row['category_name']:
    continue
  prev_cat = row['category_name']

  docs.append({
      #"_id": "category_" + re.sub('[^A-Za-z0-9]+', '_', row['category_name']) + '_' + row['category_id'],
      "_id": createId(row['category_name']),
      "entity": row['category_name'],
      "weight": row['category_popularity'],
      "type": "category",
      "data": json.dumps({"url": row['url'], "type": "category"})
    })
  if len(docs) == 100:
    SolrUtils.indexDocs(docs, collection)
    requests.get(Utils.solrBaseURL(collection=collection)+ "update?commit=true")
    docs = []

SolrUtils.indexDocs(docs, collection)
requests.get(Utils.solrBaseURL(collection=collection)+ "update?commit=true")

import requests
print("Building suggester .. ")
base_url = Utils.solrBaseURL(collection=collection)
requests.get(base_url + "suggest?wt=json&suggest.q=la&suggest.build=true")
print("Done")
