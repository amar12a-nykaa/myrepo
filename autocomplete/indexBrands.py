#!/usr/bin/python
import re
import sys
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--qa', help='Index to QA', action='store_true')
argv = vars(parser.parse_args())
print(argv)


sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

sys.path.append("/nykaa/scripts/utils")
from loopcounter import LoopCounter

from utils import createId

docs = []
collection='autocomplete'

mysql_conn = Utils.mysqlConnection()
query = "select brand, brand_popularity, brand_url from brands"
results = Utils.fetchResults(mysql_conn, query)
ctr = LoopCounter(name='Products Indexing')
for row in results:
  ctr += 1 
  if ctr.should_print():
    print(ctr.summary)

  #print(row)
  docs.append({"_id": createId(row['brand']), 
      "entity": row['brand'], 
      "weight": row['brand_popularity'], 
      "type": "brand",
      "data": json.dumps({"url": row['brand_url'], "type": "brand"})
    })
  if len(docs) == 10:
    Utils.indexCatalog(docs, collection)
    docs = []

Utils.indexCatalog(docs, collection)

import requests
print("Building suggester .. ")
base_url = Utils.solrBaseURL(collection=collection)
if argv['qa']:
  base_url = re.sub("localhost", "52.221.205.33", base_url)
requests.get(base_url + "suggest?wt=json&suggest.q=la&suggest.build=true")
print("Done")
