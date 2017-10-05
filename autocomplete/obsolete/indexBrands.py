#!/usr/bin/python
import requests
import re
import sys
import json
import argparse

sys.path.append('/nykaa/scripts/sharedutils/')
from solrutils import SolrUtils
from loopcounter import LoopCounter

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils
from utils import createId


def index_brands():
  docs = []
  collection='autocomplete'

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT brand, brand_popularity, brand_url FROM brands ORDER BY brand_popularity DESC LIMIT 200"
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
    if len(docs) >= 100:
      SolrUtils.indexDocs(docs, collection)
      requests.get(Utils.solrBaseURL(collection=collection)+ "update?commit=true")
      docs = []

  SolrUtils.indexDocs(docs, collection)
  requests.get(Utils.solrBaseURL(collection=collection)+ "update?commit=true")

def build_suggester():
  import requests
  print("Building suggester .. ")
  base_url = Utils.solrBaseURL(collection=collection)
  requests.get(base_url + "suggest?wt=json&suggest.q=la&suggest.build=true")
  print("Done")

if __name__ == '__main__':
  index_brands()
  build_suggester()



