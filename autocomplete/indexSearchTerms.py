#!/usr/bin/python
import re
import sys
import json
from pymongo import MongoClient
sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

sys.path.append("/nykaa/scripts/feed_pipeline")
from pipelineUtils import SolrUtils

sys.path.append("/nykaa/scripts/utils")
from loopcounter import LoopCounter

from utils import createId

docs = []
collection='autocomplete'

search_terms_normalized = MongoClient()['search']['search_terms_normalized']

ctr = LoopCounter(name='Products Indexing')
for row in search_terms_normalized.find():
  ctr += 1
  if ctr.should_print():
    print(ctr.summary)
  if not row['_id']:
    continue

  docs.append({
      "_id": row['_id'],
      #"_id": "search_" + row['_id'], 
      "entity": row['query'],
      "weight": row['popularity'], 
      "type": "search_query",
      "data": json.dumps({"type": "search_query", "url": "http://www.nykaa.com/search/result/?q=" + row['query'].replace(" ", "+")})
    })

  if len(docs) == 10:
    SolrUtils.indexCatalog(docs, collection)
    docs = []

SolrUtils.indexCatalog(docs, collection)

import requests
print("Building suggester .. ")
base_url = Utils.solrBaseURL(collection=collection)
requests.get(base_url + "suggest?wt=json&suggest.q=la&suggest.build=true")
print("Done")
