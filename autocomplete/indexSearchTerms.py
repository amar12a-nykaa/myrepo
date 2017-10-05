#!/usr/bin/python
import argparse
import csv
import datetime
import json
import os
import os.path
import pprint
import re
import sys
import traceback

import arrow
import editdistance
import IPython
import mysql.connector
import numpy
import omniture
import pandas as pd
import requests
from furl import furl
from IPython import embed
from pymongo import MongoClient
from stemming.porter2 import stem


sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter
from utils import createId
from solrutils import SolrUtils


collection='autocomplete'

def write_dict_to_csv(dictname, filename):
	with open(filename, 'w') as csv_file:
			writer = csv.writer(csv_file)
			for key, value in dictname.items():
				 writer.writerow([key, value])

def create_map_search_product():
  client = MongoClient()
  search_terms = client['search']['search_terms']
  search_terms_normalized = client['search']['search_terms_normalized']

  map_search_product = {}

  for query in [p['query'] for p in search_terms_normalized.find()]:
    #print(query)
    base_url = "http://localhost:8983/solr/yang/select"
    f = furl(base_url) 
    f.args['defType'] = 'dismax'
    f.args['indent'] = 'on'
    f.args['mm'] = 80
    f.args['qf'] = 'title_text_split'
    f.args['type'] = 'simple'
    f.args['bf'] = 'popularity'
    f.args['price'] = '[1 TO *]'
    f.args['q'] = str(query)
    f.args['fl'] = 'title,score,media:[json],product_url,product_id'
    f.args['wt'] = 'json'
    resp = requests.get(f.url)
    js = resp.json()
    docs = js['response']['docs']
    if docs:
      max_score = max([x['score'] for x in docs])
      docs = [x for x in docs if x['score'] == max_score]
      for doc in docs:
        #print(doc)
        doc['editdistance'] = editdistance.eval(doc['title'], query) 
      docs.sort(key=lambda x:x['editdistance'] )
     
      editdistance_threshold = 0.2 * len(query)
      docs = [x for x in docs if abs(x['editdistance'] - len(query)) <= editdistance_threshold]
      if not docs:
        continue
      doc = docs[0]
      image = ""
      try:
        image = doc['media'][0]['url']
        image = re.sub("w-[0-9]*", "w-200", image)
        image = re.sub("h-[0-9]*", "h-200", image)
      except:
        print("[ERROR] Could not index query '%s' as product because image is missing for product_id: %s" % (query, doc['product_id']))

      doc['image'] = image 
      doc = {k:v for k,v in doc.items() if k in ['thumbnail', 'title', 'product_url']}
      map_search_product[query] = docs[0]
      #print(doc)
  return map_search_product

def index_search_terms():
  map_search_product = create_map_search_product()

  docs = []

  search_terms_normalized = MongoClient()['search']['search_terms_normalized']
  cnt_product = 0
  cnt_search = 0

  ctr = LoopCounter(name='Products Indexing')
  for row in search_terms_normalized.find():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    if not row['_id']:
      continue

    query = row['query']
    if query in map_search_product:
      _type = 'product'
      url = map_search_product[query]['product_url']
      image = map_search_product[query]['image']

      data = json.dumps({"type": _type, "url": url, "image": image})
      cnt_product += 1 
      entity = map_search_product[query]['title']
      #embed()
      #exit()
    else:
      _type = 'search_query'
      url = "http://www.nykaa.com/search/result/?q=" + row['query'].replace(" ", "+")
      data = json.dumps({"type": _type, "url": url})
      entity = query 
      cnt_search += 1 

    docs.append({
        "_id": row['_id'],
        "entity": entity, 
        "weight": row['popularity'], 
        "type": _type,
        "data": data,
      })

    if len(docs) >= 100:
      SolrUtils.indexDocs(docs, collection)
      requests.get(Utils.solrBaseURL(collection=collection)+ "update?commit=true")
      docs = []

  print("cnt_product: %s" % cnt_product)
  print("cnt_search: %s" % cnt_search)

  SolrUtils.indexDocs(docs, collection)
  requests.get(Utils.solrBaseURL(collection=collection)+ "update?commit=true")

#parser = argparse.ArgumentParser()
#parser.add_argument("--dryrun",  action='store_true')
#parser.add_argument("--debug",  action='store_true')
#parser.add_argument("--id", default=0)
#argv = vars(parser.parse_args())

index_search_terms()


print("Building suggester .. ")
base_url = Utils.solrBaseURL(collection=collection)
requests.get(base_url + "suggest?wt=json&suggest.q=la&suggest.build=true")
print("Done")
