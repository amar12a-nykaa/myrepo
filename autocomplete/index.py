#!/usr/bin/python
import argparse
import csv
import json
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

sys.path.append('/nykaa/scripts/sharedutils/')
from solrutils import SolrUtils
from loopcounter import LoopCounter
from utils import createId

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

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

# http://dev.nykaa.com:8983/solr/yang/select?defType=dismax&fl=type&fq=type:"simple" OR type:"configurable"&indent=on&q=lakme&qf=title_text_split&wt=json
# http://dev.nykaa.com:8983/solr/yang/select?bf=popularity&defType=dismax&fl=type,title,price&fq=price: [1 TO *]&fq=type:"simple" OR type:"configurable"&indent=on&q=lakme&qf=title_text_split&wt=json
  for query in [p['query'] for p in search_terms_normalized.find()]:
    #print(query)
    base_url = "http://localhost:8983/solr/yang/select"
    #base_url = "http://qa.nykaa.com:8983/solr/yang/select"
    f = furl(base_url) 
    f.args['defType'] = 'dismax'
    f.args['indent'] = 'on'
    f.args['mm'] = 80
    f.args['qf'] = 'title_text_split'
    f.args['type'] = 'simple,configurable'
    f.args['bf'] = 'popularity'
    f.args['q'] = str(query)
    f.args['fq'] = ['type:"simple" OR type:"configurable"', 'price:[1 TO *]']
    f.args['fl'] = 'title,score,media:[json],product_url,product_id,price,type'
    f.args['wt'] = 'json'
    #print(f.url)
    #exit()
    resp = requests.get(f.url)
    js = resp.json()
    docs = js['response']['docs']
    if docs:
      max_score = max([x['score'] for x in docs])
      docs = [x for x in docs if x['score'] == max_score]
      for doc in docs:
        #print(doc)
        doc['editdistance'] = editdistance.eval(doc['title'].lower(), query.lower()) 
      docs.sort(key=lambda x:x['editdistance'] )
     
      if not docs:
        continue

      doc = docs[0]
      editdistance_threshold = 0.4
      if doc['editdistance'] / len(query) > editdistance_threshold:
        continue

      #print(query)
      #print(doc['title'])
      #print(doc['editdistance'], len(query), doc['editdistance'] / len(query))
      #print("===========")

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

def index_search_queries():
  map_search_product = create_map_search_product()

  docs = []

  search_terms_normalized = MongoClient()['search']['search_terms_normalized']
  cnt_product = 0
  cnt_search = 0

  ctr = LoopCounter(name='Search Queries/Product Indexing')
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



def index_brands():
  docs = []
  collection='autocomplete'

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT brand, brand_popularity, brand_url FROM brands ORDER BY brand_popularity DESC LIMIT 200"
  results = Utils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Brand Indexing')
  for row in results:
    ctr += 1 
    if ctr.should_print():
      print(ctr.summary)

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


def index_categories():
  docs = []
  collection='autocomplete'

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT id as category_id, name as category_name, url, category_popularity FROM l3_categories order by category_popularity desc"
  results = Utils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Category Indexing')
  prev_cat = None
  for row in results:
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    if prev_cat == row['category_name']:
      continue
    prev_cat = row['category_name']

    docs.append({
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

def build_suggester():
  print("Building suggester .. ")
  base_url = Utils.solrBaseURL(collection='autocomplete')
  requests.get(base_url + "suggest?wt=json&suggest.q=la&suggest.build=true")
  print("Done")

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  group = parser.add_argument_group('group')
  group.add_argument("-c", "--category", action='store_true')
  group.add_argument("-b", "--brand", action='store_true')
  group.add_argument("-s", "--search-query", action='store_true')
  argv = vars(parser.parse_args())

  required_args = ['category', 'brand', 'search_query']
  index_all = not any([argv[x] for x in required_args])
  #print("index_all: %s" % index_all)
  #embed()
  
  if argv['category'] or index_all:
    index_categories()
  if argv['brand'] or index_all:
    index_brands()
  if argv['search_query'] or index_all:
    index_search_queries()

  build_suggester()
