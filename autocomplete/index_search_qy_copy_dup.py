#!/usr/bin/python
import argparse
import csv
import json
from pprint import pprint
import re
import sys
import traceback

import arrow
import editdistance
import elasticsearch
import IPython
import mysql.connector
import numpy
import os
import requests
from furl import furl
from IPython import embed
from pymongo import MongoClient
from stemming.porter2 import stem

sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter
from solrutils import SolrUtils
from esutils import EsUtils
from utils import createId

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

collection='autocomplete'

ES_SCHEMA =  json.load(open(  os.path.join(os.path.dirname(__file__), 'schema.json')))
es = Utils.esConn()

def create_map_search_product_es():
  DEBUG = False 
  client = MongoClient()
  search_terms = client['search']['search_terms']
  search_terms_normalized = client['search']['search_terms_normalized']

  map_search_product = {}

  queries = [{'query' : 'lakme enrich lip crayon'}]

  es_index = EsUtils.get_active_inactive_indexes('livecore')['active_index']
  for query in [p['query'] for p in queries]:
    querydsl = {
      "query": {
        "function_score": {
          "query": {
            "bool": {
              "must": {
                "dis_max": {
                  "queries": [
                    {
                      "match": {
                        "title_text_split": {
                          "query": str(query),
                          "minimum_should_match": "-20%"
                        }
                      }
                    }
                  ]
                }
              },
              "filter": [
                {
                  "terms" : {
                    "type" : ["simple", "configurable"]
                  }
                },
                {
                  "range" : {
                    "price" : {
                      "gte" : 1
                    }
                  }
                }
              ]
            }
          },
          "field_value_factor": {
            "field": "popularity",
            "factor": 1,
            "modifier": "none",
            "missing": 1
          },
          "score_mode": "multiply",
          "boost_mode": "multiply"
        }
      },
      "_source" : ["title", "score", "media", "product_url", "product_id", "price", "type"]
    }
    print(querydsl)
    response = Utils.makeESRequest(querydsl, es_index)
    docs = response['hits']['hits']

    if docs:
      max_score = max([x['_score'] for x in docs])
      docs = [x['_source'] for x in docs if x['_score'] == max_score]
      
      for doc in docs:
        doc['editdistance'] = editdistance.eval(doc['title'].lower(), query.lower()) 
      docs.sort(key=lambda x:x['editdistance'] )
     
      if not docs:
        continue

      doc = docs[0]
      editdistance_threshold = 0.4
      if doc['editdistance'] / len(query) > editdistance_threshold:
        continue

      if DEBUG:
        print(query)
        print(doc['title'])
        print(doc['editdistance'], len(query), doc['editdistance'] / len(query))
        print("===========")

      image = ""
      image_base = ""
      try:
        media = json.loads(doc['media'])
        image = media[0]['url']
        image = re.sub("w-[0-9]*", "w-60", image)
        image = re.sub("h-[0-9]*", "h-60", image)
        
        image_base = re.sub("\/tr[^\/]*", "",  image) 
      except:
        print("[ERROR] Could not index query '%s' as product because image is missing for product_id: %s" % (query, doc['product_id']))

      doc['image'] = image 
      doc['image_base'] = image_base 
      doc = {k:v for k,v in doc.items() if k in ['image', 'image_base', 'title', 'product_url', 'product_id']}
      map_search_product[query] = docs[0]
  return map_search_product

def create_map_search_product():
  DEBUG = False 
  client = MongoClient()
  search_terms = client['search']['search_terms']
  search_terms_normalized = client['search']['search_terms_normalized']

  map_search_product = {}

  for query in [p['query'] for p in search_terms_normalized.find(no_cursor_timeout=True)]:
    base_url = Utils.solrHostName() + "/solr/yang/select"
    #embed()
    #exit()
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
    resp = requests.get(f.url)
    js = resp.json()
    docs = js['response']['docs']
    if docs:
      max_score = max([x['score'] for x in docs])
      docs = [x for x in docs if x['score'] == max_score]
      
      for doc in docs:
        doc['editdistance'] = editdistance.eval(doc['title'].lower(), query.lower()) 
      docs.sort(key=lambda x:x['editdistance'] )
     
      if not docs:
        continue

      doc = docs[0]
      editdistance_threshold = 0.4
      if doc['editdistance'] / len(query) > editdistance_threshold:
        continue

      if DEBUG:
        print(query)
        print(doc['title'])
        print(doc['editdistance'], len(query), doc['editdistance'] / len(query))
        print("===========")

      image = ""
      image_base = ""
      try:
        image = doc['media'][0]['url']
        image = re.sub("w-[0-9]*", "w-60", image)
        image = re.sub("h-[0-9]*", "h-60", image)
        
        image_base = re.sub("\/tr[^\/]*", "",  image) 
      except:
        print("[ERROR] Could not index query '%s' as product because image is missing for product_id: %s" % (query, doc['product_id']))

      doc['image'] = image 
      doc['image_base'] = image_base 
      doc = {k:v for k,v in doc.items() if k in ['image', 'image_base', 'title', 'product_url', 'product_id']}
      map_search_product[query] = docs[0]
  return map_search_product

def index_search_queries(collection, searchengine):
  map_search_product = create_map_search_product()
  map_search_product_es = create_map_search_product_es()

  docs = []

  search_terms_normalized = MongoClient()['search']['search_terms_normalized']
  cnt_product = 0
  cnt_search = 0
  cnt_product_es = 0
  cnt_search_es = 0
  ctr = LoopCounter(name='Search Queries')
  num_errors_searchquery_to_product_mapping = 0
  num_errors_searchquery_to_product_mapping_es = 0
  num_search_queries_that_should_map_to_products = 0
  num_search_queries_that_should_map_to_products_es = 0
  for row in search_terms_normalized.find(no_cursor_timeout=True):
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    if not row['_id']:
      continue

    query = row['query']
    line = {}
    is_query_mapped_to_a_product_successfully = False
    if query in map_search_product:
      num_search_queries_that_should_map_to_products += 1
      _type = 'product'
      try:
        url = map_search_product[query]['product_url']
        image = map_search_product[query]['image']
        image_base = map_search_product[query]['image_base']
        product_id = map_search_product[query]['product_id']

        data = json.dumps({"type": _type, "url": url, "image": image, "image_base": image_base, "id": product_id })
        cnt_product += 1 
        entity = map_search_product[query]['title']
        is_query_mapped_to_a_product_successfully = True
        line['solr'] = entity
      except:
        #print("map_search_product[%s]: %s" %(query, map_search_product[query]))
        print("ERROR: Error in mapping  productid: %s to search term '%s'" % (product_id, query))
        num_errors_searchquery_to_product_mapping += 1

    is_query_mapped_to_a_product_successfully_es = False
    if query in map_search_product_es:
      num_search_queries_that_should_map_to_products_es += 1
      _type = 'product'
      try:
        url = map_search_product_es[query]['product_url']
        image = map_search_product_es[query]['image']
        image_base = map_search_product_es[query]['image_base']
        product_id = map_search_product_es[query]['product_id']

        data = json.dumps({"type": _type, "url": url, "image": image, "image_base": image_base, "id": product_id })
        cnt_product_es += 1 
        entity = map_search_product_es[query]['title']
        is_query_mapped_to_a_product_successfully_es = True
        line['es'] = entity
      except:
        #print("map_search_product[%s]: %s" %(query, map_search_product[query]))
        print("ERROR: Error in mapping  productid: %s to search term '%s'" % (product_id, query))
        num_errors_searchquery_to_product_mapping_es += 1

    if not is_query_mapped_to_a_product_successfully:
      _type = 'search_query'
      url = "http://www.nykaa.com/search/result/?q=" + row['query'].replace(" ", "+")
      data = json.dumps({"type": _type, "url": url})
      entity = query 
      cnt_search += 1 
      line['solr'] = ''
    if not is_query_mapped_to_a_product_successfully_es:
      _type = 'search_query'
      url = "http://www.nykaa.com/search/result/?q=" + row['query'].replace(" ", "+")
      data = json.dumps({"type": _type, "url": url})
      entity = query 
      cnt_search_es += 1 
      line['es'] = ''

    if entity == "argan oil":
      row['popularity'] = 200

    print(row['query'] + ','  + line['solr'] + ',' + line['es'])

    if len(docs) >= 100:
      docs = []
  
  total_search_queries = search_terms_normalized.count()
  if num_errors_searchquery_to_product_mapping/num_search_queries_that_should_map_to_products * 100  > 2:
    raise Exception("Solr Too many search queries failed to get mapped to products. Expected: %s. Failed: %s" % \
      (num_search_queries_that_should_map_to_products, num_errors_searchquery_to_product_mapping))


  if num_errors_searchquery_to_product_mapping_es/num_search_queries_that_should_map_to_products_es * 100  > 2:
    raise Exception("ES Too many search queries failed to get mapped to products. Expected: %s. Failed: %s" % \
      (num_search_queries_that_should_map_to_products_es, num_errors_searchquery_to_product_mapping_es))

  #print("fail percentage:")
  #print(num_errors_searchquery_to_product_mapping/num_search_queries_that_should_map_to_products * 100)

  #print("fail percentage:")
  #print(num_errors_searchquery_to_product_mapping/num_search_queries_that_should_map_to_products * 100)

  print("solr cnt_product: %s" % cnt_product)
  print("solr cnt_search: %s" % cnt_search)

  print("ES cnt_product: %s" % cnt_product)
  print("ES cnt_search: %s" % cnt_search)


#index_search_queries('yin', 'elasticsearch')
print(create_map_search_product_es())
