#!/usr/bin/python
import argparse
import csv
import json
from pprint import pprint
import re
import sys
import time
import threading
import traceback

import arrow
import editdistance
import elasticsearch
import IPython
import mysql.connector
import numpy
import os
import pymongo
import requests
from furl import furl
from IPython import embed
from pymongo import MongoClient
from stemming.porter2 import stem

sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter
from esutils import EsUtils
from apiutils import ApiUtils
from idutils import createId

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils, MemcacheUtils

from ensure_mongo_indexes import ensure_mongo_indices_now
ensure_mongo_indices_now()

collection='autocomplete'
search_terms_normalized_daily = Utils.mongoClient()['search']['search_terms_normalized_daily']
query_product_map_table = Utils.mongoClient()['search']['query_product_map']
query_product_not_found_table = Utils.mongoClient()['search']['query_product_not_found']
top_queries = []
ES_SCHEMA =  json.load(open(  os.path.join(os.path.dirname(__file__), 'schema.json')))
es = Utils.esConn()

GLOBAL_FAST_INDEXING = False

MIN_COUNTS = {
  "product": 30000,
  "brand": 1000,
  "category": 200,
  "brand_category": 10000,
  "search_query": 40000,
  "category_facet": 600,
}
Utils.mysql_write("create or replace view l3_categories_clean as select * from l3_categories where url not like '%luxe%' and url not like '%shop-by-concern%' and category_popularity>0;")

def restart_apache_memcached():
  print("Restarting Apache and Memcached")
  os.system("/etc/init.d/apache2 restart")
  MemcacheUtils.flush_all()

def write_dict_to_csv(dictname, filename):
  with open(filename, 'w') as csv_file:
      writer = csv.writer(csv_file)
      for key, value in dictname.items():
         writer.writerow([key, value])

def index_docs(searchengine, docs, collection):
  for doc in docs:
    doc['entity'] += " s" # This is a trick to hnadle sideeffect of combining shingles and edge ngram token filters
  assert searchengine == 'elasticsearch'
  EsUtils.indexDocs(docs, collection)

def create_map_search_product():
  DEBUG = False 
  map_search_product = {}
  for prod in query_product_map_table.find():
    map_search_product[prod['_id']] = prod

  map_search_product_not_found = {}
  for prod in query_product_not_found_table.find():
    map_search_product_not_found[prod['_id']] = ""

  es_index = EsUtils.get_active_inactive_indexes('livecore')['active_index']
  limit = 50000
  ctr = LoopCounter(name='create_map_search_product')
  for query in (p['query'] for p in search_terms_normalized_daily.find(no_cursor_timeout=True).sort([("popularity", pymongo.DESCENDING)])):
    if len(query)>50:
      continue

    if len(top_queries) == limit:
      break

    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    if createId(query) in map_search_product:
      continue
    if createId(query) in map_search_product_not_found:
      top_queries.append(query)
      continue

    querydsl = {
      "query": {
        "function_score": {
          "query": {
            "bool": {
              "must": {
                "dis_max": {
                  "queries": [ { "match": { "title_text_split": { "query": str(query), "minimum_should_match": "-20%" } } } ]
                }
              },
              "filter": [
                { "terms" : { "type" : ["simple", "configurable"] } },
                { "range" : { "price" : { "gte" : 1 } } }
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

    response = Utils.makeESRequest(querydsl, es_index)
    docs = response['hits']['hits']

    if not docs:
      top_queries.append(query)
      query_product_not_found_table.update_one({"_id": createId(query)}, {"$set": {"query": query}}, upsert=True)
    else:
      max_score = max([x['_score'] for x in docs])
      docs = [x['_source'] for x in docs if x['_score'] == max_score]
      
      for doc in docs:
        doc['editdistance'] = editdistance.eval(doc['title'].lower(), query.lower()) 
      docs.sort(key=lambda x:x['editdistance'] )
     
      if not docs:
        top_queries.append(query)
        query_product_not_found_table.update_one({"_id": createId(query)}, {"$set": {"query": query}}, upsert=True)
        continue

      doc = docs[0]
      editdistance_threshold = 0.4
      if doc['editdistance'] / len(query) > editdistance_threshold:
        top_queries.append(query)
        query_product_not_found_table.update_one({"_id": createId(query)}, {"$set": {"query": query}}, upsert=True)
        continue

      if DEBUG:
        print(query)
        print(doc['title'])
        print(doc['editdistance'], len(query), doc['editdistance'] / len(query))
        print("===========")

      image = ""
      image_base = ""
      try:
        media = json.loads(doc['media'][0])
        image = media['url']
        image = re.sub("w-[0-9]*", "w-60", image)
        image = re.sub("h-[0-9]*", "h-60", image)
        image_base = re.sub("\/tr[^\/]*", "",  image) 
      except:
        print("[ERROR] Could not index query '%s' as product because image is missing for product_id: %s" % (query, doc['product_id']))
      doc['image'] = image 
      doc['image_base'] = image_base 
      doc = {k:v for k,v in doc.items() if k in ['image', 'image_base', 'title', 'product_url', 'product_id']}
      doc['query'] = query 

      map_search_product[createId(query)] = docs[0]
      query_product_map_table.update_one({"_id": createId(query)}, {"$set": doc}, upsert=True)

  return map_search_product

def index_search_queries(collection, searchengine):
  map_search_product = create_map_search_product()

  docs = []

  cnt_product = 0
  cnt_search = 0

  ctr = LoopCounter(name='Search Queries')
  n = 1000 
  top_queries.reverse()
  assert top_queries
  for queries_1k in  [top_queries[i * n:(i + 1) * n] for i in range((len(top_queries) + n - 1) // n )]: 
    for row in search_terms_normalized_daily.find({'query' : {"$in": queries_1k}}):
      if len(row['query'])>50:
        continue
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)

      query = row['query']
      corrected_query = row['suggested_query']
      is_corrected = False
      if(query != corrected_query):
        is_corrected = True
      _type = 'search_query'
      url = "http://www.nykaa.com/search/result/?q=" + corrected_query.replace(" ", "+")
      data = json.dumps({"type": _type, "url": url})
      entity = query 
      cnt_search += 1 

      if entity == "argan oil":
        row['popularity'] = 201

      docs.append({
        "_id" : createId(row['_id']),
        "id": createId(row['_id']),
        "entity": entity,
        "corrected_query": corrected_query,
        "is_corrected": is_corrected,
        "weight": row['popularity'],
        "type": _type,
        "data": data,
        "source": "search_query"
      })

      if len(docs) >= 100:
        index_docs(searchengine, docs, collection)
        docs = []
  
  total_search_queries = search_terms_normalized_daily.count()

  print("cnt_product: %s" % cnt_product)
  print("cnt_search: %s" % cnt_search)

  index_docs(searchengine, docs, collection)

def index_brands(collection, searchengine):
  docs = []

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT brand_id, brand, brand_popularity, brand_popularity_men, brand_url, brand_men_url FROM brands ORDER BY brand_popularity DESC"
  results = Utils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Brand Indexing - ' + searchengine)
  for row in results:
    ctr += 1 
    if ctr.should_print():
      print(ctr.summary)
    is_men = False
    if row['brand_popularity_men'] > 0:
      is_men = True

    docs.append({"_id": createId(row['brand']), 
        "entity": row['brand'], 
        "weight": row['brand_popularity'],
        "weight_men" : row['brand_popularity_men'],
        "type": "brand",
        "data": json.dumps({"url": row['brand_url'], "type": "brand", "rank": ctr.count, "id": row['brand_id'], "men_url" : row['brand_men_url']}),
        "id": row['brand_id'],
        "is_men" : is_men,
        "source": "brand"
      
      })
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

  index_docs(searchengine, docs, collection)

def index_categories(collection, searchengine):
  docs = []

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT id as category_id, name as category_name, url, men_url, category_popularity, category_popularity_men FROM l3_categories_clean order by name, category_popularity desc"
  results = Utils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Category Indexing - ' + searchengine)
  prev_cat = None
  for row in results:
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    if prev_cat == row['category_name']:
      continue
    prev_cat = row['category_name']

    category_url = row['url']
    category_men_url = row['men_url']
    url = "http://www.nykaa.com/search/result/?q=" + prev_cat.replace(" ", "+")
    men_url = "http://www.nykaaman.com/search/result/?q=" + prev_cat.replace(" ", "+")

    is_men = False
    if row['category_popularity_men'] > 0:
      is_men = True

#    if row['category_name'].lower() in ['concealer', 'lipstick', 'nail polish', 'eyeliner', 'kajal']:
#      continue
    docs.append({
        "_id": createId(row['category_name']),
        "entity": row['category_name'],
        "weight": row['category_popularity'],
        "weight_men" : row['category_popularity_men'],
        "type": "category",
        "data": json.dumps({"url": url, "type": "category", "id": row['category_id'], "category_url" : category_url,
                            "men_url":men_url, "category_men_url" : category_men_url}),
        "id": row['category_id'],
        "is_men" : is_men,
        "source": "category"
      })
    if len(docs) == 100:
      index_docs(searchengine, docs, collection)
      docs = []

  index_docs(searchengine, docs, collection)

def index_brands_categories(collection, searchengine):
  docs = []

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT brand_id, brand, category_name, category_id, popularity, popularity_men FROM brand_category"
  results = Utils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Brand Category Indexing - ' + searchengine)
  for row in results:
    ctr += 1 
    if ctr.should_print():
      print(ctr.summary)

    is_men = False
    if row['popularity_men'] > 0:
      is_men = True

    url = "http://www.nykaa.com/search/result/?ptype=search&q=" + row['brand'] + " " + row['category_name']
    men_url = "http://www.nykaaman.com/search/result/?ptype=search&q=" + row['brand'] + " " + row['category_name']
    docs.append({"_id": createId(row['brand'] +"_"+row['category_name']), 
        "entity": row['brand'] + " " + row['category_name'],  
        "weight": row['popularity'],
        "weight_men" : row['popularity_men'],
        "type": "brand_category",
        "data": json.dumps({"url": url, "type": "brand_category", "men_url" : men_url}),
        "brand_id": row['brand_id'],
        "category_id": row['category_id'],
        "category_name": row['category_name'],
        "is_men" : is_men,
        "source": "brand_category"
      })
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []


  index_docs(searchengine, docs, collection)

def index_category_facets(collection, searchengine):
  docs = []

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT category_name, category_id, facet_val, popularity, popularity_men FROM category_facets"
  results = Utils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Category Facet Indexing - ' + searchengine)
  for row in results:
    ctr += 1 
    if ctr.should_print():
      print(ctr.summary)

    is_men = False
    if row['popularity_men'] > 0:
      is_men = True

    url = "http://www.nykaa.com/search/result/?ptype=search&q=" + row['facet_val'] + " " + row['category_name']
    men_url = "http://www.nykaaman.com/search/result/?ptype=search&q=" + row['facet_val'] + " " + row['category_name']
    docs.append({"_id": createId(row['facet_val'] +"_"+row['category_name']), 
        "entity": row['facet_val'] + " " + row['category_name'],  
        "weight": row['popularity'],
        "weight_men" : row['popularity_men'],
        "type": "category_facet",
        "data": json.dumps({"url": url, "type": "category_facet", "men_url" : men_url}),
        "category_id": row['category_id'],
        "category_name": row['category_name'],
        "is_men" : is_men,
        "source": "category_facet"
      })
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

    #print(row['brand'], ctr.count)

  index_docs(searchengine, docs, collection)


def validate_min_count(force_run, allowed_min_docs):
  #force_run = False

  indexes = EsUtils.get_active_inactive_indexes('autocomplete')
  active_index = indexes['active_index']
  inactive_index = indexes['inactive_index']

# Verify correctness of indexing by comparing total number of documents in both active and inactive indexes
  params = {'q': '*:*', 'rows': '0'}
  query = { "size": 0, "query":{ "match_all": {} } }
  num_docs_active = Utils.makeESRequest(query, index=active_index)['hits']['total']
  num_docs_inactive = Utils.makeESRequest(query, index=inactive_index)['hits']['total']
  print('Number of documents in active index(%s): %s'%(active_index, num_docs_active))
  print('Number of documents in inactive index(%s): %s'%(inactive_index, num_docs_inactive))

# if it decreased more than 5% of current, abort and throw an error
  if not num_docs_active:
    if num_docs_inactive:
      docs_ratio = 1
    else:
      docs_ratio = 0
  else:
    docs_ratio = num_docs_inactive/num_docs_active
  if docs_ratio < 0.95 and not force_run:
    if allowed_min_docs > 0 and  num_docs_inactive > allowed_min_docs:
      print("Validation is OK since num_docs_inactive > allowed_min_docs")
      return
    msg = "[ERROR] Number of documents decreased by more than 5% of current documents. Please verify the data or run with --force option to force run the indexing."
    print(msg)
    raise Exception(msg)

  def get_count(_type):
    querydsl = { "size": 0, "query":{ "match": {"type": _type} } }
    indexes = EsUtils.get_active_inactive_indexes('autocomplete')
    es_index = indexes['inactive_index']
    return Utils.makeESRequest(querydsl, es_index)['hits']['total']

  for _type, count in MIN_COUNTS.items():
    found_count = get_count(_type)
    if found_count < count:
      msg = "Failure: Count check failed for %s. Found: %s. Minimum required: %s" % (_type, found_count, count)
      print(msg)
      assert found_count >= count, msg
    else:
      print("Success: Min count check for %s is ok" % _type)



def index_products(collection, searchengine):


  popularity = Utils.mongoClient()['search']['popularity']

    
  def flush_index_products(rows):
    docs = []
    cnt_product = 0
    cnt_search = 0
    cnt_missing_keys = 0 

    ids = [x['_id'] for x in rows]
    if not ids:
      return
    products = fetch_product_by_parentids(ids)
    for row in rows:

      parent_id = row['parent_id']
      #print(parent_id)
      if not row['_id']:
        continue

      product = products.get(parent_id)
      if not product:
        continue
      required_keys = set(["product_url", 'image', 'title', 'image_base'])
      missing_keys = required_keys - set(list(product.keys())) 
      if missing_keys:
        #print("[ERROR] Required keys missing for %s: %s" % (parent_id, missing_keys))
        cnt_missing_keys+= 1
        continue

      _type = 'product'
      url = product['product_url']
      image = product['image']
      image_base = product['image_base']
      men_url = None
      weight_men = 0
      is_men = False
      if 'men' in product['catalog_tag']:
        men_url = url.replace(".nykaa.com", ".nykaaman.com")
        weight_men = row['popularity']
        is_men = True
      data = json.dumps({"type": _type, "url": url, "image": image, 'image_base': image_base,  "id": parent_id, "men_url" : men_url})
      #cnt_product += 1 
      docs.append({
          "_id": createId(product['title']),
          "entity": product['title'], 
          "weight": row['popularity'],
          "weight_men" : weight_men,
          "type": _type,
          "data": data,
          "id": parent_id,
          "is_men" : is_men,
          "source": "product"
        })

      if len(docs) >= 100:
        index_docs(searchengine, docs, collection)
        docs = []
    index_docs(searchengine, docs, collection)

  #print("cnt_product: %s" % cnt_product)
#  print("cnt_search: %s" % cnt_search)
#  print("cnt_missing_keys: %s" % cnt_missing_keys)


  rows_1k = []
  rows_untested = {}
  ids = []

  ctr = LoopCounter(name='Product Indexing - ' + searchengine)
  limit = 50000 if not GLOBAL_FAST_INDEXING else 5000
  count = 0 
  for row in popularity.find(no_cursor_timeout=True).sort([("popularity", pymongo.DESCENDING)]):# .limit(limit):
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    rows_untested[row['_id']] = row
    if len(rows_untested) >= 1000:
      ids = list(rows_untested.keys())
      for product in requests.get("http://"+ApiUtils.get_host()+"/apis/v2/product.listids?ids=%s" % ",".join(ids)).json()['result']['products']:
        if product['price'] < 1 or product['pro_flag'] == 1 or product['is_service'] == True:
          continue
        rows_1k.append(rows_untested[product['product_id']])
      flush_index_products(rows_1k)
      rows_1k = []
      rows_untested = {}

    if ctr.count >= limit:
      break
  flush_index_products(rows_1k)
    


def fetch_product_by_parentids(parent_ids):
  DEBUG = False 
  queries = []
  for parent_id in parent_ids:
    query = {
      "query": {
        "bool": 
          {
            "must":[
              {"term":{"product_id": parent_id}},
              {"term":{"parent_id": parent_id}}
            ],
            "must_not": [
              {"term":{"category_ids": "2413"}}
            ], 
            "should": [
              {"term":{"type": "simple"}},
              {"term":{"type": "configurable"}}
              ]
          }
        },
      "_source":["product_id", "title","score", "media", "product_url", "price", "type", "parent_id", "catalog_tag"]
    }
    queries.append("{}")
    queries.append(json.dumps(query))

  product = {}
  response = Utils.makeESRequest(queries, 'livecore', msearch=True)
  response['responses'][0]['hits']['hits']
  final_docs = {}
  for docs in [x['hits']['hits'] for x in response['responses']]:
    if docs:
      assert len(docs) == 1, "More than 1 docs foud for query %s" % parent_id
      
      doc = docs[0]['_source']

      if DEBUG:
        print(parent_id)
        print(doc['title'])
        print("===========")

      image = ""
      image_base = ""
      try:
        image = json.loads(doc['media'][0])['url']
        image = re.sub("w-[0-9]*", "w-60", image)
        image = re.sub("h-[0-9]*", "h-60", image)
        image_base = re.sub("\/tr[^\/]*", "",  image) 
      except:
        print("[ERROR] Could not index product because image is missing for product_id: %s" % doc['product_id'])

      doc['image'] = image 
      doc['image_base'] = image_base 
      doc = {k:v for k,v in doc.items() if k in ['image', 'image_base', 'title', 'product_url', 'parent_id', 'catalog_tag']}
      final_docs[doc['parent_id']] = doc
  return final_docs


def index_engine(engine, collection=None, active=None, inactive=None, swap=False, index_search_queries_arg=False, index_products_arg=False, index_categories_arg=False, index_brands_arg=False,index_brands_categories_arg=False, index_category_facets_arg=False, index_all=False, force_run=False, allowed_min_docs=0 ):
    assert len([x for x in [collection, active, inactive] if x]) == 1, "Only one of the following should be true"

    if index_all:

      index_products_arg = True
      index_search_queries_arg= True
      index_categories_arg= True
      index_brands_arg= True
      index_brands_categories_arg= True
      index_category_facets_arg = True

    print(locals())
    assert engine == 'elasticsearch'
    EngineUtils = EsUtils

    index = None
    print('Starting %s Processing' % engine)
    if collection: 
      index = collection 
    elif active:
      index = EngineUtils.get_active_inactive_indexes('autocomplete')['active_index']
    elif inactive:
      index = EngineUtils.get_active_inactive_indexes('autocomplete')['inactive_index']
    else:
      index = None

    print("Index: %s" % index)

    index_client = elasticsearch.client.IndicesClient(es)
    if index_all and index_client.exists(index = index):
      print("Deleting index: %s" % index)
      index_client.delete(index = index)
    if not index_client.exists(index = index):
      index_client.create( index=index, body= ES_SCHEMA)

    if index:
      print("Indexing: %s" % index)
      def index_parallel(_types, **kwargs):
        threads = []
        for _type in _types:
          arg = kwargs["index_" + _type + "_arg"] 
          func = globals()["index_" + _type ] 
          if arg:
            t = threading.Thread(target=func, args=(index, engine))
            threads.append((_type,t))
            t.start()

        for _type, t in threads: 
          print("Waiting for thread: %s" % _type)
          t.join()
          print("Thread %s is complete" % _type)

      kwargs = {k:v for k,v in locals().items() if 'index_' in k}
      index_parallel(['search_queries', 'products'], **kwargs)
      index_parallel(['categories', 'brands', 'brands_categories', 'category_facets'], **kwargs)
    


      print('Done processing ',  engine)
      restart_apache_memcached()
    
    if swap:
      print("Swapping Index")
      validate_min_count(force_run=force_run, allowed_min_docs=allowed_min_docs)
      indexes = EngineUtils.get_active_inactive_indexes('autocomplete')
      EngineUtils.switch_index_alias('autocomplete', indexes['active_index'], indexes['inactive_index'])

if __name__ == '__main__':

  #embed()
  #map=create_map_search_product()
  #pprint(fetch_product_by_parentids(['27269', '28760']))
  #exit()
  parser = argparse.ArgumentParser()

  group = parser.add_argument_group('group')
  group.add_argument("-c", "--category", action='store_true')
  group.add_argument("-b", "--brand", action='store_true')
  group.add_argument("-s", "--search-query", action='store_true')
  group.add_argument("-p", "--product", action='store_true')
  group.add_argument("--brand-category", action='store_true')
  group.add_argument("--category-facet", action='store_true')

  parser.add_argument("--buildonly", action='store_true', help="Build Suggester")
  parser.add_argument("--fast", action='store_true', help="Index a fraction of products and search queries to save on indexing time")
  parser.add_argument("--force", action='store_true', help="Ignore Validation")
  parser.add_argument("--allowed_min_docs", type=int, default=0, help="Minimum number of docs allowed")

  collection_state = parser.add_mutually_exclusive_group(required=True)
  collection_state.add_argument("--inactive", action='store_true')
  collection_state.add_argument("--active", action='store_true')
  collection_state.add_argument("--collection")
  
  parser.add_argument("--swap", action='store_true', help="Swap the Core")

  argv = vars(parser.parse_args())

  GLOBAL_FAST_INDEXING = argv['fast']

  required_args = ['category', 'brand', 'search_query', 'product', 'brand_category', 'category_facet']
  index_all = not any([argv[x] for x in required_args]) and not argv['buildonly']

  startts = time.time()
  index_engine(engine='elasticsearch', collection=argv['collection'], active=argv['active'], inactive=argv['inactive'], swap=argv['swap'], index_products_arg=argv['product'], index_search_queries_arg=argv['search_query'], index_categories_arg=argv['category'], index_brands_arg=argv['brand'], index_brands_categories_arg=argv['brand_category'], index_category_facets_arg=argv['category_facet'],index_all=index_all, force_run=argv['force'], allowed_min_docs=argv['allowed_min_docs'])
  mins = round((time.time()-startts)/60, 2)
  print("Time taken: %s mins" % mins)
