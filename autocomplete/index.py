#!/usr/bin/python
import argparse
import csv
import json
from pprint import pprint
import re
import pandas as pd
import sys
import time
import threading
import traceback
import urllib

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
from stemming.porter2 import stem

sys.path.append('/nykaa/scripts/sharedutils/')
from mongoutils import MongoUtils
from loopcounter import LoopCounter
from esutils import EsUtils
from apiutils import ApiUtils
from idutils import createId
from categoryutils import getVariants

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
from disc.v2.utils import MemcacheUtils
sys.path.append("/nykaa/scripts/utils")
import searchutils as SearchUtils

from ensure_mongo_indexes import ensure_mongo_indices_now
ensure_mongo_indices_now()

collection='autocomplete'
search_terms_normalized_daily = MongoUtils.getClient()['search']['search_terms_normalized_daily']
query_product_map_table = MongoUtils.getClient()['search']['query_product_map']
query_product_not_found_table = MongoUtils.getClient()['search']['query_product_not_found']
feedback_data_autocomplete = MongoUtils.getClient()['search']['feedback_data_autocomplete']
top_queries = []
ES_SCHEMA =  json.load(open(  os.path.join(os.path.dirname(__file__), 'schema.json')))
es = DiscUtils.esConn()

STORE_LIST = SearchUtils.VALID_CATALOG_TAGS
GLOBAL_FAST_INDEXING = False

MIN_COUNTS = {
  "product": 1000,
  "brand": 10,
  "category": 20,
  "brand_category": 100,
  "search_query": 2000,
  "category_facet": 20,
}
PasUtils.mysql_write("create or replace view l3_categories_clean as select * from l3_categories where url not like '%shop-by-concern%' and category_popularity>0;")

brandLandingMap = {"herm" : "/hermes?ptype=lst&id=7917"}

def get_brand_formatted(brand_name):
  brand_name_lower = brand_name.lower()
  if 'bynykaafashion' in ''.join(brand_name_lower.split()):
    brand_name_lower = brand_name_lower.replace('by nykaafashion', '')
    brand_name_lower = brand_name_lower.replace('by nykaa fashion', '')
    brand_name = brand_name_lower.title().strip()
  return brand_name

def add_store_popularity(doc, row):
  store_popularity = row.get('store_popularity', "{}")
  if not store_popularity:
    store_popularity = "{}"
  row = json.loads(store_popularity)
  for store in STORE_LIST:
    key = "weight_" + store
    data = row.get(store, 0)
    if data > 0:
      doc.update({"is_"+store: True})
    else:
      doc.update({"is_" + store: False})
    doc.update({key: data})
  return doc

def get_feedback_data(entity):
    search_term = entity.lower()
    feedback_data = feedback_data_autocomplete.find_one({"search_term": search_term})
    if feedback_data:
        return feedback_data['typed_terms']
    return {}

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
    doc['typed_terms'] = get_feedback_data(doc['entity'])
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
  limit = 100000
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

    response = DiscUtils.makeESRequest(querydsl, es_index)
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
  df = pd.read_csv('/nykaa/scripts/low_ctr_queries.csv', encoding="ISO-8859-1", sep=',', header=0)
  df['names'] = df['names'].str.lower()
  low_ctr_query_list = list(df['names'].values)

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
      url = "/search/result/?" + str(urllib.parse.urlencode({'q': corrected_query}))
      data = json.dumps({"type": _type, "url": url, "corrected_query" : corrected_query})
      entity = query 
      cnt_search += 1 

      if entity == "argan oil":
        row['popularity'] = 201

      docs.append({
        "_id" : createId(row['_id']),
        "id": createId(row['_id']),
        "entity": entity,
        "is_corrected": is_corrected,
        "weight": row['popularity'],
        "type": _type,
        "data": data,
        "is_nykaa": True,
        "is_structured": row.get("is_structured", False),
        "weight_nykaa": row['popularity'],
        "is_visible": False if entity in low_ctr_query_list else True,
        "nz_query": row.get("nz_query", True),
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

  mysql_conn = DiscUtils.mysqlConnection()
  query = "SELECT brand_id, brand, brand_popularity, store_popularity, brand_url, brand_men_url FROM brands ORDER BY brand_popularity DESC"
  results = DiscUtils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Brand Indexing - ' + searchengine)
  for row in results:
    ctr += 1 
    if ctr.should_print():
      print(ctr.summary)

    id = createId(row['brand'])
    url = row['brand_url']
    if id in brandLandingMap.keys():
        url = brandLandingMap[id]
    doc = {"_id": createId(row['brand']),
        "entity": row['brand'],
        "weight": row['brand_popularity'],
        "type": "brand",
        "data": json.dumps({"url": url, "type": "brand", "rank": ctr.count, "id": row['brand_id'], "men_url" : row['brand_url']}),
        "id": row['brand_id'],
        "is_visible": True,
        "source": "brand"
    }
    doc = add_store_popularity(doc, row)
    docs.append(doc)
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

  index_docs(searchengine, docs, collection)


def index_category(params):
  def getCategoryDoc(row, variant, type):
    category_url = row['url']
    category_men_url = row['men_url']
    store = row.get('store', 'nykaa')
    id = type + "_" + str(row['category_id']) + "_" + variant + "_" + store
    
    url = "/search/result/?" + str(urllib.parse.urlencode({'q': variant}))
    men_url = "/search/result/?" + str(urllib.parse.urlencode({'q': variant}))
    
    doc = {
      "_id": createId(id),
      "entity": variant,
      "weight": row['category_popularity'],
      "type": type,
      "data": json.dumps({"url": url, "type": "category", "id": row['category_id'], "category_url": category_url,
                          "men_url": men_url, "category_men_url": category_men_url}),
      "id": row['category_id'],
      "is_visible": True,
      "source": "category"
    }
    doc = add_store_popularity(doc, row)
    return doc
  
  docs = []
  tablename = params.get('table')
  type = params.get('type')
  searchengine = params.get('searchengine')
  collection = params.get('collection')
  
  mysql_conn = DiscUtils.mysqlConnection()
  query = "SELECT id as category_id, name as category_name, url, men_url, category_popularity, store_popularity, store " \
          "FROM %s order by name, category_popularity desc" % tablename
  results = DiscUtils.fetchResults(mysql_conn, query)
  name = type + 'Indexing - ' + searchengine
  ctr = LoopCounter(name=name)
  
  for row in results:
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    
    variants = getVariants(row['category_id'])
    if variants:
      for variant in variants:
        categoryDoc = getCategoryDoc(row, variant, type)
        docs.append(categoryDoc)
    else:
      categoryDoc = getCategoryDoc(row, row['category_name'], type)
      docs.append(categoryDoc)
    
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []
  
  index_docs(searchengine, docs, collection)


def index_categories(collection, searchengine):
  params = {
    'collection': collection,
    'searchengine': searchengine,
    'table': 'l3_categories_clean',
    'type': 'category'
  }
  index_category(params)
  

def index_l1_categories(collection, searchengine):
  params = {
    'collection': collection,
    'searchengine': searchengine,
    'table': 'all_categories',
    'type': 'l1_category'
  }
  index_category(params)
  

def index_brands_categories(collection, searchengine):

  def getBrandCategoryDoc(row, variant):
    store = row.get('store', 'nykaa')
    brand_name_formatted = row['brand'].replace('By Nykaa Fashion', '')
    brand_category = row['brand'] + " " + variant
    brand_category_formatted = brand_name_formatted + " " + variant
    url = "/search/result/?ptype=search&" + str(urllib.parse.urlencode({'q': brand_category}))
    men_url = "/search/result/?ptype=search&" + str(urllib.parse.urlencode({'q': brand_category}))
    id = row['brand'] + "_" + variant + "_" + str(row['category_id']) + "_" + store
    doc = {"_id": createId(id),
           "entity": brand_category_formatted,
           "weight": row['popularity'],
           "type": "brand_category",
           "data": json.dumps({"url": url, "type": "brand_category", "men_url": men_url}),
           "brand_id": row['brand_id'],
           "category_id": row['category_id'],
           "category_name": variant,
           "is_visible": True,
           "source": "brand_category"
           }
    doc = add_store_popularity(doc, row)
    return doc

  docs = []

  mysql_conn = DiscUtils.mysqlConnection()
  query = "SELECT brand_id, brand, category_name, category_id, popularity, store_popularity, store FROM brand_category"
  results = DiscUtils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Brand Category Indexing - ' + searchengine)
  for row in results:
    ctr += 1 
    if ctr.should_print():
      print(ctr.summary)
    variants = getVariants(row['category_id'])
    if variants:
      for variant in variants:
        brandCategoryDoc = getBrandCategoryDoc(row, variant)
        docs.append(brandCategoryDoc)
    else:
      brandCategoryDoc = getBrandCategoryDoc(row, row['category_name'])
      docs.append(brandCategoryDoc)

    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

  index_docs(searchengine, docs, collection)

def index_category_facets(collection, searchengine):
  docs = []

  mysql_conn = DiscUtils.mysqlConnection()
  query = "SELECT category_name, category_id, facet_val, popularity, store_popularity, store FROM category_facets"
  results = DiscUtils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Category Facet Indexing - ' + searchengine)
  for row in results:
    ctr += 1 
    if ctr.should_print():
      print(ctr.summary)

    category_facet = row['facet_val'] + " " + row['category_name']
    url = "/search/result/?ptype=search&" + str(urllib.parse.urlencode({'q': category_facet}))
    men_url = "/search/result/?ptype=search&" + str(urllib.parse.urlencode({'q': category_facet}))
    id = row['facet_val'] + "_" + row['category_name'] + "_" + str(row['category_id']) + "_" + row['store']
    doc = {"_id": createId(id),
        "entity": row['facet_val'] + " " + row['category_name'],
        "weight": row['popularity'],
        "type": "category_facet",
        "data": json.dumps({"url": url, "type": "category_facet", "men_url" : men_url}),
        "category_id": row['category_id'],
        "category_name": row['category_name'],
        "is_visible": True,
        "source": "category_facet"
      }
    doc = add_store_popularity(doc, row)
    docs.append(doc)
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

    #print(row['brand'], ctr.count)

  index_docs(searchengine, docs, collection)

def index_custom_queries(collection, searchengine):
  docs = []

  input_file = csv.DictReader(open("/nykaa/scripts/autocomplete/custom_queries.csv"))
  for row in input_file:
    query = row['query']
    _type = 'search_query'
    url = "/search/result/?" + str(urllib.parse.urlencode({'q': query}))
    data = json.dumps({"type": _type, "url": url, "corrected_query": query})
    doc = {
      "_id": createId(query),
      "id": createId(query),
      "entity": query,
      "weight": row['nykaa'],
      "is_corrected": False,
      "is_visible": True,
      "type": _type,
      "data": data,
      "source": "override"
    }
    store_popularity = {}
    for store in STORE_LIST:
      store_popularity[store] = float(row.get(store,0))
    row['store_popularity'] = json.dumps(store_popularity)
    doc = add_store_popularity(doc, row)
    docs.append(doc)
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

  index_docs(searchengine, docs, collection)


def validate_min_count(force_run, allowed_min_docs):
  #force_run = False

  indexes = EsUtils.get_active_inactive_indexes('autocomplete')
  active_index = indexes['active_index']
  inactive_index = indexes['inactive_index']

# Verify correctness of indexing by comparing total number of documents in both active and inactive indexes
  params = {'q': '*:*', 'rows': '0'}
  query = { "size": 0, "query":{ "match_all": {} } }
  num_docs_active = DiscUtils.makeESRequest(query, index=active_index)['hits']['total']
  num_docs_inactive = DiscUtils.makeESRequest(query, index=inactive_index)['hits']['total']
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
    return DiscUtils.makeESRequest(querydsl, es_index)['hits']['total']

  for _type, count in MIN_COUNTS.items():
    found_count = get_count(_type)
    if found_count < count:
      msg = "Failure: Count check failed for %s. Found: %s. Minimum required: %s" % (_type, found_count, count)
      print(msg)
      assert found_count >= count, msg
    else:
      print("Success: Min count check for %s is ok" % _type)



def index_products(collection, searchengine):

  DEBUG = False

  def flush_index_products(products_list):
    docs = []
    cnt_missing_keys = 0
    uniqueList = []
    for product in products_list:
      id = product.get('product_id', 0)
      parent_id = product.get('parent_id', 0)

      if not id:
        continue

      required_keys = set(["product_url", 'image', 'title', 'image_base'])
      missing_keys = required_keys - set(list(product.keys())) 
      if missing_keys:
        #print("[ERROR] Required keys missing for %s: %s" % (parent_id, missing_keys))
        cnt_missing_keys+= 1
        continue

      _type = 'product'
      url = product['product_url']
      url_parts = url.partition('com')
      if len(url_parts) == 3:
        url = url_parts[2]
      image = product['image']
      image_base = product['image_base']
      men_url = None
      if 'men' in product['catalog_tag']:
        men_url = url.replace(".nykaa.com", ".nykaaman.com")
      data = json.dumps({"type": _type, "url": url, "image": image, 'image_base': image_base,  "id": id,
                         "men_url": men_url, "parent_id": parent_id})
      unique_id = createId(product['title'])
      if unique_id in uniqueList:
        continue
      store_popularity = {}
      popularity_to_store = product['popularity']
      if product['popularity'] == 0:
        popularity_to_store = 0.0001
      for store in STORE_LIST:
        if store in product['catalog_tag']:
          store_popularity[store] = popularity_to_store
          
      product['store_popularity'] = json.dumps(store_popularity)
      
      doc = {
          "_id": unique_id,
          "entity": product['title'],
          "weight": product['popularity'],
          "type": _type,
          "data": data,
          "id": id,
          "is_visible": True,
          "source": "product"
        }
      doc = add_store_popularity(doc, product)
      docs.append(doc)
      uniqueList.append(unique_id)
      if len(docs) >= 100:
        index_docs(searchengine, docs, collection)
        docs = []
    index_docs(searchengine, docs, collection)

  ctr = LoopCounter(name='Product Indexing - ' + searchengine)
  results = fetch_products_from_es(size=10000)
  final_docs = []
  count=0
  for docs in results:
    if docs:
      try:
        product_id = docs['_source']['product_id']

        doc = docs['_source']

        if DEBUG:
          print(product_id)
          print(doc['title'])
          print("===========")

        image = ""
        image_base = ""
        try:
          image = json.loads(doc['media'][0])['url']
          image = re.sub("h-[0-9]*,w-[0-9]*,cm", "h-60,w-60,cm", image)
          image_base = re.sub("\/tr[^\/]*", "", image)
        except:
          print("[ERROR] Could not index product because image is missing for product_id: %s" % doc['product_id'])

        doc['image'] = image
        doc['image_base'] = image_base
        doc = {k: v for k, v in doc.items() if
               k in ['image', 'popularity','image_base', 'title', 'product_url', 'parent_id', 'catalog_tag', 'product_id']}
        final_docs.append(doc)
        count+=1
        ctr += 1
        if ctr.should_print():
          print(ctr.summary)
      except Exception as e:
        print(e)
      if count%1000==0:
        flush_index_products(final_docs)
        final_docs = []

  flush_index_products(final_docs)



def fetch_products_from_es(size):
  query = {
    "size": size,
    "query": {
      "bool":
        {
          "must": [
            {"range": {
              "price": {
                "gte": 1
              }
            }},
            {"term": {"is_service": False}},
            {"term": {"is_searchable_i": 1}}
          ],
          "must_not": [
            {"term": {"category_ids": "2413"}},
            {"terms": {"brand_ids": SearchUtils.BRAND_EXCLUDE_LIST}},
            {"term": {"type": "bundle"}},
            {"term": {"disabled": True}}
          ]
        }
    },
    "_source": ["product_id", "popularity", "title", "score", "media", "product_url", "price", "type",
                "parent_id", "catalog_tag"]
  }
  results = EsUtils.scrollESForProducts(index='livecore',query=query)
  return results


def index_engine(engine, collection=None, active=None, inactive=None, swap=False, index_search_queries_arg=False, index_products_arg=False, index_categories_arg=False, index_brands_arg=False,index_brands_categories_arg=False, index_category_facets_arg=False, index_custom_queries_arg=False, index_all=False, force_run=False, allowed_min_docs=0 ):
    assert len([x for x in [collection, active, inactive] if x]) == 1, "Only one of the following should be true"

    if index_all:

      index_products_arg = True
      index_search_queries_arg= True
      index_categories_arg= True
      index_brands_arg= True
      index_brands_categories_arg= True
      index_category_facets_arg = True
      index_custom_queries_arg = True
      index_l1_categories_arg = True

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
      index_parallel(['search_queries'], **kwargs)
      index_parallel(['category_facets'], **kwargs)
      index_parallel(['products'], **kwargs)
      index_parallel(['l1_categories'], **kwargs)
      index_parallel(['categories', 'brands', 'brands_categories'], **kwargs)
      index_parallel(['custom_queries'], **kwargs)
    

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
  group.add_argument("--custom_queries", action='store_true')

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

  required_args = ['category', 'brand', 'search_query', 'product', 'brand_category', 'category_facet', 'custom_queries']
  index_all = not any([argv[x] for x in required_args]) and not argv['buildonly']

  startts = time.time()
  index_engine(engine='elasticsearch', collection=argv['collection'], active=argv['active'], inactive=argv['inactive'], swap=argv['swap'], index_products_arg=argv['product'], index_search_queries_arg=argv['search_query'], index_categories_arg=argv['category'], index_brands_arg=argv['brand'], index_brands_categories_arg=argv['brand_category'], index_category_facets_arg=argv['category_facet'],index_custom_queries_arg=argv['custom_queries'],index_all=index_all, force_run=argv['force'], allowed_min_docs=argv['allowed_min_docs'])
  mins = round((time.time()-startts)/60, 2)
  print("Time taken: %s mins" % mins)
