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
from utils import createId

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

collection='autocomplete'

def write_dict_to_csv(dictname, filename):
  with open(filename, 'w') as csv_file:
      writer = csv.writer(csv_file)
      for key, value in dictname.items():
         writer.writerow([key, value])

def index_docs(searchengine, docs, collection):
  if searchengine == 'solr':
    SolrUtils.indexDocs(docs, collection)
    requests.get(Utils.solrBaseURL(collection=collection)+ "update?commit=true")
  if searchengine == 'elasticsearch':
    EsUtils.indexDocs(docs, collection)

def create_map_search_product():
  DEBUG = False 
  client = MongoClient()
  search_terms = client['search']['search_terms']
  search_terms_normalized = client['search']['search_terms_normalized']

  map_search_product = {}

  for query in [p['query'] for p in search_terms_normalized.find()]:
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

  docs = []

  search_terms_normalized = MongoClient()['search']['search_terms_normalized']
  cnt_product = 0
  cnt_search = 0

  ctr = LoopCounter(name='Search Queries')
  num_errors_searchquery_to_product_mapping = 0
  num_search_queries_that_should_map_to_products = 0
  for row in search_terms_normalized.find():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    if not row['_id']:
      continue

    query = row['query']
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

      except:
        #print("map_search_product[%s]: %s" %(query, map_search_product[query]))
        print("ERROR: Error in mapping  productid: %s to search term '%s'" % (product_id, query))
        num_errors_searchquery_to_product_mapping += 1

    if not is_query_mapped_to_a_product_successfully:
      _type = 'search_query'
      url = "http://www.nykaa.com/search/result/?q=" + row['query'].replace(" ", "+")
      data = json.dumps({"type": _type, "url": url})
      entity = query 
      cnt_search += 1 

    if entity == "argan oil":
      row['popularity'] = 200

    docs.append({
      "_id" : createId(row['_id']),
      "id": createId(row['_id']),
      "entity": entity,
      "weight": row['popularity'],
      "type": _type,
      "data": data,
    })

    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []
  
  total_search_queries = search_terms_normalized.count()
  if num_errors_searchquery_to_product_mapping/num_search_queries_that_should_map_to_products * 100  > 2:
    raise Exception("Too many search queries failed to get mapped to products. Expected: %s. Failed: %s" % \
      (num_search_queries_that_should_map_to_products, num_errors_searchquery_to_product_mapping))

  #print("fail percentage:")
  #print(num_errors_searchquery_to_product_mapping/num_search_queries_that_should_map_to_products * 100)


  print("cnt_product: %s" % cnt_product)
  print("cnt_search: %s" % cnt_search)

  index_docs(searchengine, docs, collection)

def index_brands(collection, searchengine):
  docs = []

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT brand_id, brand, brand_popularity, brand_url FROM brands ORDER BY brand_popularity DESC"
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
        "data": json.dumps({"url": row['brand_url'], "type": "brand", "rank": ctr.count, "id": row['brand_id']}),
        "id": row['brand_id'],
      })
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

    print(row['brand'], ctr.count)

  index_docs(searchengine, docs, collection)

def index_categories(collection, searchengine):
  docs = []

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT id as category_id, name as category_name, url, category_popularity FROM l3_categories order by name, category_popularity desc"
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

#    if row['category_name'].lower() in ['concealer', 'lipstick', 'nail polish', 'eyeliner', 'kajal']:
#      continue
    docs.append({
        "_id": createId(row['category_name']),
        "entity": row['category_name'],
        "weight": row['category_popularity'],
        "type": "category",
        "data": json.dumps({"url": row['url'], "type": "category", "id": row['category_id']}),
        "id": row['category_id']
      })
    if len(docs) == 100:
      index_docs(searchengine, docs, collection)
      docs = []

  index_docs(searchengine, docs, collection)

def index_products(collection, searchengine):

  docs = []

  popularity = MongoClient()['search']['popularity']

  cnt_product = 0
  cnt_search = 0
  cnt_missing_solr = 0 
  cnt_missing_keys = 0 

  ctr = LoopCounter(name='Product Indexing')
  for row in popularity.find():
    parent_id = row['parent_id']
    #print(parent_id)
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    if not row['_id']:
      continue

    product = fetch_product_by_parentid(parent_id)
    if not product:
      #print("[ERROR] Product missing in solr yin yang: %s" % parent_id)
      cnt_missing_solr += 1
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

    data = json.dumps({"type": _type, "url": url, "image": image, 'image_base': image_base,  "id": parent_id})
    cnt_product += 1 
    docs.append({
        "_id": createId(product['title']),
        "entity": product['title'], 
        "weight": row['popularity'], 
        "type": _type,
        "data": data,
        "id": parent_id
      })

    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

  print("cnt_product: %s" % cnt_product)
  print("cnt_search: %s" % cnt_search)
  print("cnt_missing_solr: %s" % cnt_missing_solr)
  print("cnt_missing_keys: %s" % cnt_missing_keys)

  index_docs(searchengine, docs, collection)

def build_suggester(collection):
  print("Building suggester .. ")
  base_url = Utils.solrBaseURL(collection=collection)
  requests.get(base_url + "suggest?wt=json&suggest.q=la&suggest.build=true")
  r = requests.get(base_url + "suggestsmall?wt=json&suggest.q=la&suggest.build=true")

def index_all(collection):
  index_search_queries(collection)
  index_products(collection)
  index_categories(collection)
  index_brands(collection)
  build_suggester(collection)

def fetch_product_by_parentid(parent_id):
  DEBUG = False 
  product = {}
  base_url = Utils.solrHostName() + "/solr/yang/select"
  f = furl(base_url) 
  f.args['q'] = 'parent_id:%s AND product_id:%s' % (parent_id,parent_id)
  f.args['fq'] = 'type:"simple" OR type:"configurable"'
  f.args['fl'] = 'title,score,media:[json],product_url,product_id,price,type'
  f.args['wt'] = 'json'
  resp = requests.get(f.url)
  js = resp.json()
  docs = js['response']['docs']
  if docs:
    assert len(docs) == 1, "More than 1 docs foud for query %s" % parent_id
    
    doc = docs[0]

    if DEBUG:
      print(parent_id)
      print(doc['title'])
      print("===========")

    image = ""
    image_base = ""
    try:
      image = doc['media'][0]['url']
      image = re.sub("w-[0-9]*", "w-60", image)
      image = re.sub("h-[0-9]*", "h-60", image)
      image_base = re.sub("\/tr[^\/]*", "",  image) 
    except:
      print("[ERROR] Could not index product because image is missing for product_id: %s" % doc['product_id'])

    doc['image'] = image 
    doc['image_base'] = image_base 
    doc = {k:v for k,v in doc.items() if k in ['image', 'image_base', 'title', 'product_url']}
    return doc
  return None


if __name__ == '__main__':

  #print(fetch_product_by_parentid("7723"))
  #map=create_map_search_product()
  #embed()
  #exit()
  parser = argparse.ArgumentParser()

  group = parser.add_argument_group('group')
  group.add_argument("-c", "--category", action='store_true')
  group.add_argument("-b", "--brand", action='store_true')
  group.add_argument("-s", "--search-query", action='store_true')
  group.add_argument("-p", "--product", action='store_true')

  parser.add_argument("--buildonly", action='store_true', help="Build Suggester")

  collection_state = parser.add_mutually_exclusive_group(required=True)
  collection_state.add_argument("--inactive", action='store_true')
  collection_state.add_argument("--active", action='store_true')
  collection_state.add_argument("--collection")
  collection_state.add_argument("--swap", action='store_true', help="Swap the Core")

  argv = vars(parser.parse_args())

  required_args = ['category', 'brand', 'search_query', 'product']
  index_all = not any([argv[x] for x in required_args]) and not argv['buildonly']
 
  # solr
  if argv['collection']:
    collection = argv['collection']
  elif argv['active']:
    collection = SolrUtils.get_active_inactive_collections('autocomplete')['active_collection']
  elif argv['inactive']:
    collection = SolrUtils.get_active_inactive_collections('autocomplete')['inactive_collection']
  elif argv['swap']:
    SolrUtils.swap_core('autocomplete')
    exit()

  print("Indexing to collection: %s" % collection)
  if argv['search_query'] or index_all:
    index_search_queries(collection, 'solr')
  if argv['product'] or index_all:
    index_products(collection, 'solr')
  if argv['category'] or index_all:
    index_categories(collection, 'solr')
  if argv['brand'] or index_all:
    index_brands(collection, 'solr')
  build_suggester(collection)


  if argv['buildonly']:
    build_suggester(collection)

  # elastic search
  print('Done processing Solr')
  print('Starting ElasticSearch Processing')
  if argv['collection']:
    index = argv['collection']
  elif argv['active']:
    index = EsUtils.get_active_inactive_indexes('autocomplete')['active_index']
  elif argv['inactive']:
    index = EsUtils.get_active_inactive_indexes('autocomplete')['inactive_index']
  elif argv['swap']:
    indexes = EsUtils.get_active_inactive_indexes('autocomplete')
    EsUtils.switch_index_alias('autocomplete', indexes['active_index'], indexes['inactive_index'])
    exit()

  print("Indexing: %s" % index)
  if argv['search_query'] or index_all:
    index_search_queries(index, 'elasticsearch')
  if argv['product'] or index_all:
    index_products(index, 'elasticsearch')
  if argv['category'] or index_all:
    index_categories(index, 'elasticsearch')
  if argv['brand'] or index_all:
    index_brands(index, 'elasticsearch')

  print("Restarting Apache and Memcached")
  os.system("/etc/init.d/apache2 restart")
  os.system("/etc/init.d/memcached restart")
  
