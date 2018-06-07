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

class EntityIndexer:
  DOCS_BATCH_SIZE = 1000

  def index_brands(collection):
    docs = []

    mysql_conn = Utils.mysqlConnection()
    query = "SELECT brand_id, brand, brand_popularity, brand_url FROM brands ORDER BY brand_popularity DESC"
    results = Utils.fetchResults(mysql_conn, query)
    ctr = LoopCounter(name='Brand Indexing')
    for row in results:
      ctr += 1 
      if ctr.should_print():
        print(ctr.summary)

      docs.append({
        "_id": createId(row['brand']), 
        "entity": row['brand'], 
        "weight": row['brand_popularity'], 
        "type": "brand",
        "id": row['brand_id']
      })
      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

      print(row['brand'], ctr.count)

    EsUtils.indexDocs(docs, collection)

  def index_categories(collection):
    docs = []

    mysql_conn = Utils.mysqlConnection()
    query = "SELECT id as category_id, name as category_name, url, category_popularity FROM l3_categories where url not like '%luxe%' and url not like '%shop-by-concern%' order by name, category_popularity desc"
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
        "id": row['category_id'],
      })
      if len(docs) == 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

    EsUtils.indexDocs(docs, collection)

  def index_brands_categories(collection):
    docs = []

    mysql_conn = Utils.mysqlConnection()
    query = "SELECT brand_id, brand, category_name, category_id, popularity FROM brand_category"
    results = Utils.fetchResults(mysql_conn, query)
    ctr = LoopCounter(name='Brand Category Indexing - ' + searchengine)
    for row in results:
      ctr += 1 
      if ctr.should_print():
        print(ctr.summary)

      docs.append({
        "_id": createId(row['brand'] +"_"+row['category_name']), 
        "entity": row['brand'] + " " + row['category_name'],  
        "weight": row['popularity'], 
        "type": "brand_category",
        "brand_id": row['brand_id'],
        "brand_name": row['brand'],
        "category_id": row['category_id'],
        "category_name": row['category_name'],
        "source": "brand_category"
      })
      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

      print(row['brand'], ctr.count)

    EsUtils.indexDocs(docs, collection)

  def indexEntities(collection=None, active=None, inactive=None, swap=False, index_categories_arg=False, index_brands_arg=False,index_brands_categories_arg=False, index_all=False):
    index = None
    print('Starting Processing')
    if collection: 
      index = collection 
    elif active:
      index = EsUtils.get_active_inactive_indexes('entity')['active_index']
    elif inactive:
      index = EsUtils.get_active_inactive_indexes('entity')['inactive_index']
    else:
      index = None

    if index_all:
      index_categories_arg = True
      index_brands_arg = True
      index_brands_categories_arg = True

    if index:
      #clear the index
      index_client = EsUtils.get_index_client()
      if index_client.exists(index):
        print("Deleting index: %s" % index)
        index_client.delete(index)
      schema = json.load(open(os.path.join(os.path.dirname(__file__), 'entity_schema.json')))
      index_client.create(index, schema)
      print("Creating index: %s" % index)

      if index_categories_arg:
        EntityIndexer.index_categories(index)
      if index_brands_arg:
        EntityIndexer.index_brands(index)
      if index_brands_categories_arg:
        EntityIndexer.index_brands_categories(index)

    if swap:
      print("Swapping Index")
      indexes = EsUtils.get_active_inactive_indexes('entity')
      EsUtils.switch_index_alias('entity', indexes['active_index'], indexes['inactive_index'])
      exit()

if __name__ == "__main__": 
  parser = argparse.ArgumentParser()

  group = parser.add_argument_group('group')
  group.add_argument("-c", "--category", action='store_true')
  group.add_argument("-b", "--brand", action='store_true')
  group.add_argument("--brand-category", action='store_true')

  collection_state = parser.add_mutually_exclusive_group(required=True)
  collection_state.add_argument("--inactive", action='store_true')
  collection_state.add_argument("--active", action='store_true')
  collection_state.add_argument("--collection")
  
  parser.add_argument("--swap", action='store_true', help="Swap the Core")
  argv = vars(parser.parse_args())

  required_args = ['category', 'brand', 'brand_category']
  index_all = not any([argv[x] for x in required_args])
  EntityIndexer.indexEntities(collection=argv['collection'], active=argv['active'], inactive=argv['inactive'], swap=argv['swap'], index_categories_arg=argv['category'], index_brands_arg=argv['brand'], index_brands_categories_arg=argv['brand_category'], index_all=index_all)

