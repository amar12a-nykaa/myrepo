#!/usr/bin/python
import argparse
import csv
import json
import pandas as pd
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

from stemming.porter2 import stem

sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter
from esutils import EsUtils
from idutils import createId
from categoryutils import getVariants

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
sys.path.append("/nykaa/scripts/utils")
import searchutils as SearchUtils


filter_attribute_map = [("656","concern"), ("661","preference"), ("659","formulation"), ("664","finish"), ("658","color"),
                        ("655","gender"), ("657","skin_type"), ("677","hair_type"), ("857","ingredient"),
                        ("665","skin_tone"), ("663","coverage"), ("812","wiring"), ("813","padding"),
                        ("815","fabric"), ("822","pattern"), ("823","rise")]
FILTER_WEIGHT = 50
ASSORTMENT_WEIGHT = 1


class EntityIndexer:
  DOCS_BATCH_SIZE = 1000

  
  def index_assortment_gap(collection):
    docs = []
    df = pd.read_csv('/nykaa/scripts/feed_pipeline/entity_assortment_gaps_config.csv')
    brand_list = list(df['Brands'])

    ctr = LoopCounter(name='Assortment Gap Indexing')
    for row in brand_list:
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)

      assortment_doc = {
        "_id": createId(row),
        "entity": row,
        "weight": ASSORTMENT_WEIGHT,
        "type": "assortment_gap",
        "id": ctr.count
      }
      for tag in SearchUtils.VALID_CATALOG_TAGS:
        store_doc = assortment_doc.copy()
        store_doc['store'] = tag
        store_doc['_id'] = assortment_doc['_id'] + tag
        docs.append(store_doc)
      
      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

      print(row, ctr.count)

    EsUtils.indexDocs(docs, collection)

  
  def index_brands(collection):
    docs = []
  
    synonyms = {
      'The Body Shop' : ['Body Shop'],
      "The Face Shop": ["Face Shop"],
      "The Body Care": ["Body Care"],
      "Make Up Forever": ["Makeup Forever"],
      "Maybelline New York": ["Maybelline"],
      "NYX Professional Makeup": ["NYX"],
      "Huda Beauty": ["Huda"],
      "Vaadi Herbals": ["Vaadi"],
      "Kama Ayurveda": ["Kama"],
      "Layer'r": ["Layer"],
    }
    mysql_conn = PasUtils.mysqlConnection()
    query = "SELECT brand_id, brand, brand_popularity, store_popularity, brand_url FROM brands ORDER BY brand_popularity DESC"
    results = PasUtils.fetchResults(mysql_conn, query)
    ctr = LoopCounter(name='Brand Indexing')
    for row in results:
      ctr += 1 
      if ctr.should_print():
        print(ctr.summary)

      brand_doc = {
        "_id": createId(row['brand']), 
        "entity": row['brand'], 
        "weight": row['brand_popularity'], 
        "type": "brand",
        "id": row['brand_id']
      }

      if row['brand'] in synonyms:
        brand_doc["entity_synonyms"] = synonyms[row['brand']]
      
      store_popularity = json.loads(row['store_popularity'])
      for tag, value in store_popularity.items():
        if value <= 0.0001:
          continue
        store_doc = brand_doc.copy()
        store_doc['store'] = tag
        store_doc['_id'] = brand_doc['_id'] + tag
        docs.append(store_doc)
      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

      print(row['brand'], ctr.count)

    EsUtils.indexDocs(docs, collection)

  
  def index_categories(collection):

    def getCategoryDoc(row, variant):
      id = "category_" + str(row['category_id']) + "_" + variant + "_" + row['store']
      doc = {
        "_id": createId(id),
        "entity": variant,
        "weight": row['category_popularity'],
        "type": "category",
        "id": row['category_id'],
        "store": row['store']
      }
      return doc
    docs = []
    mysql_conn = PasUtils.mysqlConnection()
    query = "SELECT id as category_id, name as category_name, url, category_popularity, store FROM l3_categories " \
              "where url not like '%luxe%' and url not like '%shop-by-concern%' order by store, name, category_popularity desc"
    results = PasUtils.fetchResults(mysql_conn, query)
    ctr = LoopCounter(name='Category Indexing')
    prev_cat = None
    for row in results:
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)

      if prev_cat == row['category_name']:
        continue
      prev_cat = row['category_name']
      variants = getVariants(row['category_id'])
      if variants:
        for variant in variants:
          categoryDoc = getCategoryDoc(row, variant)
          docs.append(categoryDoc)
      else:
        categoryDoc = getCategoryDoc(row, prev_cat)
        docs.append(categoryDoc)

      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

    EsUtils.indexDocs(docs, collection)

  
  def index_all_categories(collection):

    def getCategoryDoc(row, variant):
      id = "category_" + str(row['category_id']) + "_" + variant + "_" + row['store']
      doc = {
        "_id": createId(id),
        "entity": variant,
        "weight": row['category_popularity'],
        "type": "l1_category",
        "id": row['category_id'],
        "store": row['store']
      }
      return doc
    docs = []
    mysql_conn = PasUtils.mysqlConnection()
    query = """SELECT id as category_id, name as category_name, category_popularity, store FROM all_categories
                order by store, name, category_popularity desc"""
    results = PasUtils.fetchResults(mysql_conn, query)
    ctr = LoopCounter(name='Category Indexing')
    prev_cat = None
    for row in results:
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)

      if prev_cat == row['category_name']:
        continue
      prev_cat = row['category_name']
      variants = getVariants(row['category_id'])
      if variants:
        for variant in variants:
          categoryDoc = getCategoryDoc(row, variant)
          docs.append(categoryDoc)
      else:
        categoryDoc = getCategoryDoc(row, prev_cat)
        docs.append(categoryDoc)

      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

    EsUtils.indexDocs(docs, collection)

  
  def index_filters(collection):
    mysql_conn = PasUtils.nykaaMysqlConnection()
    synonyms = {'10777': {'name': 'Acne/Blemishes', 'synonym': ['acne', 'anti acne', 'blemishes', 'anti blemishes']},
                '10753': {'name': 'Dark Spots/Pigmentation', 'synonym': ['dark spots', 'pigmentation', 'anti pigmentation']},
                '10771': {'name': 'Pores/Blackheads/Whiteheads', 'synonym': ['pores', 'blackheads', 'whiteheads']},
                '10749': {'name': 'Fine Lines/Wrinkles', 'synonym': ['fines lines', 'wrinkle', 'anti wrinkle']},
                '10776': {'name': 'Anti-ageing', 'synonym': ['ageing', 'anti ageing']},
                '10770': {'name': 'Brightening/Fairness', 'synonym': ['brightening', 'fairness']},
                '91637': {'name': 'Hairfall & Thinning', 'synonym': ['anti hairfall', 'hairfall', 'thinning']},
                '91638': {'name': 'Dry & Frizzy Hair', 'synonym': ['dry hair', 'frizzy hair']},
                '10755': {'name': 'Dandruff', 'synonym': ['anti dandruff']},
                '80231': {'name': 'Tan Removal', 'synonym': ['tan', 'anti tan', 'de tan']},
                '12089': {'name': 'Lotion/Body Butter', 'synonym': ['lotion', 'body butter']},
                '10711': {'name': 'Female', 'synonym': ['women', 'woman', 'ladies']},
                '10710': {'name': 'Male', 'synonym': ['men', 'man']},
                '67293': {'name': 'Solid/Plain', 'synonym': ['solid', 'plain']},
                '96358': {'name': 'Embellished/Sequined', 'synonym': ['embellished', 'sequined']},
                '10887': {'name': 'Medium/Wheatish Skin', 'synonym': ['medium skin', 'wheatish skin']},
                '10886': {'name': 'Fair/Light Skin', 'synonym': ['fair skin', 'light skin']},
                '10888': {'name': 'Dusky/Dark Skin', 'synonym': ['dusky skin', 'dark skin']},
                '11075': {'name': 'Normal hair'},
                '11079': {'name': 'Curly hair'},
                '11073': {'name': 'Straight hair'},
                '11076': {'name': 'Fine hair'},
                '11074': {'name': 'Oily hair'},
                '11078': {'name': 'Dry hair'},
                '11077': {'name': 'Dull Hair'},
                '11072': {'name': 'Thick hair'},
                '11071': {'name': 'Thin hair'},
                '91639': {'name': 'Wavy hair'},
                '91643': {'name': 'Argan'},
                '10781': {'name': 'Dry skin'},
                '10779': {'name': 'Oily skin'},
                '10780': {'name': 'Normal skin'},
                '10778': {'name': 'Sensitive skin'},
                '10782': {'name': 'Combination skin'},
    }
    for filt in filter_attribute_map:
      id = filt[0]
      filter = filt[1]
      query = """select eov.value as name, eov.option_id as filter_id from eav_attribute_option eo join eav_attribute_option_value eov
                    on eo.option_id = eov.option_id and eov.store_id = 0 where attribute_id = %s"""%id
      results = PasUtils.fetchResults(mysql_conn, query)
      ctr = LoopCounter(name='%s Indexing' % filter)
      docs = []
      for row in results:
        ctr += 1
        if ctr.should_print():
          print(ctr.summary)

        filter_doc = {
          "_id": createId(row['name']),
          "entity": row['name'].strip(),
          "weight": FILTER_WEIGHT,
          "type": filter,
          "id": str(row['filter_id'])
        }
        if filter_doc["id"] in synonyms:
          filter_doc["entity"] = synonyms[filter_doc["id"]]["name"]
          filter_doc["_id"] = createId(filter_doc["entity"])
          if 'synonym' in synonyms[filter_doc["id"]]:
            filter_doc["entity_synonyms"] = synonyms[filter_doc["id"]]["synonym"]
        for tag in SearchUtils.VALID_CATALOG_TAGS:
          store_doc = filter_doc.copy()
          store_doc['store'] = tag
          store_doc['_id'] = filter_doc['_id'] + tag
          docs.append(store_doc)
        if len(docs) >= 100:
          EsUtils.indexDocs(docs, collection)
          docs = []

        print(filter, filter_doc["entity"], filter_doc["id"])
      EsUtils.indexDocs(docs, collection)

  
  def index(collection=None, active=None, inactive=None, swap=False, index_categories_arg=False,
                      index_brands_arg=False, index_filters_arg=False, index_all=False):
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
      index_filters_arg = True

    if index:
      #clear the index
      index_client = EsUtils.get_index_client()
      if index_client.exists(index):
        print("Deleting index: %s" % index)
        index_client.delete(index)
      schema = json.load(open(os.path.join(os.path.dirname(__file__), 'entity_schema.json')))
      index_client.create(index, schema)
      print("Creating index: %s" % index)

      EntityIndexer.index_assortment_gap(index)
      if index_filters_arg:
        EntityIndexer.index_filters(index)
      if index_categories_arg:
        EntityIndexer.index_all_categories(index)
        EntityIndexer.index_categories(index)
      if index_brands_arg:
        EntityIndexer.index_brands(index)

    if swap:
      print("Swapping Index")
      indexes = EsUtils.get_active_inactive_indexes('entity')
      EsUtils.switch_index_alias('entity', indexes['active_index'], indexes['inactive_index'])


if __name__ == "__main__":
  parser = argparse.ArgumentParser()

  group = parser.add_argument_group('group')
  group.add_argument("-c", "--category", action='store_true')
  group.add_argument("-b", "--brand", action='store_true')
  group.add_argument("--filters", action='store_true')

  collection_state = parser.add_mutually_exclusive_group(required=True)
  collection_state.add_argument("--inactive", action='store_true')
  collection_state.add_argument("--active", action='store_true')
  collection_state.add_argument("--collection")
  
  parser.add_argument("--swap", action='store_true', help="Swap the Core")
  argv = vars(parser.parse_args())

  required_args = ['category', 'brand', 'filters']
  index_all = not any([argv[x] for x in required_args])

  EntityIndexer.index(collection=argv['collection'], active=argv['active'], inactive=argv['inactive'],
                            swap=argv['swap'], index_categories_arg=argv['category'], index_brands_arg=argv['brand'],
                            index_filters_arg=argv['filters'], index_all=index_all)
