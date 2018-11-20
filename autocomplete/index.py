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
feedback_data_autocomplete = Utils.mongoClient()['search']['feedback_data_autocomplete']
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

multicategoryList = {
    "3161": {"variant": ["Aria Leya"], "name": "Aria + Leya"},
    "1612": {"variant": ["Arthritis", "Osteoporosis"], "name": "Arthritis & Osteoporosis"},
    "1466": {"variant": ["Badges", "Clothing Pins"], "name": "Badges and Clothing Pins"},
    "4387": {"variant": ["Bags", "Wallets"], "name": "Bags and Wallets"},
    "1311": {"variant": ["Bath Gels", "Shower Gels"], "name": "Bath / Shower Gels"},
    "232": {"variant": ["BB Cream", "CC Cream"], "name": "BB & CC Cream"},
    "2086": {"variant": ["Beard Care", "Moustache Care"], "name": "Beard & Moustache Care"},
    "2604": {"variant": ["Beard Care", "Moustache Care"], "name": "Beard & Moustache Care"},
    "4059": {"variant": ["Beard Trimming", "Beard Shaving"], "name": "Beard Trimming & Shaving"},
    "3627": {"variant": ["Blankets", "Comforters"], "name": "Blankets & Comforters"},
    "1422": {"variant": ["Body Mist", "Body Spray"], "name": "Body Mist/Spray"},
    "1644": {"variant": ["Body Mist", "Body Spray"], "name": "Body Mist/Spray"},
    "970": {"variant": ["Body Mist", "Body Spray"], "name": "Body Mist/Spray"},
    "406": {"variant": ["Body Mist", "Body Splashes"], "name": "Body mists and splashes"},
    "1386": {"variant": ["Body Scrubbers", "Body Brushes"], "name": "Body Scrubbers & Brushes"},
    "3452": {"variant": ["Body Wrap", "Body Envelopment"], "name": "Body Wrap/ Envelopment"},
    "6925": {"variant": ["Bodysuits", "Rompers"], "name": "Bodysuits & Rompers"},
    "6931": {"variant": ["Bodysuits", "Rompers"], "name": "Bodysuits & Rompers"},
    "1607": {"variant": ["Bones", "Joints"], "name": "Bones & Joints"},
    "3652": {"variant": ["Bowls", "Platters"], "name": "Bowls & Platters"},
    "3097": {"variant": ["Bra", "Panty"], "name": "Bra / Panty"},
    "2023": {"variant": ["Brain", "Memory"], "name": "Brain & Memory"},
    "3109": {"variant": ["Bridal", "Sexy"], "name": "Bridal / Sexy"},
    "3083": {"variant": ["Brief", "Hipster"], "name": "Brief / Hipster"},
    "3101": {"variant": ["Camis", "Tops"], "name": "Camis / Tops"},
    "3662": {"variant": ["Candle Holders", "Votives"], "name": "Candle Holders & Votives"},
    "3628": {"variant": ["Candle Holders", "Votives"], "name": "Candle Holders & Votives"},
    "4525": {"variant": ["Capris", "Leggings"], "name": "Capris/Leggings"},
    "4377": {"variant": ["Cards", "Envelopes"], "name": "Cards and Envelopes"},
    "563": {"variant": ["Chains", "Waist Chains"], "name": "Chains and Waist Chains"},
    "1324": {"variant": ["Colognes", "Perfumes"], "name": "Colognes & Perfumes (EDT & EDP)"},
    "7337": {"variant": ["Colognes", "Perfumes"], "name": "Colognes and Perfumes (EDT and EDP)"},
    "430":  {"variant": ["Cotton Buds", "Cotton Balls"], "name": "Cotton Buds & Balls"},
    "1647": {"variant": ["Cotton Buds", "Cotton Balls"], "name": "Cotton Buds and Balls"},
    "1675": {"variant": ["Cotton Buds", "Cotton Balls", "Cotton Wipes"], "name": "Cotton Buds, Balls & Wipes"},
    "1815": {"variant": ["Crabtree", "Evelyn"], "name": "Crabtree & Evelyn"},
    "1676": {"variant": ["Creams", "Lotions", "Oils"], "name": "Creams, Lotions & Oils"},
    "1411": {"variant": ["Curling Irons", "Stylers"], "name": "Curling Irons/Stylers"},
    "346": {"variant": ["Curly", "Wavy"], "name": "Curly & Wavy"},
    "3631": {"variant": ["Cushions", "Covers"], "name": "Cushions & Covers"},
    "2078": {"variant": ["Dark Circles", "Wrinkles"], "name": "Dark Circles / Wrinkles"},
    "536": {"variant": ["Dark Circles", "Wrinkles"], "name": "Dark Circles/Wrinkles"},
    "289": {"variant": ["Day Cream", "Night Cream"], "name": "Day/Night Cream"},
    "3069": {"variant": ["Demi", "Balconette"], "name": "Demi / Balconette"},
    "377": {"variant": ["Deodorants", "Roll Ons"], "name": "Deodorants/Roll-Ons"},
    "971": {"variant": ["Deodorants", "Roll Ons"], "name": "Deodorants/Roll-Ons"},
    "979": {"variant": ["Deodorants", "Roll Ons"], "name": "Deodorants/Roll-ons"},
    "1323": {"variant": ["Deodorants", "Roll Ons"], "name": "Deodorants/Roll-ons"},
    "1650": {"variant": ["Deodorants", "Roll Ons"], "name": "Deodorants/Roll-Ons"},
    "7336": {"variant": ["Deodorants", "Roll Ons"], "name": "Deodorants/Roll-ons"},
    "2046": {"variant": ["Dry Hair", "Frizzy Hair"], "name": "Dry & Frizzy Hair"},
    "573": {"variant": ["Dry Hair", "Frizzy Hair"], "name": "Dry & Frizzy Hair"},
    "362": {"variant": ["Dryers", "Stylers"], "name": "Dryers & Stylers"},
    "967": {"variant": ["EDT", "Colognes"], "name": "EDT/Colognes"},
    "7562": {"variant": ["Face Care", "Body Care"], "name": "Face & Body Care"},
    "290": {"variant": ["Floss", "Tongue Cleaners"], "name": "Floss & Tongue Cleaners"},
    "331": {"variant": ["Gels", "Waxes"], "name": "Gels & Waxes"},
    "3626": {"variant": ["Gift Bags", "Gift Boxes"], "name": "Gift Bags & Boxes"},
    "4386": {"variant": ["Gift Bags", "Gift Boxes"], "name": "Gift Bags and Boxes"},
    "2041": {"variant": ["Hair Creams", "Hair Masks"], "name": "Hair Creams & Masks"},
    "793":  {"variant": ["Hair Gels", "Hair Wax"], "name": "Hair Gels & Wax"},
    "5949": {"variant": ["Hair Tools", "Hair Accessories"], "name": "Hair Tools & Accessories"},
    "1608": {"variant": ["Hair", "Skin", "Nails"], "name": "Hair, Skin & Nails"},
    "1214": {"variant": ["Hairfall", "Thinning"], "name": "Hairfall & Thinning"},
    "583": {"variant": ["Hand Creams", "Foot Creams"], "name": "Hand & Foot Creams"},
    "1318": {"variant": ["Hand Care", "Foot Care"], "name": "Hand and Foot Care"},
    "3671": {"variant": ["Idols", "Murti"], "name": "Idols / Murti"},
    "4432": {"variant": ["Joker", "Witch"], "name": "Joker & Witch"},
    "3737": {"variant": ["Jugs", "Carafe"], "name": "Jugs & Carafe"},
    "3094": {"variant": ["Leggings", "Pants"], "name": "Leggings / Pants"},
    "6909": {"variant": ["Lens Solution", "Lens Accessories"], "name": "Lens Solution & Accessories"},
    "396":  {"variant": ["Loofahs", "Sponges"], "name": "Loofahs & Sponges"},
    "371":  {"variant": ["Lotions", "Creams"], "name": "Lotions & Creams"},
    "691":  {"variant": ["Lotions", "Creams"], "name": "Lotions & Creams"},
    "4397": {"variant": ["Makeup Pouches", "Vanity Kits"], "name": "Makeup Pouches and Vanity Kits"},
    "5987": {"variant": ["Manicure", "Pedicure"], "name": "Manicure & Pedicure"},
    "3277": {"variant": ["Manicure Kits", "Pedicure Kits"], "name": "Manicure & Pedicure Kits"},
    "688": {"variant": ["Manicure Kits", "Pedicure Kits"], "name": "Manicure & Pedicure Kits"},
    "529": {"variant": ["Masks", "Peels"], "name": "Masks & Peels"},
    "1303": {"variant": ["Masks", "Peels"], "name": "Masks & Peels"},
    "227": {"variant": ["Masks", "Peels"], "name": "Masks & Peels"},
    "7309": {"variant": ["Masks", "Peels"], "name": "Masks & Peels"},
    "2075": {"variant": ["Massage Oil", "Body Oil"], "name": "Massage / Body Oil"},
    "554": {"variant": ["Massage Oils", "Carrier Oils"], "name": "Massage / Carrier Oils"},
    "1315": {"variant": ["Massage Oils", "Aromatherapy Oils"], "name": "Massage & Aromatherapy Oils"},
    "1516": {"variant": ["Massage Gels", "Massage Creams"], "name": "Massage Gels & Creams"},
    "3656": {"variant": ["Mugs", "Cups"], "name": "Mugs & Cups"},
    "4376": {"variant": ["Notebooks", "Notepads", "Folders"], "name": "Notebooks, Notepads and Folders"},
    "3819": {"variant": ["Nykaa Kits", "Nykaa Combos"], "name": "Nykaa Kits and Combos"},
    "4527": {"variant": ["Panties", "Girl Shorts"], "name": "Panties/Girl Shorts"},
    "3077": {"variant": ["Pasties", "Stick-ons"], "name": "Pasties / Stick-ons"},
    "974": {"variant": ["Perfumes"], "name": "Perfumes (EDT & EDP)"},
    "962": {"variant": ["Perfumes"], "name": "Perfumes (EDT & EDP)"},
    "375": {"variant": ["Perfumes"], "name": "Perfumes (EDT & EDP)"},
    "3568": {"variant": ["Perfumes"], "name": "Perfumes (EDT & EDP)"},
    "1322": {"variant": ["Perfumes"], "name": "Perfumes (EDT/EDP)"},
    "1290": {"variant": ["Pre Shaves", "Post Shaves"], "name": "Pre & Post Shaves"},
    "1410": {"variant": ["Pre Shaves", "Post Shaves"], "name": "Pre & Post Shaves"},
    "7292": {"variant": ["Pre Shaves", "Post Shaves"], "name": "Pre & Post Shaves"},
    "4412": {"variant": ["Ray", "Dale"], "name": "Ray & Dale"},
    "1287": {"variant": ["Razors", "Cartridges"], "name": "Razors & Cartridges"},
    "1398": {"variant": ["Razors", "Cartridges"], "name": "Razors & Cartridges"},
    "7289": {"variant": ["Razors", "Cartridges"], "name": "Razors & Cartridges"},
    "363": {"variant": ["Rollers", "Curlers"], "name": "Rollers & Curlers"},
    "2478": {"variant": ["Salon", "Spa"], "name": "Salon/Spa"},
    "3115": {"variant": ["Sarongs", "Cover-ups"], "name": "Sarongs & Cover-ups"},
    "4372": {"variant": ["Scarves", "Stoles"], "name": "Scarves and Stoles"},
    "369": {"variant": ["Scrubs", "Exfoliants"], "name": "Scrubs & Exfoliants"},
    "530": {"variant": ["Scrubs", "Exfoliators"], "name": "Scrubs & Exfoliators"},
    "1304": {"variant": ["Scrubs", "Exfoliators"], "name": "Scrubs & Exfoliators"},
    "282": {"variant": ["Scrubs", "Exfoliators"], "name": "Scrubs & Exfoliators"},
    "7310": {"variant": ["Scrubs", "Exfoliators"], "name": "Scrubs & Exfoliators"},
    "6926": {"variant": ["Sets", "Suits"], "name": "Sets & Suits"},
    "6932": {"variant": ["Sets", "Suits"], "name": "Sets & Suits"},
    "7138": {"variant": ["Shaping Camis", "Shaping Slips"], "name": "Shaping Camis & Slips"},
    "1288": {"variant": ["Shavers", "Trimmers"], "name": "Shavers & Trimmers"},
    "2085": {"variant": ["Shavers", "Trimmers"], "name": "Shavers & Trimmers"},
    "7290": {"variant": ["Shavers", "Trimmers"], "name": "Shavers & Trimmers"},
    "800": {"variant": ["Shavers", "Trimmers"], "name": "Shavers & Trimmers"},
    "39": {"variant": ["Shaving", "Hair Removal"], "name": "Shaving & Hair Removal"},
    "1553": {"variant": ["Shaving Creams", "Shaving Foams", "Shaving Gels"], "name": "Shaving Cream, Foams & Gels"},
    "1289": {"variant": ["Shaving Creams", "Shaving Foams", "Shaving Gels"], "name": "Shaving Creams, Foams & Gels"},
    "7291": {"variant": ["Shaving Creams", "Shaving Foams", "Shaving Gels"], "name": "Shaving Creams, Foams & Gels"},
    "1484": {"variant": ["Shopping Bags", "Shopping Totes"], "name": "Shopping Bags and Totes"},
    "368": {"variant": ["Shower Gels", "Body Wash"], "name": "Shower Gels & Body Wash"},
    "4526": {"variant": ["Slips", "Skirts"], "name": "Slips/Skirts"},
    "271": {"variant": ["Sponges", "Applicators"], "name": "Sponges & Applicators"},
    "3455": {"variant": ["Steam", "Bath"], "name": "Steam / Bath"},
    "3079": {"variant": ["Stockings", "Garters"], "name": "Stockings / Garters"},
    "3668": {"variant": ["Storage", "Organization"], "name": "Storage & Organization"},
    "6933": {"variant": ["T-Shirts", "Shirts"], "name": "T-Shirts & Shirts"},
    "3654": {"variant": ["Table Mats", "Table Runners"], "name": "Table Mats & Runners"},
    "4528": {"variant": ["Tanks", "Camis"], "name": "Tanks & Camis"},
    "3092": {"variant": ["Tanks", "Tees"], "name": "Tanks & Tees"},
    "746": {"variant": ["Teeth Care", "Dental Care"], "name": "Teeth & Dental Care"},
    "7466": {"variant": ["Tissue Boxes", "Handkerchiefs"], "name": "Tissue Boxes & Handkerchiefs"},
    "223": {"variant": ["Toners", "Astringents"], "name": "Toners & Astringents"},
    "6927": {"variant": ["Tops", "T-Shirts"], "name": "Tops & T-Shirts"},
    "1481": {"variant": ["Travel Bags", "Backpacks"], "name": "Travel Bags and Backpacks"},
    "4524": {"variant": ["Tummy", "Waist Cinchers"], "name": "Tummy And Waist Cinchers"},
    "3650": {"variant": ["Wall Art", "Wall Mirrors"], "name": "Wall Art & Mirrors"},
    "3658": {"variant": ["Wine Gift Boxes", "Wine Holders"], "name": "Wine Gift Boxes & Holders"},
    "3105": {"variant": ["Wraps", "Gowns"], "name": "Wraps / Gowns"}
  }

brandLandingMap = {"herm" : "https://www.nykaa.com/hermes?ptype=lst&id=7917"}

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

    id = createId(row['brand'])
    url = row['brand_url']
    if id in brandLandingMap.keys():
        url = brandLandingMap[id]
    docs.append({"_id": createId(row['brand']), 
        "entity": row['brand'], 
        "weight": row['brand_popularity'],
        "weight_men" : row['brand_popularity_men'],
        "type": "brand",
        "data": json.dumps({"url": url, "type": "brand", "rank": ctr.count, "id": row['brand_id'], "men_url" : row['brand_men_url']}),
        "id": row['brand_id'],
        "is_men" : is_men,
        "source": "brand"
      
      })
    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

  index_docs(searchengine, docs, collection)

def index_categories(collection, searchengine):
  global multicategoryList

  def getCategoryDoc(row, variant):
    category_url = row['url']
    category_men_url = row['men_url']
    url = "http://www.nykaa.com/search/result/?q=" + variant.replace(" ", "+")
    men_url = "http://www.nykaaman.com/search/result/?q=" + variant.replace(" ", "+")
    is_men = False
    if row['category_popularity_men'] > 0:
      is_men = True
    doc = {
      "_id": createId(variant),
      "entity": variant,
      "weight": row['category_popularity'],
      "weight_men": row['category_popularity_men'],
      "type": "category",
      "data": json.dumps({"url": url, "type": "category", "id": row['category_id'], "category_url": category_url,
                          "men_url": men_url, "category_men_url": category_men_url}),
      "id": row['category_id'],
      "is_men": is_men,
      "source": "category"
    }
    return doc

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

    if row['category_id'] in multicategoryList:
      for variant in multicategoryList[row['category_id']]['variant']:
        categoryDoc = getCategoryDoc(row, variant)
        docs.append(categoryDoc)
    else:
      categoryDoc = getCategoryDoc(row, prev_cat)
      docs.append(categoryDoc)

    if len(docs) >= 100:
      index_docs(searchengine, docs, collection)
      docs = []

  index_docs(searchengine, docs, collection)

def index_brands_categories(collection, searchengine):
  global multicategoryList

  def getBrandCategoryDoc(row, variant):
    is_men = False
    if row['popularity_men'] > 0:
      is_men = True

    url = "http://www.nykaa.com/search/result/?ptype=search&q=" + row['brand'] + " " + variant
    men_url = "http://www.nykaaman.com/search/result/?ptype=search&q=" + row['brand'] + " " + variant
    doc = {"_id": createId(row['brand'] + "_" + variant),
                 "entity": row['brand'] + " " + variant,
                 "weight": row['popularity'],
                 "weight_men": row['popularity_men'],
                 "type": "brand_category",
                 "data": json.dumps({"url": url, "type": "brand_category", "men_url": men_url}),
                 "brand_id": row['brand_id'],
                 "category_id": row['category_id'],
                 "category_name": variant,
                 "is_men": is_men,
                 "source": "brand_category"
                 }
    return doc

  docs = []

  mysql_conn = Utils.mysqlConnection()
  query = "SELECT brand_id, brand, category_name, category_id, popularity, popularity_men FROM brand_category"
  results = Utils.fetchResults(mysql_conn, query)
  ctr = LoopCounter(name='Brand Category Indexing - ' + searchengine)
  for row in results:
    ctr += 1 
    if ctr.should_print():
      print(ctr.summary)

    if row['category_id'] in multicategoryList:
      for variant in multicategoryList[row['category_id']]['variant']:
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
  count = {'value': 0}
    
  def flush_index_products(rows):
    docs = []
    cnt_product = 0
    cnt_search = 0
    cnt_missing_keys = 0 

    ids = [x['_id'] for x in rows]
    if not ids:
      return
    products = fetch_product_by_ids(ids)
    for row in rows:
      id = row['_id']
      #print(parent_id)
      if not row['_id']:
        continue

      product = products.get(id)
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
      data = json.dumps({"type": _type, "url": url, "image": image, 'image_base': image_base,  "id": id, "men_url": men_url})
      count['value'] += 1
      docs.append({
          "_id": createId(product['title']),
          "entity": product['title'], 
          "weight": row['popularity'],
          "weight_men": weight_men,
          "type": _type,
          "data": data,
          "id": id,
          "is_men": is_men,
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
  limit = 150000 if not GLOBAL_FAST_INDEXING else 5000
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

    if count['value'] >= limit:
      break
  flush_index_products(rows_1k)
    


def fetch_product_by_ids(ids):
  DEBUG = False 
  queries = []
  for id in ids:
    query = {
      "query": {
        "bool": 
          {
            "must":[
              {"term":{"product_id": id}}
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
  final_docs = {}
  for docs in [x['hits']['hits'] for x in response['responses']]:
    if docs:
      product_id = docs[0]['_source']['product_id']
      assert len(docs) == 1, "More than 1 docs found for query %s" %product_id
      
      doc = docs[0]['_source']

      if DEBUG:
        print(product_id)
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
      doc = {k:v for k,v in doc.items() if k in ['image', 'image_base', 'title', 'product_url', 'parent_id', 'catalog_tag', 'product_id']}
      final_docs[doc['product_id']] = doc
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
      index_parallel(['category_facets'], **kwargs)
      index_parallel(['categories', 'brands', 'brands_categories'], **kwargs)
    


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
