import argparse
import datetime
import json
import os
import os.path
import pprint
import re
import sys
import traceback
from collections import OrderedDict
from contextlib import closing

import arrow
import mysql.connector
import numpy
import omniture
import pandas as pd
import pymongo
from IPython import embed
from pymongo import MongoClient, UpdateOne
from stemming.porter2 import stem

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

from ensure_mongo_indexes import ensure_mongo_indices_now

from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize
ps = PorterStemmer()

DAILY_THRESHOLD = 3

client = Utils.mongoClient()
search_terms_daily = client['search']['search_terms_daily']
search_terms_normalized = client['search']['search_terms_normalized_daily']
corrected_search_query = client['search']['corrected_search_query']
search_terms_normalized.remove({})
ensure_mongo_indices_now()

correct_term_list = ["correct words","everyuth","kerastase","farsali","krylon","armaf","Cosrx","focallure","ennscloset",
                     "studiowest","odonil","gucci","kryolan","sephora","foreo","footwear","rhoda","Fenty","Hilary","spoolie","jovees","devacurl","biore",
                     "quirky","stay","parampara","dermadew","kokoglam","embryolisse","tigi","mediker","dermacol","Anastasia","essie","sale","bajaj","burberry",
                     "sesa","sigma","spencer","puna","modicare","hugo","gelatin","stila","ordinary","spawake","mederma","mauri","benetint","amaira","meon","tony",
                     "renee","boxes","aashka","prepair","meilin","krea","dress","sivanna","etude","kadhi","laneige","gucci","jaclyn","hilary",
                     "anastasia","becca","sigma","farsali","majirel","satthwa","fenty","vibrator","focallure","krylon","tigi","armaf","cosrx","soumis","studiowest",
                     "evion","darmacol","odonil","comedogenic","suthol","suwasthi","kerastase","nexus","footwear","badescu","rebonding","jeffree","devacurl","odbo",
                     "sesderma","tilbury","dildo","glatt","essie","ethiglo","prada","dermadew","trylo","nycil","cipla","biore","giambattista","luxliss","parampara",
                     "dyson","episoft","vcare","ofra","nizoral","elnett","mediker","photostable","urbangabru","ketomac","popfeel","igora","wishtrend","jefree",
                     "hillary","skeyndor","raga","dresses","protectant","boroline","burts","seth","gelatin","panstick","goradia","policy","crimping","bajaj",
                     "spencer","glue","reloaded","dior","hoola","opticare","mario","bees","prepair","clothes","oxyglow","chapstick","stencil","smokers","clutcher",
                     "supra","artistry","cerave","suncross","suncare","stilla","skinfood","caprese","cantu","cameleon","glamego","hamdard","arish","soda","benzoyl",
                     "tape","catrice","liplove","clarina","glister","colorista","mistral","scholl","bathrobe","fastrack","christian","mauri","emolene","bioaqua",
                     "lodhradi","chance","bedhead","dawn","bake","persistence","bloating","carmex","roulette","acnestar","safi","flormar","margo","nudestix","instaglam"]

def create_missing_indices():
  indices = search_terms_daily.list_indexes()
  if 'query_1' not in [x['name'] for x in indices]:
    print("Creating missing index .. ")
    search_terms_daily.create_index([("query", pymongo.ASCENDING)])

create_missing_indices()

def getQuerySuggestion(query_id, query, algo):
    for term in corrected_search_query.find({"_id": query_id}):
        modified_query = term["suggested_query"]
        return modified_query

    if algo == 'default':
        es_query = """{
             "size": 1,
             "query": {
                 "match": {
                   "title_brand_category": "%s"
                 }
             }, 
             "suggest": 
              { 
                "text":"%s", 
                "term_suggester": 
                { 
                  "term": { 
                    "field": "title_brand_category"
                  }
                }
              }
        }""" % (query,query)
        es_result = Utils.makeESRequest(es_query, "livecore")
        doc_found = es_result['hits']['hits'][0]['_source']['title_brand_category'] if len(es_result['hits']['hits']) > 0 else ""
        doc_found = doc_found.lower()
        doc_found = doc_found.split()
        if es_result.get('suggest', {}).get('term_suggester'):
            modified_query = query.lower()
            for term_suggestion in es_result['suggest']['term_suggester']:
                if term_suggestion.get('text') in doc_found:
                    continue
                if term_suggestion.get('text') in correct_term_list:
                    continue
                if term_suggestion.get('options') and term_suggestion['options'][0]['score'] > 0.7:
                    frequency = term_suggestion['options'][0]['freq']
                    new_term = term_suggestion['options'][0]['text']
                    for suggestion in term_suggestion['options'][1:]:
                        if suggestion['freq'] > frequency and suggestion['score'] > 0.7:
                            frequency = suggestion['freq']
                            new_term = suggestion['text']
                    modified_query = modified_query.replace(term_suggestion['text'], new_term)
            return modified_query
        return query
    return query

def normalize(a):
    if not max(a) - min(a):
        return 0
    return (a - min(a)) / (max(a) - min(a))

def normalize_search_terms():


    date_buckets = [(0, 60), (61, 120), (121, 180), (181, 240)]
    dfs = []

    bucket_results = []
    for bucket_id, date_bucket in enumerate(date_buckets):
        startday = date_bucket[1] * -1 
        endday = date_bucket[0] * -1 
        startdate = arrow.now().replace(days=startday, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(tzinfo=None) 
        enddate = arrow.now().replace(days=endday, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(tzinfo=None)
        print(startdate, enddate) 
        bucket_results = []
        # TODO need to set count sum to count

        for term in search_terms_daily.aggregate([
            {"$match": {"date": {"$gte": startdate, "$lte": enddate}, "internal_search_term_conversion_instance": {"$gte": DAILY_THRESHOLD}}},
            {"$project": {"term": { "$toLower": "$term"}, "date":"$date", "count": "$internal_search_term_conversion_instance"}},
            {"$group": {"_id": "$term", "count": {"$sum": "$count"}}}, 
            {"$sort":{ "count": -1}},
        ], allowDiskUse=True):
            term['id'] = term.pop("_id")
            bucket_results.append(term)

        if not bucket_results:
            print("Skipping popularity computation")
            continue
        
        print("Computing popularity")
        df = pd.DataFrame(bucket_results) 
        df['norm_count'] = normalize(df['count'])

        df['popularity'] = (len(date_buckets) - bucket_id)*df['norm_count']
        dfs.append(df.loc[:, ['id', 'popularity']].set_index('id'))

    final_df = pd.DataFrame([])
    if dfs:
        final_df = dfs[0]
        for i in range(1, len(dfs)):
            final_df = pd.DataFrame.add(final_df, dfs[i], fill_value=0)
        final_df.popularity = final_df.popularity.fillna(0)

        final_df['popularity_recent'] = 100 * normalize(final_df['popularity'])
        final_df.drop(['popularity'], axis=1, inplace=True)
        #print(final_df)

    # Calculate total popularity
    print ("Calculating total popularity")
    date_6months_ago = arrow.now().replace(days=-6*30, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(tzinfo=None)

    bucket_results = []
    for term in search_terms_daily.aggregate([
        #{"$match": {"term" : {"$in": ['Lipstick', 'nars']}}},
        {"$match": {"internal_search_term_conversion_instance": {"$gte": DAILY_THRESHOLD}}},
        #{"$match": {"date": {"$gte": date_6months_ago, "$lte": enddate}, "internal_search_term_conversion_instance": {"$gte": DAILY_THRESHOLD}}},
        {"$project": {"term": { "$toLower": "$term"}, "date":"$date","count": "$internal_search_term_conversion_instance" }},
        {"$group": {"_id": "$term", "count": {"$sum": "$count"} }}, 
        {"$sort":{ "count": -1}},
    ], allowDiskUse=True):
        term['id'] = term.pop("_id")
        bucket_results.append(term)

    df = pd.DataFrame(bucket_results)
    df['norm_count'] = normalize(df['count'])
    df['popularity_total'] = df['norm_count']
    df = df.set_index('id')

    a = pd.merge(df, final_df, how='outer', left_index=True, right_index=True).reset_index()
    a.popularity_recent = a.popularity_recent.fillna(0) 
    a['popularity'] = 100 * normalize(0.7 * a['popularity_total'] + 0.3 * a['popularity_recent'])
    a.popularity = a.popularity.fillna(0) 

    requests = []
    corrections = []
    #brand_index = normalize_array("select brand as term from nykaa.brands where brand like 'l%'")
    #category_index = normalize_array("select name as term from nykaa.l3_categories")
    search_terms_normalized.remove({})
    cats_stemmed = set([ps.stem(x['name']) for x in Utils.mysql_read("select name from l3_categories ")])
    brands_stemmed = set([ps.stem(x['brand']) for x in Utils.mysql_read("select brand from brands")])
    cats_brands_stemmed = cats_stemmed | brands_stemmed

    for i, row in a.iterrows():
        query = row['id'].lower()
        if ps.stem(query) in cats_brands_stemmed:
            a.drop(i, inplace=True)
    
    a['popularity'] = 100 * normalize(a['popularity'])
    a = a.sort_values(by='popularity')
    for i, row in a.iterrows():
        query = row['id'].lower()
        if ps.stem(query) in cats_brands_stemmed:
          continue

        algo = 'default'
        query_id = re.sub('[^A-Za-z0-9]+', '_', row['id'].lower())

        try:
            suggested_query = getQuerySuggestion(query_id, query, algo)
            requests.append(UpdateOne({"_id":  query_id},
                                      {"$set": {"query": row['id'].lower(), 'popularity': row['popularity'], "suggested_query": suggested_query.lower()}}, upsert=True))
            corrections.append(UpdateOne({"_id":  query_id},
                                      {"$set": {"query": row['id'].lower(), "suggested_query": suggested_query.lower(), "algo": algo}}, upsert=True))
        except:
            print(traceback.format_exc())
            print(row)
        if i % 10000 == 0:
            search_terms_normalized.bulk_write(requests)
            corrected_search_query.bulk_write(corrections)
            requests = []
            corrections = []
    if requests:
        search_terms_normalized.bulk_write(requests)
        corrected_search_query.bulk_write(corrections)

"""
  current_month = arrow.now().format("YYYY-MM")
  res = search_terms_daily.aggregate(
    [
      #{"$limit" :1000},
      {"$match": {"month": {"$lt": current_month}, "count": {"$gt": 200 }}},
      {"$project": {"term": { "$toLower": "$term"}, "month":"$month", "count": "$count"}},
      {"$group": {"_id": "$term", "count": {"$sum": "$count"}}}, 
      {"$sort":{ "count": -1}},
      #{"$limit" :100},
    ])

  def normalize_term(term):
    term = term.lower()
    term = re.sub('[^A-Za-z0-9 ]', "", term) 
    term = re.sub("colour", 'color', term)
    return stem(term)
    
  def normalize_array(query):
    index = set()
    for row in Utils.mysql_read(query): 
      row = row['term']
      for term in row.split(" "):
        term = normalize_term(term)
        index.add(term)
    return index

  brand_index = normalize_array("select brand as term from nykaa.brands where brand like 'l%'")
  category_index = normalize_array("select name as term from nykaa.l3_categories")
  search_terms_normalized.remove({})

  first = True
  for row in res:
    if first:
      max_query_count = row['count']
      first = False
    popularity = row['count'] / max_query_count * 100
    if not row['_id']:
      continue
    terms_not_found = []
    for term in row['_id'].split(" "):
      term = normalize_term(term)
      if term in brand_index:
        #print("found %s in brand_index" % term)
        pass
      elif term in category_index:
        #print("found %s in category_index" % term)
        pass
      else:
        terms_not_found.append(term)

    if not terms_not_found:
      continue

    search_terms_normalized.update({"_id":  re.sub('[^A-Za-z0-9]+', '_', row['_id'].lower())}, {"query": row['_id'].lower(), "count": row['count'], 'popularity': popularity}, upsert=True)

  print("Terms not found: %s" % terms_not_found)
"""

if __name__ == "__main__":
  normalize_search_terms()
