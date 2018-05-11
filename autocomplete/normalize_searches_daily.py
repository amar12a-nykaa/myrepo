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
#from loopcounter import LoopCounter
from IPython import embed


import arrow
import mysql.connector
import numpy
import omniture
import pandas as pd
from pymongo import MongoClient, UpdateOne
from stemming.porter2 import stem

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

def create_missing_indices():
  indices = search_terms_daily.list_indexes()
  if 'query_1' not in [x['name'] for x in indices]:
    search_terms_daily.create_index([("query", pymongo.ASCENDING)])

create_missing_indices()

def normalize(a):
    if not max(a) - min(a):
        return 0
    return (a - min(a)) / (max(a) - min(a))

def normalize_search_terms():

    client = MongoClient("172.30.3.5")
    search_terms_daily = client['search']['search_terms_daily']
    search_terms_normalized = client['search']['search_terms_normalized_daily']

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
            {"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
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

    bucket_results = []
    for term in search_terms_daily.aggregate([
        #{"$match": {"term" : {"$in": ['Lipstick', 'nars']}}},
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

    #brand_index = normalize_array("select brand as term from nykaa.brands where brand like 'l%'")
    #category_index = normalize_array("select name as term from nykaa.l3_categories")
    search_terms_normalized.remove({})

    for i, row in a.iterrows():
        try:
            requests.append(UpdateOne({"_id":  re.sub('[^A-Za-z0-9]+', '_', row['id'].lower())}, {"$set": {"query": row['id'].lower(), 'popularity': row['popularity']}}, upsert=True))
        except:
            print(traceback.format_exc())
            print(row)
        if i % 1000000 == 0:
            search_terms_normalized.bulk_write(requests)
            requests = []
#        search_terms_normalized.update({"_id":  re.sub('[^A-Za-z0-9]+', '_', row['id'].lower())}, {"query": row['id'].lower(), 'popularity': row['popularity']}, upsert=True)
    if requests:
        search_terms_normalized.bulk_write(requests)

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
