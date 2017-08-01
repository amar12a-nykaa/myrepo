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
import IPython
import mysql.connector
import numpy
import omniture
import pandas as pd
from pymongo import MongoClient
from stemming.porter2 import stem

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

#sys.path.append("/nykaa/scripts/utils")
#from omnitureutils import OmnitureUtils

client = MongoClient()
search_terms = client['search']['search_terms']
search_terms_normalized = client['search']['search_terms_normalized']

current_month = arrow.now().format("YYYY-MM")
res = search_terms.aggregate(
  [
    #{"$limit" :1000},
    {"$match": {"month": {"$lt": current_month}, "count": {"$gt": 200 }}},
    {"$project": {"term": { "$toLower": "$term"}, "month":"$month", "count": "$count"}},
    {"$group": {"_id": "$term", "count": {"$sum": "$count"}}}, 
    {"$sort":{ "count": -1}},
    #{"$limit" :100},
  ])

#print(res)
#max_query_count = res[0]['count']
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

#
#  terms = [normalize_term(x['term']) for x in Utils.mysql_read(query)]
#  return set(terms)
#  terms_index = {k:1 for k in terms}
#  return terms_index


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
      print("found %s in brand_index" % term)
      pass
    elif term in category_index:
      print("found %s in category_index" % term)
      pass
    else:
      terms_not_found.append(term)

#  if row['_id'] == 'loreal hair colour':
#    IPython.embed()
  if not terms_not_found:
    continue

  search_terms_normalized.update({"_id":  re.sub('[^A-Za-z0-9]+', '_', row['_id'].lower())}, {"query": row['_id'].lower(), "count": row['count'], 'popularity': popularity}, upsert=True)

