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
from collections import defaultdict 

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
from elasticsearch import helpers, Elasticsearch

sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter
from esutils import EsUtils
from apiutils import ApiUtils
from idutils import createId

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils, MemcacheUtils


collection='autocomplete'
search_terms_normalized_daily = MongoUtils.getClient()['search']['search_terms_normalized_daily']
query_product_map_table = MongoUtils.getClient()['search']['query_product_map']
query_product_not_found_table = MongoUtils.getClient()['search']['query_product_not_found']
top_queries = []
es = Utils.esConn()
es_index = EsUtils.get_active_inactive_indexes('livecore')['active_index']

singles = defaultdict(int)
doubles = defaultdict(int)

def w_shingle(string, w):
  """Return the set of contiguous sequences (shingles) of `w` words
  in `string`."""
  words = string.split()
  num_words = len(words)

  # Confirm that 0 < `w` <= `num_words`
  if w > num_words or w == 0:
      raise Exception('Invalid argument -w')

  # If w is equal to the number of words in the input string, the
  # only item in the set is `words`.
  return [("".join(words[i:i + w]), words[i:i + w]) for i in range(len(words) - w + 1)]


products = helpers.scan(es,
    query={"query": {"match_all": {}}, "_source": "title", "size": 1000},
    index=es_index,
    doc_type="product"
)

for i, prod in enumerate(products): 
  title = prod['_source']['title']
  title = re.sub("[^a-zA-Z0-9 ]", " ", title)    
  title = re.sub(" +", " ", title)    
  title = title.lower()
  shingles_tuple = w_shingle(title, 2)

  for w, content in shingles_tuple:
    doubles[w] = content
  for w in title.split(" "):
    singles[w] += 1


S = set(list(singles.keys()))
D = set(list(doubles.keys()))
matches = S & D
matches = sorted(list(matches), key=lambda x:doubles[x] )
with open("/tmp/reverse_shingles.txt", "w") as f:
  for match in matches:
    f.write(match + " -> " + " ".join(doubles[match]) +"  ... " +str(singles[match]) +  "\n")
