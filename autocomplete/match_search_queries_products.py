import csv 
import editdistance
import argparse
import datetime
import json
import os
import os.path
import pprint
import re
import sys
import traceback

import arrow
import IPython
import mysql.connector
import numpy
import omniture
import pandas as pd
import requests
from furl import furl
from IPython import embed
from pymongo import MongoClient
from stemming.porter2 import stem

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

#sys.path.append("/nykaa/scripts/utils")
#from omnitureutils import OmnitureUtils

def write_dict_to_csv(dictname, filename):
	with open(filename, 'w') as csv_file:
			writer = csv.writer(csv_file)
			for key, value in dictname.items():
				 writer.writerow([key, value])

client = MongoClient()
search_terms = client['search']['search_terms']
search_terms_normalized = client['search']['search_terms_normalized']

map_search_product = {}

for query in [p['query'] for p in search_terms_normalized.find()]:
  #print(query)
  base_url = "http://localhost:8983/solr/yang/select"
  f = furl(base_url) 
  f.args['defType'] = 'dismax'
  f.args['indent'] = 'on'
  f.args['mm'] = 80
  f.args['qf'] = 'title_text_split'
  f.args['type'] = 'simple'
  f.args['bf'] = 'popularity'
  f.args['price'] = '[1 TO *]'
  f.args['q'] = str(query)
  f.args['fl'] = 'title,score'
  f.args['wt'] = 'json'
  resp = requests.get(f.url)
  js = resp.json()
  docs = js['response']['docs']
  if docs:
    max_score = max([x['score'] for x in docs])
    docs = [x for x in docs if x['score'] == max_score]
    for doc in docs:
      #print(doc)
      doc['editdistance'] = editdistance.eval(doc['title'], query) 
    docs.sort(key=lambda x:x['editdistance'] )

    map_search_product[query] = docs[0]['title']

  #print(docs)
  #embed()
  #exit()
#pprint.pprint(map_search_product)
write_dict_to_csv(map_search_product, '/tmp/map_search_product.csv')



  
