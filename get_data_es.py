import os
from collections import defaultdict
import sys
import time
import json
import psutil
import argparse
import operator
import csv

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

es_conn = DiscUtils.esConn()
ES_BATCH_SIZE = 10000
scroll_id = None
luxe_products = []
product_2_mrp = {}
dict_ES = []
while True:
    if not scroll_id:
        query = {
            "size": 10000,
            "query": { "match_all": {} },
            "_source": ["sku","mrp","type", "quantity"]
        }
        response = es_conn.search(index='livecore', body=query, scroll='1m')
    else:
        response = es_conn.scroll(scroll_id=scroll_id, scroll='1m')

    if not response['hits']['hits']:
        break
    scroll_id = response['_scroll_id']
    print("Processing " + str(len(response['hits']['hits'])) + " products products")
    for p in response['hits']['hits']:
      dict_ES.append(p['_source'])
    break

keys = dict_ES[0].keys()
with open('dataES1.csv', 'w') as output_file:
  dict_writer = csv.DictWriter(output_file, keys)
  #dict_writer.writeheader()
  dict_writer.writerows(dict_ES)


