import os
from collections import defaultdict
import sys
import time
import json
import psutil
import argparse
import operator
sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

es_conn = Utils.esConn()
ES_BATCH_SIZE = 10000
scroll_id = None
luxe_products = []
product_2_mrp = {}
while True:
    if not scroll_id:
        query = {
            "size": ES_BATCH_SIZE,
            "query": { "match_all": {} },
            "_source": ["product_id", "is_luxe", "mrp"]
        }
        response = es_conn.search(index='livecore', body=query, scroll='1m')
    else:
        response = es_conn.scroll(scroll_id=scroll_id, scroll='1m')

    if not response['hits']['hits']:
        break
    scroll_id = response['_scroll_id']
    print("Processing " + str(len(response['hits']['hits'])) + " products products")
    luxe_products += [int(p['_source']['product_id']) for p in response['hits']['hits'] if p["_source"]["is_luxe"]]
    product_2_mrp.update({int(p["_source"]["product_id"]): p["_source"]["mrp"] for p in response["hits"]["hits"]})

with open("luxe.json", "r+") as f:
    json.dump(luxe_products, f, indent=4)

with open("product_2_mrp.json", "r+") as f:
    json.dump(product_2_mrp, f, indent=4)
