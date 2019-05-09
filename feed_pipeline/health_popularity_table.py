import json
import socket
import sys
import traceback
import urllib.parse
import urllib.request
from datetime import datetime

import elasticsearch
import requests
from IPython import embed
from elasticsearch import helpers, Elasticsearch


sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils

sys.path.append('/home/apis/pds_api/')
from pas.v2.utils import CATALOG_COLLECTION_ALIAS, MemcacheUtils, Utils
client = MongoUtils.getClient()
popularity_table = client['search']['popularity']

count =0 
for row in Utils.mysql_read("select * from products where type ='configurable' and disabled = 0  and product_id is not null"):
  product_id = row['product_id']
  _type = row['type']
  if not popularity_table.find_one({"_id": product_id}):
    print("Missing %12s: %s" % (_type, product_id ))
    count += 1


print("Number of missing configurable products: %s" % count)

