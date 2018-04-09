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

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

import requests
import IPython

client = MongoClient()
raw_data = client['search']['raw_data']
processed_data = client['search']['processed_data']
popularity_table = client['search']['popularity']


for p in Utils.mysql_read("select product_id, mrp, sku from products limit 100"):
  if not p['product_id']:
    continue
  print(p)
  r = requests.get("http://172.30.1.69/apis/v1/pas.get?sku=%s&type=simple" % p['sku'])
  print(r.text)

  #IPython.embed()
  #sys.exit()

