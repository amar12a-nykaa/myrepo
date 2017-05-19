import argparse
import datetime
import json
import os
import os.path
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

import urllib
from urllib import parse

embed = IPython.embed

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

results = Utils.fetchResults(Utils.mysqlConnection("r"), "select sku from products where disabled = 0 LIMIT 20000", None)
with open("/tmp/solr_urls.py", 'w') as f:
  for result in results:
    sku = urllib.parse.quote_plus(result['sku'])
    f.write("http://internal-SPSAPITargetGroup-internal-1197013483.ap-southeast-1.elb.amazonaws.com/apis/v1/product.list?sku=%s&variants=true\n" % sku)




