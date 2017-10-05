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
from pas.v1.utils import Utils

sys.path.append("/nykaa/scripts/sharedutils")
from omnitureutils import OmnitureUtils

parser = argparse.ArgumentParser()
parser.add_argument("--startdate", help="startdate in YYYYMMDD format or number of days to add from today i.e -4", type=OmnitureUtils.valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
parser.add_argument("--enddate", help="enddate in YYYYMMDD format or number of days to add from today i.e -4", type=OmnitureUtils.valid_date, default=arrow.now().replace().format('YYYY-MM-DD'))

argv = vars(parser.parse_args())


startdatetime = arrow.get(argv['startdate']).datetime
enddatetime = arrow.get(argv['enddate']).datetime


res = Utils.mysql_write("delete from  products where sku = '0-47469-00509-2'")
#print(res)
#sys.exit()

suites = {}
suites['mobile']  = OmnitureUtils.get_suite_mobile()
suites['web']  = OmnitureUtils.get_suite_web()

#Dimension: Internal search term conversion
#metrics: internal search term conversion instances

platform = 'mobile'

def fetch_data():

  print("Running report .. ")
#  total_rows = argv['num_prods'] or 0
#  if not total_rows:
#    top = 50000
#  else:
#    top = min(total_rows, 50000)
  total_rows = 10000
  top = 10000

  for platform in ['mobile']: 
    print("== %s ==" % platform) 
    startingWith = 0  
    report_cnt = 1 
    while(True): 
      print("== report  %d ==" % report_cnt) 
      report_cnt += 1 
      MAX_ATTEMPTS = 3  
      for attempt in range(0,MAX_ATTEMPTS): 
        print("Attempt %r to fetch omniture data" % attempt) 
        try: 
          report = suites[platform].report \
            .metric('instances') \
            .element('evar6', top=top, startingWith=0)\
            .range(argv['startdate'], argv['enddate'])\
            .granularity("month")\
            .run()
        except: 
          print("[ERROR] Attempt %r to fetch omniture data failed!" % attempt) 
          print(traceback.format_exc()) 
          pass 
        else: 
          break 
 
      print(" --- ") 
      data = report.data 
      for product in data: 
        #print(product)
        if 'datetime' in product: 
          product['month'] = datetime.datetime.strftime(product['datetime'], '%Y-%m') 
          product[platform] = platform 
        yield product 
 
      if len(data) < top or (total_rows and top * report_cnt > total_rows): 
        break 
      startingWith += top 

client = MongoClient()
search_terms = client['search']['search_terms'] 
for prod in fetch_data():
  #print(prod)
  prod = {k:v for k,v in prod.items() if k in ['month', 'instances', 'evar6']}
  prod['count'] = prod.pop('instances')
  prod['term'] = prod.pop('evar6')
  search_terms.update({'month':prod['month'], 'term': prod['term']}, {"$set": prod}, upsert=True)


#create table search_queries(platform varchar(20), query varchar(100)); 

#conn = Utils.mysqlConnection()

