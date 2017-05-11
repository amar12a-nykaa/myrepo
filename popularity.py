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

embed = IPython.embed

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

client = MongoClient()
raw_data = client['search']['raw_data']
processed_data = client['search']['processed_data']
popularity_table = client['search']['popularity']

def valid_date(s):
  try:
    if re.search("^-?[0-9]+$", s):
      adddays = int(s)
      assert abs(adddays) < 500, "Reports can be fetched only 500 days in past." 
      now = arrow.utcnow()
      return now.replace(days=adddays).format('YYYY-MM-DD')
    else:
      return arrow.get(s, 'YYYY-MM-DD').format('YYYY-MM-DD')
      #return datetime.datetime.strptime(s, "%Y-%m-%d")
  except ValueError:
    msg = "Not a valid date: '{0}'.".format(s)
    raise argparse.ArgumentTypeError(msg)


parser = argparse.ArgumentParser()
parser.add_argument("--num-prods", '-n', required=True, help="Number of products to be fetched from omniture. Pass 0 for fetching all.", default=0, type=int)
parser.add_argument("--yes", '-y', help="Pass this to avoid prompt and run full report", action='store_true')
parser.add_argument("--startdate", help="startdate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
parser.add_argument("--enddate", help="enddate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace().format('YYYY-MM-DD'))
parser.add_argument("--fetch-omniture", help="Only runs the report and prints.", action='store_true')
parser.add_argument("--preprocess", help="Only runs the report and prints.", action='store_true')
parser.add_argument("--popularity", help="Calculates popularity", action='store_true')
parser.add_argument("--post-to-solr", help="Posts popularity to Solr", action='store_true')
parser.add_argument("--dump-metrics", help="Dump metrics into a file", action='store_true')

parser.add_argument("--from-file", help="Read report from file", type=str)
parser.add_argument("--table", type=str, default='popularity')
parser.add_argument("--debug", action='store_true')
argv = vars(parser.parse_args())

debug = argv['debug']
TABLE = argv['table']
startdatetime = arrow.get(argv['startdate']).datetime
enddatetime = arrow.get(argv['enddate']).datetime

if argv['num_prods'] == 0 and not argv['yes']:
  response = input("Are you sure you want to run full report? [Y/n]")
  if response == 'Y':
    print("Running full report .. ")
  else:
    print("Exiting")
    sys.exit()

if argv['num_prods'] and argv['fetch_omniture']== 0:
  print("=== Running full report. All data will be flushed. === ")

if argv['fetch_omniture'] or argv['dump_metrics']:
  analytics = omniture.authenticate('soumen.seth:FSN E-Commerce', '770f388b78d019017d5e8bd7a63883fb')
  suites = {}
  suites['web'] = analytics.suites['fsnecommerceprod']
  suites['mobile'] = analytics.suites['fsnecommercemobileappprod']

if argv['dump_metrics']:
  dir_path = os.path.dirname(os.path.realpath(__file__))
  print("Dumping metrics in files:" )
  filename = os.path.join(dir_path, "analytics.suites.txt")
  with open(filename, 'w') as f:
    f.write("%s" % suite)
  f1 = filename

  filename = os.path.join(dir_path, "metrics.txt")
  with open(filename, 'w') as f:
    f.write("%s" % suite.metrics)
  f2 = filename

  filename = os.path.join(dir_path, "elements.txt")
  with open(filename, 'w') as f:
    f.write("%s" % suite.elements)
  f3 = filename

  print('{f1}\n{f2}\n{f3}'.format(f1=f1,f2=f2, f3=f3))

  sys.exit()


def fetch_data():
  if argv['from_file']:
    with open(argv['from_file'],  'r') as f:
      for line in f:
        print(line)
        yield json.loads(line)
    return

  print("Running report .. ")
  total_rows = argv['num_prods'] or 0
  if not total_rows:
    top = 50000
  else:
    top = min(total_rows, 50000)

  for platform in ['web', 'mobile']:
    print("== %s ==" % platform)
    startingWith = 0 
    report_cnt = 1
    while(True):
      print("== report  %d ==" % report_cnt)
      report_cnt += 1
      report = suites[platform].report \
          .metric('event5') \
          .metric('cartadditions') \
          .metric('orders') \
          .element('product', top=top, startingWith=startingWith)\
          .element('category')\
          .range(argv['startdate'], argv['enddate'])\
          .granularity("day")\
          .run()
      print(" --- ")
      data = report.data
      for product in data:
        if 'datetime' in product:
          product['date'] = datetime.datetime.strftime(product['datetime'], '%Y-%m-%d')
          product[platform] = platform
        yield product

      if len(data) < top or (total_rows and top * report_cnt > total_rows):
        break
      startingWith += top


def write_report_data_to_db():

  for product in fetch_data():
    d = {
      "views": product['event5'],
      "cart_additions": product['cartadditions'],
      "orders": product['orders'],
      "productid": product['product'],
      "date" : product['datetime'],
      "platform": product['platform']
    }

    raw_data.update({"date": d['date'], 'productid': d['productid'], 'platform': d['platform']}, d, upsert=True)

def preprocess_data():
  print("preprocess_data")
  print(argv)
  for product in raw_data.find({"date": {"$gt": startdatetime, "$lt": enddatetime}}):
    print(product)
#TODO preprocessing 
    p = product
    processed_data.update({"date": p['date'], "productid": p['productid'], 'platform': d['platform']}, p, upsert=True)

def normalize(a):
  return (a-min(a))/(max(a)-min(a))

def calculate_popularity():
  results = []
  for p in processed_data.aggregate([{"$group": {"_id": "$productid" , "views": {"$sum": "$views"}, "cart_additions": {"$sum": "$cart_additions"}, "orders": {"$sum": "$orders"}}},\
      {"$limit": 10},\
      ]):
    p['productid'] = p.pop("_id")
    results.append(p)

  df = pd.DataFrame(results)

  df['Vn'] = normalize(df['views'])
  df['Cn'] = normalize(df['cart_additions'])
  df['On'] = normalize(df['orders'])
  df['popularity'] = normalize(numpy.log(1 + df['Vn'] + 2*df['Cn'] + 3*df['On'])) * 100

  print(df)
  for i, row in df.iterrows():
    row = dict(row)
    popularity_table.update({"_id": row['productid'], "productid": row['productid']}, row, upsert=True)
  sys.exit()

def post_to_solr():
  #qwerty
  #asdf
  #zxcv
  #qwer

if argv['fetch_omniture']:
  write_report_data_to_db()

if argv['preprocess']:
  preprocess_data()

if argv['popularity']:
  calculate_popularity()

if argv['post_to_solr']:
  post_to_solr()
