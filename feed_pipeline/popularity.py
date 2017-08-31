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

sys.path.append("/nykaa/scripts/utils")
from loopcounter import LoopCounter

embed = IPython.embed

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
print(startdatetime)
print(enddatetime)
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
    f.write("%s" % suites['mobile'])
  f1 = filename

  filename = os.path.join(dir_path, "metrics.txt")
  with open(filename, 'w') as f:
    f.write("%s" % suites['mobile'].metrics)
  f2 = filename

  filename = os.path.join(dir_path, "elements.txt")
  with open(filename, 'w') as f:
    f.write("%s" % suites['mobile'].elements)
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
      MAX_ATTEMPTS = 3 
      for attempt in range(0,MAX_ATTEMPTS):
        print("Attempt %r to fetch omniture data" % attempt)
        try:
          report = suites[platform].report \
              .metric('event5') \
              .metric('cartadditions') \
              .metric('orders') \
              .element('product', top=top, startingWith=startingWith)\
              .element('category')\
              .range(argv['startdate'], argv['enddate'])\
              .granularity("day")\
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
      #"platform": product['platform']
    }

    raw_data.update({"date": d['date'], 'productid': d['productid'], 
      #'platform': d['platform']
      }, d, upsert=True)

def preprocess_data():
  print("preprocess_data")
  print(argv)
  total = raw_data.count({"date": {"$gte": startdatetime, "$lte": enddatetime}})

  ctr = LoopCounter(name='Preprocessing: ', total = total)
  for product in raw_data.find({"date": {"$gte": startdatetime, "$lte": enddatetime}}, no_cursor_timeout=True):
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    p = product
    processed_data.update({"date": p['date'], "productid": p['productid']}, p, upsert=True)

def normalize(a):
  return (a-min(a))/(max(a)-min(a))

def calculate_popularity():
  results = []
  ctr = LoopCounter(name='Popularity: ')
  for p in processed_data.aggregate([{"$group": {"_id": "$productid" , "views": {"$sum": "$views"}, "cart_additions": {"$sum": "$cart_additions"}, "orders": {"$sum": "$orders"}}},\
      #{"$limit": 10},\
      ]):
    p['productid'] = p.pop("_id")
    results.append(p)

  df = pd.DataFrame(results)

  df['Vn'] = normalize(df['views'])
  df['Cn'] = normalize(df['cart_additions'])
  df['On'] = normalize(df['orders'])
  df['popularity'] = normalize(numpy.log(1 + df['Vn'] + 2*df['Cn'] + 3*df['On'])) * 100

  #print(df)
  for i, row in df.iterrows():
    row = dict(row)
    popularity_table.update({"_id": row['productid'], "productid": row['productid']}, row, upsert=True)
  sys.exit()


class SolrPostManager:
  size = 0
  BATCH_SIZE = 200
  docs = []
  ids = []
  id_popularity = {}

  def post_to_solr(self, productid, popularity):
    self.id_popularity[productid] = popularity
    if productid and 'unspecified' not in productid:
      self.ids.append(productid)

    if len(self.ids) > self.BATCH_SIZE:
      self.flush()
    return


  def flush(self ):
    ids = self.ids

    required_fields = ['sku', 'product_id']
    params = {}
    params['q'] = " OR ".join(["product_id:%s" %x for x in ids] )
    params['fl'] = ",".join(required_fields)

    response = Utils.makeSolrRequest(params)
    docs = response['docs']
    final_docs = []
    for i, doc in enumerate(docs):
      doc['popularity'] = self.id_popularity[doc['product_id']]
      doc = {k: v for k,v in doc.items() if k in required_fields}
      doc.update({"popularity":  {"set": self.id_popularity[doc['product_id']]}})
      final_docs.append(doc)

    print("flushing... ")
    try:
      response = Utils.updateCatalog(final_docs)
    except:
      print("[ERROR] Could not post to solr following ids: %s" % [x['product_id'] for x in final_docs])
    self.ids = []
    

def post_to_solr(productid, popularity):
  params = {}
  params['q'] = "product_id:%s" % productid
  try:
    response = Utils.makeSolrRequest(params)
    if not response['docs']:
      print("productid not found: %s" % productid)
      return
    doc = response['docs'][0]
    required_fields = ["create_time", "discount", "in_stock", "mrp", "popularity", "price", "product_id", "psku", "sku", "title", "type", "update_time", "visibility", 'product_id']
    doc = {k: v for k,v in doc.items() if k in required_fields}

    doc.update({"popularity":  {"set": popularity}})
    response = Utils.updateCatalog([doc])
    print(response)
  except:
    print("[ERROR] Could not update this product on Solr: %s" % productid)
if argv['fetch_omniture']:
  print("fetch_omniture start: %s" % arrow.now())
  write_report_data_to_db()
  print("fetch_omniture end: %s" % arrow.now())

if argv['preprocess']:
  print("preprocess start: %s" % arrow.now())
  preprocess_data()
  print("preprocess end: %s" % arrow.now())

if argv['popularity']:
  print("popularity start: %s" % arrow.now())
  calculate_popularity()
  print("popularity end: %s" % arrow.now())

if argv['post_to_solr']:
  post_mgr = SolrPostManager()
  for p in popularity_table.find(no_cursor_timeout=True):
    post_mgr.post_to_solr(productid=p['productid'], popularity=p['popularity'])
  post_mgr.flush()
