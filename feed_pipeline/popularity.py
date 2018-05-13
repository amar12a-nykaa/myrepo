import argparse
import datetime
import json
import os
import os.path
import pprint
import re
import sys
import traceback
from collections import OrderedDict, defaultdict
from contextlib import closing

import arrow
from IPython import embed
import mysql.connector
import numpy
import omniture
import pandas as pd
from pymongo import MongoClient

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter

sys.path.append("/nykaa/scripts/feed_pipeline")
from popularity_api import get_popularity_for_id  

WEIGHT_VIEWS = 35
WEIGHT_UNITS = 35
WEIGHT_CART_ADDITIONS = 10
WEIGHT_REVENUE = 20

#WEIGHT_VIEWS = 10
#WEIGHT_UNITS = 10
#WEIGHT_CART_ADDITIONS = 10
#WEIGHT_REVENUE = 70

client = MongoClient("172.30.3.5")
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
  except ValueError:
    msg = "Not a valid date: '{0}'.".format(s)
    raise argparse.ArgumentTypeError(msg)


parser = argparse.ArgumentParser()
parser.add_argument("--startdate", help="startdate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
parser.add_argument("--enddate", help="enddate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace().format('YYYY-MM-DD'))
parser.add_argument("--preprocess", help="Only runs the report and prints.", action='store_true')
parser.add_argument("--popularity", help="Calculates popularity", action='store_true')
parser.add_argument("--post-to-solr", help="Posts popularity to Solr", action='store_true')
parser.add_argument("--dump-metrics", help="Dump metrics into a file", action='store_true')

parser.add_argument("--from-file", help="Read report from file", type=str)
parser.add_argument("--table", type=str, default='popularity')
parser.add_argument("--print-popularity-ids", type=str)
parser.add_argument("--debug", action='store_true')
parser.add_argument("--id", help='id to process. Only works with post-to-solr')
parser.add_argument("--platform", default='web,app')
parser.add_argument("--num-prods", '-n', help="obsolete", default=0, type=int)
parser.add_argument("--yes", '-y', help="obsolete", action='store_true')
argv = vars(parser.parse_args())


debug = argv['debug']
TABLE = argv['table']
startdatetime = arrow.get(argv['startdate']).datetime
enddatetime = arrow.get(argv['enddate']).datetime
print(startdatetime)
print(enddatetime)
platforms = argv["platform"].split(",")


if argv['dump_metrics']:
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


def preprocess_data():
  print("preprocess_data")
  print(argv)

  ctr = LoopCounter(name='Preprocessing')
  for product in raw_data.aggregate([
      {"$match": {"date": {"$gte": startdatetime, "$lte": enddatetime}}}, 
      {"$group": {
          "_id": {"date": "$date", "parent_id": "$parent_id"},
          "views": {"$sum": "$views"}, 
          "cart_additions": {"$sum": "$cart_additions"}, 
          "orders": {"$sum": "$orders"} ,
          "revenue": {"$sum": "$revenue"},
          "units": {"$sum": "$units"},
      }}
      ], allowDiskUse=True):
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    p = product
    p['parent_id'] = p['_id'].get('parent_id')
    p['date'] = p['_id']['date']
    p.pop("_id")
    if not p['parent_id']:
      continue
    try:
      processed_data.update({ "date": p['date'], "parent_id": p['parent_id']}, p, upsert=True)
    except:
      print("[ERROR] processed_data.update error %s " % p)
      raise

def normalize(a):
  return (a-min(a))/(max(a)-min(a))

def calculate_popularity():
  timestamp = arrow.now().datetime
  results = []
  ctr = LoopCounter(name='Popularity: ')

  date_buckets = [(0,60), (61, 120), (121, 180)]
  dfs = []
  for bucket_id, date_bucket in enumerate(date_buckets):
    startday = date_bucket[1] * -1
    endday = date_bucket[0] * -1
    startdate = arrow.now().replace(days=startday, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(tzinfo=None)
    enddate = arrow.now().replace(days=endday, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(tzinfo=None)

    bucket_results = []
    for p in processed_data.aggregate([
        {"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
        {"$group": {"_id": "$parent_id", 
          "views": {"$sum": "$views"}, 
          "cart_additions": {"$sum": "$cart_additions"}, 
          "orders": {"$sum": "$orders"},
          "revenue": {"$sum": "$revenue"},
          "units": {"$sum": "$units"},
        }},\
      ]):
      p['parent_id'] = p.pop("_id")
      bucket_results.append(p)

    if not bucket_results:
      print("Skipping :", date_bucket)
    else:
      print("Processing:", date_bucket)
      df = pd.DataFrame(bucket_results)
      df['Vn'] = normalize(df['views'])
      df['Cn'] = normalize(df['cart_additions'])
      df['On'] = normalize(df['orders'])
      df['Rn'] = normalize(df['revenue'])
      df['Un'] = normalize(df['units'])

      df['popularity'] = (len(date_buckets) - bucket_id) *\
        normalize(numpy.log(1 + WEIGHT_VIEWS * df['Vn'] + WEIGHT_UNITS * df['Un'] + WEIGHT_CART_ADDITIONS * df['Cn'] + WEIGHT_REVENUE * df['Rn'])) * 100
      dfs.append(df.loc[:, ['parent_id', 'popularity']].set_index('parent_id'))
        
  if argv['print_popularity_ids']:
    ids = [x.strip() for x in argv['print_popularity_ids'].split(",") if x]
    for _id in ids: 
      for i, df in enumerate(dfs):
        try:
          print("popularity per month:", date_buckets[i], _id, dfs[i].loc[_id])
        except:
          pass

  final_df = dfs[0] 
  
  for i in range(1, len(dfs)):
    final_df = pd.DataFrame.add(final_df, dfs[i], fill_value=0)
  final_df.popularity = final_df.popularity.fillna(0)

  final_df['popularity_recent'] = 100 * normalize(final_df['popularity'])
  final_df.drop(['popularity'], axis = 1, inplace = True)

  # Calculate total popularity
  for p in processed_data.aggregate(
      [{"$group": {
        "_id": "$parent_id", 
        "views": {"$sum": "$views"}, 
        "cart_additions": {"$sum": "$cart_additions"}, 
        "orders": {"$sum": "$orders"},
        "revenue": {"$sum": "$revenue"},
        "units": {"$sum": "$units"},
        }},\
      ]):
    p['parent_id'] = p.pop("_id")
    results.append(p)

  df = pd.DataFrame(results)
  df['Vn'] = normalize(df['views'])
  df['Cn'] = normalize(df['cart_additions'])
  df['On'] = normalize(df['orders'])
  df['Rn'] = normalize(df['revenue'])
  df['Un'] = normalize(df['units'])
  df['popularity_total'] = normalize(numpy.log(1 + WEIGHT_VIEWS * df['Vn'] + WEIGHT_UNITS * df['Un'] + WEIGHT_CART_ADDITIONS * df['Cn'] + WEIGHT_REVENUE * df['Rn'])) * 100
  df = df.set_index("parent_id") 

  a = pd.merge(df, final_df, how='outer', left_index=True, right_index=True).reset_index()
  a.popularity_recent = a.popularity_recent.fillna(0)
  a['popularity'] = 100 * normalize(0.7 * a['popularity_total'] + 0.3 * a['popularity_recent'])
  a.popularity= a.popularity.fillna(0)

  ctr = LoopCounter(name='Writing popularity to db', total = len(a.index))
  for i, row in a.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    row = dict(row)
    #row = {k:v for k,v in row.items() if k in ['cart_additions', 'last_calculated', 'orders', 'parent_id', 'popularity', 'revenue', 'units', 'views']}
    row['last_calculated'] = timestamp

    if row.get('parent_id'):
      popularity_table.replace_one({"_id": row['parent_id'], "parent_id": row['parent_id']}, row, upsert=True)

  popularity_table.remove({"last_calculated": {"$ne": timestamp}})

class SolrPostManager:
  size = 0
  BATCH_SIZE = 10
  docs = []
  ids = []
  id_object = {}

  def post_to_solr(self, product_id, obj):
    self.id_object[product_id] = obj 
    if product_id and 'unspecified' not in product_id:
      self.ids.append(product_id)

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
      _id = doc['product_id']
      doc = {k: v for k,v in doc.items() if k in required_fields}
      doc.update({k:{"set": v} for k,v in self.id_object[_id].items()})

      final_docs.append(doc)

    #print("flushing... ")
    try:
      response = Utils.updateCatalog(final_docs)
      #print(response)
    except:
      print(traceback.format_exc())
      print("[ERROR] Could not post to solr following ids: %s" % [x['product_id'] for x in final_docs])
    self.ids = []
    

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
  query = {}
  if argv['id']:
    query = {"_id": argv['id']}
    print("query: %s" % query)
  ctr = LoopCounter(name='Post Popularity data to Solr: ', total = popularity_table.count())
  for p in popularity_table.find(query,no_cursor_timeout=True):
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    obj = {
      "popularity":p.get('popularity_total_recent', 0),
    }
    post_mgr.post_to_solr(product_id=p['product_id'], obj=obj)
  post_mgr.flush()
