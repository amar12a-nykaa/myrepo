import argparse
import datetime
import json
import os
import os.path
import pprint
import re
import requests
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
from pandas.io import sql
from pymongo import MongoClient
from pipelineUtils import PipelineUtils
sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter

sys.path.append("/nykaa/scripts/feed_pipeline")
from popularity_api import get_popularity_for_id  

WEIGHT_VIEWS = 20
WEIGHT_UNITS = 40
WEIGHT_CART_ADDITIONS = 10
WEIGHT_REVENUE = 30
WEIGHT_UNITS_BY_VIEWS = 20 
#WEIGHT_VIEWS = 10
#WEIGHT_UNITS = 10
#WEIGHT_CART_ADDITIONS = 10
#WEIGHT_REVENUE = 70

client = Utils.mongoClient()
raw_data = client['search']['raw_data']
processed_data = client['search']['processed_data']
popularity_table = client['search']['popularity']

product_sales_map = {}
parent_child_distribution_map = {}

def build_product_sales_map(startdate='2018-04-01'):
  global product_sales_map
  
  where_clause = "orderdetail_dt_created >='{0}'".format(startdate)
  query =  """select product_id, sum(OrderDetail_QTY) as qty_ordered from fact_order_detail_new  where {0}  group by product_id""".format(where_clause)
  print (query)
  redshift_conn =  Utils.redshiftConnection()
  cur  =  redshift_conn.cursor()

  cur.execute(query)

  for row in cur.fetchall():
    product_sales_map[str(row[0])] = float(row[1])

def build_parent_child_distribution_map():
  global parent_child_distribution_map

  query = """select parent_product_id, product_id, count(distinct nykaa_orderno) as orders from fact_order_detail_new
              where sku_type = 'CONFIG' and orderdetail_dt_created >= (CURRENT_DATE - 60) and product_id is not null 
              group by 1,2;"""
  print(query)
  redshift_conn = Utils.redshiftConnection()
  data = pd.read_sql(query,con=redshift_conn)
  parent_data = data.groupby('parent_product_id', as_index=False)['orders'].sum()
  parent_data.rename(columns={'orders': 'total_order'}, inplace=True)
  data = pd.merge(data, parent_data, on='parent_product_id', how='inner')
  data[['product_id']] = data[['product_id']].astype(int)

  for index, row in data.iterrows():
    parent_id  = str(row['parent_product_id'])
    product_id = str(row['product_id'])
    if parent_id not in parent_child_distribution_map:
      parent_child_distribution_map[parent_id] = {}
    parent_child_distribution_map[parent_id][product_id] = float(row['orders'])/row['total_order']

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
parser.add_argument("--back_date_90_days", help="90 days back date in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace(days=-90).format('YYYY-MM-DD'))
parser.add_argument("--startdate", help="startdate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
parser.add_argument("--enddate", help="enddate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace().format('YYYY-MM-DD'))
parser.add_argument("--preprocess", help="Only runs the report and prints.", action='store_true')
parser.add_argument("--popularity", help="Calculates popularity", action='store_true')
parser.add_argument("--dump-metrics", help="Dump metrics into a file", action='store_true')

parser.add_argument("--from-file", help="Read report from file", type=str)
parser.add_argument("--table", type=str, default='popularity')
parser.add_argument("--print-popularity-ids", type=str)
parser.add_argument("--debug", action='store_true')
parser.add_argument("--platform", default='web,app')
parser.add_argument("--num-prods", '-n', help="obsolete", default=0, type=int)
parser.add_argument("--yes", '-y', help="obsolete", action='store_true')
argv = vars(parser.parse_args())


debug = argv['debug']
TABLE = argv['table']
back_date_90_days_time = arrow.get(argv['back_date_90_days']).datetime
startdatetime = arrow.get(argv['startdate']).datetime
enddatetime = arrow.get(argv['enddate']).datetime
print(startdatetime)
print(enddatetime)
platforms = argv["platform"].split(",")

build_product_sales_map(str(back_date_90_days_time))
print(len(product_sales_map))
build_parent_child_distribution_map()
print(len(parent_child_distribution_map))

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
      df['UVn'] = df['units']/ df['views']
      df['UVn'] = normalize(df['UVn'])

      df['popularity'] = (len(date_buckets) - bucket_id) *\
        normalize(numpy.log(1 + WEIGHT_VIEWS * df['Vn'] + WEIGHT_UNITS * df['Un'] + WEIGHT_CART_ADDITIONS * df['Cn'] + WEIGHT_REVENUE * df['Rn'])) * 100
      df['popularity_conversion'] = (len(date_buckets) - bucket_id) *\
        normalize(numpy.log(1 + WEIGHT_VIEWS * df['Vn'] + WEIGHT_UNITS * df['Un'] + WEIGHT_CART_ADDITIONS * df['Cn'] + WEIGHT_REVENUE * df['Rn'] + WEIGHT_UNITS_BY_VIEWS * df['UVn'])) * 100
      dfs.append(df.loc[:, ['parent_id', 'popularity', 'popularity_conversion']].set_index('parent_id'))
        
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
  final_df.popularity_conversion = final_df.popularity_conversion.fillna(0)

  final_df['popularity_recent'] = 100 * normalize(final_df['popularity'])
  final_df['popularity_conversion'] = 100 * normalize(final_df['popularity_conversion'])
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
  a['popularity'] = 100 * normalize(0.1 * a['popularity_total'] + 0.9 * a['popularity_recent'])
  a['popularity_recent'] = 100 * normalize(0.3 * a['popularity_total'] + 0.7 * a['popularity_recent'])
  a.popularity= a.popularity.fillna(0)
  a.popularity_recent = a.popularity_recent.fillna(0)
  a.popularity_conversion = a.popularity_conversion.fillna(0)

  ctr = LoopCounter(name='Writing popularity to db', total = len(a.index))
  a = a.sort_values(by='popularity', ascending=True)
  for i, row in a.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    row = dict(row)

    if row.get('parent_id'):
      child_product_list = get_all_the_child_products(row.get('parent_id'))
      if len(child_product_list) > 0:
        popularity_multiplier_factor = get_popularity_multiplier(child_product_list)
      else:
        popularity_multiplier_factor = 1

    #row = {k:v for k,v in row.items() if k in ['cart_additions', 'last_calculated', 'orders', 'parent_id', 'popularity', 'revenue', 'units', 'views']}
    row['last_calculated'] = timestamp
    row['popularity_multiplier_factor'] =  popularity_multiplier_factor
    row['popularity'] = row['popularity']* float(popularity_multiplier_factor)
    row['popularity_recent'] = row['popularity_recent']* float(popularity_multiplier_factor)
    row['popularity_conversion'] = row['popularity_conversion']* float(popularity_multiplier_factor)

    parent_id = row.get('parent_id')
    if parent_id:
      popularity_table.replace_one({"_id": parent_id}, row, upsert=True)
      if parent_id in parent_child_distribution_map:
        for child_id, sale_ratio in parent_child_distribution_map[parent_id].items():
          popularity_table.update({"_id": child_id}, {"$set": {'last_calculated': timestamp, 'parent_id': parent_id},
                                                      "$max": {'popularity': row['popularity'] * sale_ratio,
                                                               'popularity_recent': row['popularity_recent'] * sale_ratio,
                                                               'popularity_conversion': row['popularity_conversion'] * sale_ratio}
                                                      }, upsert=True)

  popularity_table.remove({"last_calculated": {"$ne": timestamp}})

def get_all_the_child_products(parent_id):
  query  = "select distinct(product_id) from catalog_product_super_link where parent_id  = '{0}'".format(parent_id)

  mysql_conn = Utils.nykaaMysqlConnection()
  data = Utils.mysql_read(query, connection=mysql_conn)
  res  = []
  for row in data:
    res.append(str(row['product_id'] ))
  return res


def check_if_product_available(product_id):

  api =  'http://'+ PipelineUtils.getAPIHost()+'/apis/v2/product.list?id={0}'.format(product_id)
  #api =  'http://preprod-api.nyk00-int.network/apis/v2/product.list?id={0}'.format(product_id)
  r  = requests.get(api)
  data  = {}
  try:
    data  =  json.loads(r.content.decode('utf-8'))
    if (not data['result']['in_stock'] ) or (data['result']['disabled']) or (float(data['result']['mrp']) <1):
      return 0
    return 1
  except:
    return 1

def get_popularity_multiplier(product_list):
  global product_sales_map
  if len(product_list) >0:
    total_purchase =0
    for p in product_list:
      if float(product_sales_map.get(str(p), 0))>0:
        total_purchase += float(product_sales_map.get(str(p), 0))

    popularity_multiplier = 1
    if total_purchase > 0:
      for p in product_list:
        flag = check_if_product_available(p)
        if not flag:
          popularity_multiplier -= float(product_sales_map.get(str(p), 0))/total_purchase
    return max(popularity_multiplier, 0)
  else:
    return 0

    

if argv['preprocess']:
  print("preprocess start: %s" % arrow.now())
  preprocess_data()
  print("preprocess end: %s" % arrow.now())

if argv['popularity']:
  print("popularity start: %s" % arrow.now())
  calculate_popularity()
  print("popularity end: %s" % arrow.now())

