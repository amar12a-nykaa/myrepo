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

from popularity_new import calculate_new_popularity
from popularity_api import get_popularity_for_id  

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils

sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils
from loopcounter import LoopCounter
from apiutils import ApiUtils


WEIGHT_VIEWS = 10
WEIGHT_UNITS = 30
WEIGHT_CART_ADDITIONS = 10
WEIGHT_REVENUE = 60
PUNISH_FACTOR=0.7
BOOST_FACTOR=1.1
PRODUCT_PUNISH_FACTOR = 0.5
POPULARITY_DECAY_FACTOR = 0.5
POPULARITY_TOTAL_RATIO = 0
POPULARITY_BUCKET_RATIO = 1
COLD_START_DECAY_FACTOR = 0.99

WEIGHT_VIEWS_NEW = 10
WEIGHT_UNITS_NEW = 30
WEIGHT_CART_ADDITIONS_NEW = 10
WEIGHT_REVENUE_NEW = 60
PUNISH_FACTOR_NEW=0.7
BOOST_FACTOR_NEW=1.1
PRODUCT_PUNISH_FACTOR_NEW = 0.5
POPULARITY_DECAY_FACTOR_NEW = 0.5
POPULARITY_TOTAL_RATIO_NEW = 0
POPULARITY_BUCKET_RATIO_NEW = 1
COLD_START_DECAY_FACTOR_NEW = 0.99

BRAND_PROMOTION_LIST = ['1937', '13754', '7666', '71596']
COLDSTART_BRAND_PROMOTION_LIST = ['1937', '13754', '7666', '71596']
PRODUCT_PUNISH_LIST = []
PRODUCT_POPULARITY_OVERRIDES =  { "37894":  40, "502824": 36, "502823": 35, "502828": 34, "502825": 23, "502827": 20,
                                  "502826": 14, "574470": 50, "573588": 50, "573572": 50, "573181": 50}

client = MongoUtils.getClient()
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
  redshift_conn =  PasUtils.redshiftConnection()
  cur  =  redshift_conn.cursor()

  cur.execute(query)

  for row in cur.fetchall():
    product_sales_map[str(row[0])] = float(row[1])

def build_parent_child_distribution_map():
  global parent_child_distribution_map

  query = """select parent_product_id, product_id, count(distinct nykaa_orderno) as orders from fact_order_detail_new
              where sku_type = 'CONFIG' and orderdetail_dt_created >= (CURRENT_DATE - 30) and product_id is not null 
              group by 1,2;"""
  print(query)
  redshift_conn = PasUtils.redshiftConnection()
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

def create_product_attribute():
  query = """select product_id, sku_type, brand_code, mrp, l3_id from dim_sku"""
  print(query)
  redshift_conn = PasUtils.redshiftConnection()
  product_attribute = pd.read_sql(query, con=redshift_conn)
  return product_attribute

parser = argparse.ArgumentParser()
parser.add_argument("--back_date_30_days", help="90 days back date in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
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
back_date_30_days_time = arrow.get(argv['back_date_30_days']).datetime
startdatetime = arrow.get(argv['startdate']).datetime
enddatetime = arrow.get(argv['enddate']).datetime
print(startdatetime)
print(enddatetime)
platforms = argv["platform"].split(",")

build_product_sales_map(str(back_date_30_days_time))
build_parent_child_distribution_map()
print("product_sales_map len: %s" % len(product_sales_map))

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


def get_bucket_results(date_bucket=None):
  if date_bucket:
    startday = date_bucket[1] * -1
    endday = date_bucket[0] * -1
    startdate = arrow.now().replace(days=startday, hour=0, minute=0, second=0, microsecond=0,
                                  tzinfo=None).datetime.replace(tzinfo=None)
    enddate = arrow.now().replace(days=endday, hour=0, minute=0, second=0, microsecond=0,
                                  tzinfo=None).datetime.replace(tzinfo=None)

  else:
    startdate = arrow.get('2011-01-01', 'YYYY-MM-DD').datetime.replace(tzinfo=None)
    enddate = arrow.now().replace(days=0, hour=0, minute=0, second=0, microsecond=0,
                                  tzinfo=None).datetime.replace(tzinfo=None)

  bucket_results = []
  for p in processed_data.aggregate([{"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
                                       {"$group": {"_id": "$parent_id",
                                        "views": {"$sum": "$views"},
                                        "cart_additions": {"$sum": "$cart_additions"},
                                        "orders": {"$sum": "$orders"},
                                        "revenue": {"$sum": "$revenue"},
                                        "units": {"$sum": "$units"},
                                        }},
  ]):
    p['parent_id'] = p.pop("_id")
    bucket_results.append(p)

  if not bucket_results:
    print("Skipping bucket:", date_bucket)
    return None

  print("Processing:", date_bucket)
  df = pd.DataFrame(bucket_results)
  df['Vn'] = normalize(df['views'])
  df['Cn'] = normalize(df['cart_additions'])
  df['On'] = normalize(df['orders'])
  df['Rn'] = normalize(df['revenue'])
  df['Un'] = normalize(df['units'])
  df['UVn'] = df['units'] / df['views']
  df['UVn'] = normalize(df['UVn'])

  return df

def calculate_popularity():
  timestamp = arrow.now().datetime
  results = []

  bucket_start_day = 0 
  bucket_end_day = 180 
  bucket_batch_size = 15
  date_buckets = []
  i = bucket_start_day
  while i < bucket_end_day:
    date_buckets.append((i, i+bucket_batch_size -1))
    i += bucket_batch_size

  print(date_buckets)

  dfs = []
  for bucket_id, date_bucket in enumerate(date_buckets):
    df = get_bucket_results(date_bucket)
    if df is None:
      continue
    multiplication_factor = POPULARITY_DECAY_FACTOR ** (bucket_id + 1)
    print("date_bucket: %s" % str(date_bucket))
    print("bucket_id: %s multiplication_factor: %s" % (bucket_id, multiplication_factor))

    df['popularity'] = multiplication_factor * normalize(numpy.log(1 +
                       WEIGHT_VIEWS * df['Vn'] + WEIGHT_UNITS * df['Un'] + WEIGHT_CART_ADDITIONS *
                       df['Cn'] + WEIGHT_REVENUE * df['Rn'])) * 100
    multiplication_factor_new = POPULARITY_DECAY_FACTOR_NEW ** (bucket_id + 1)
    df['popularity_new'] = multiplication_factor_new * normalize(numpy.log(1 +
                       WEIGHT_VIEWS_NEW * df['Vn'] + WEIGHT_UNITS_NEW * df['Un'] + WEIGHT_CART_ADDITIONS_NEW *
                       df['Cn'] + WEIGHT_REVENUE_NEW * df['Rn'])) * 100

    dfs.append(df.loc[:, ['parent_id', 'popularity', 'popularity_new']].set_index('parent_id'))
        
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
  final_df.popularity_new = final_df.popularity_new.fillna(0)

  final_df['popularity_bucket'] = 100 * normalize(final_df['popularity'])
  final_df['popularity_new_bucket'] = 100 * normalize(final_df['popularity_new'])
  final_df.drop(['popularity', 'popularity_new'], axis = 1, inplace = True)

  df = get_bucket_results()
  df['popularity_total'] = normalize(numpy.log(1 + WEIGHT_VIEWS * df['Vn'] + WEIGHT_UNITS * df['Un'] + WEIGHT_CART_ADDITIONS * df['Cn'] + WEIGHT_REVENUE * df['Rn'])) * 100
  df['popularity_new_total'] = normalize(numpy.log(1 + WEIGHT_VIEWS_NEW * df['Vn'] + WEIGHT_UNITS_NEW * df['Un'] + WEIGHT_CART_ADDITIONS_NEW * df['Cn'] + WEIGHT_REVENUE_NEW * df['Rn'])) * 100
  df = df.set_index("parent_id")

  a = pd.merge(df, final_df, how='outer', left_index=True, right_index=True).reset_index()
  a['popularity'] = 100 * normalize(POPULARITY_TOTAL_RATIO * a['popularity_total'] + POPULARITY_BUCKET_RATIO * a['popularity_bucket'])
  a['popularity_new'] = 100 * normalize(POPULARITY_TOTAL_RATIO_NEW * a['popularity_new_total'] + POPULARITY_BUCKET_RATIO_NEW * a['popularity_new_bucket'])
  a.popularity= a.popularity.fillna(0)
  a.popularity_new = a.popularity_new.fillna(0)

  ctr = LoopCounter(name='Writing popularity to db', total = len(a.index))
  a = applyBoost(a)
  a = handleColdStart(a)
  a = applyOffers(a)
  a.rename(columns={'popularity_new': 'popularity_recent'}, inplace=True)
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

    parent_id = row.get('parent_id')
    if parent_id:
      popularity_table.replace_one({"_id": parent_id}, row, upsert=True)
      if parent_id in parent_child_distribution_map:
        for child_id, sale_ratio in parent_child_distribution_map[parent_id].items():
          popularity_table.remove({"_id": child_id, "last_calculated": {"$ne": timestamp}})
          popularity_table.update({"_id": child_id}, {"$set": {'last_calculated': timestamp, 'parent_id': parent_id},
                                                      "$max": {'popularity': row['popularity'] * sale_ratio,
                                                            'popularity_recent': row['popularity_recent'] * sale_ratio}
                                                      }, upsert=True)

  popularity_table.remove({"last_calculated": {"$ne": timestamp}})
  #Update popularity_recent using new logic
  calculate_new_popularity()
  override_popularity()


def override_popularity():
  for id, popularity in PRODUCT_POPULARITY_OVERRIDES.items():
    popularity_table.update({"_id": id}, {"$set": {'popularity': popularity, 'popularity_recent': popularity}})

def get_all_the_child_products(parent_id):
  query  = "select distinct(product_id) from catalog_product_super_link where parent_id  = '{0}'".format(parent_id)

  mysql_conn = PasUtils.nykaaMysqlConnection()
  data = PasUtils.mysql_read(query, connection=mysql_conn)
  res  = []
  for row in data:
    res.append(str(row['product_id'] ))
  return res


def check_if_product_available(product_id):

  api = 'http://'+ ApiUtils.get_host() +'/apis/v2/product.list?id={0}'.format(product_id)
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

def applyBoost(df):
  product_attr = create_product_attribute()
  dtype = dict(parent_id=int)
  temp_df = pd.merge(df.astype(dtype), product_attr, how='left', left_on=['parent_id'], right_on=['product_id'])

  #punish combo products
  def punish_combos(row):
    if row['sku_type'] and str(row['sku_type']).lower() == 'bundle':
      row['popularity'] = row['popularity'] * PUNISH_FACTOR
      row['popularity_new'] = row['popularity_new'] * PUNISH_FACTOR_NEW
    return row
  temp_df = temp_df.apply(punish_combos, axis=1)

  #promote nykaa products
  def promote_nykaa_products(row):
    if row['brand_code'] in BRAND_PROMOTION_LIST:
      row['popularity'] = row['popularity'] * BOOST_FACTOR
      row['popularity_new'] = row['popularity_new'] * BOOST_FACTOR_NEW
    return row
  temp_df = temp_df.apply(promote_nykaa_products, axis=1)

  #promote indivisual products
  def punish_products_by_id(row):
    if row['product_id'] in PRODUCT_PUNISH_LIST:
      row['popularity'] = row['popularity'] * PRODUCT_PUNISH_FACTOR
      row['popularity_new'] = row['popularity_new'] * PRODUCT_PUNISH_FACTOR_NEW
    return row
  # temp_df = temp_df.apply(punish_products_by_id, axis=1)

  temp_df.drop(['product_id', 'sku_type', 'brand_code', 'mrp', 'l3_id'], axis=1, inplace=True)
  temp_df = temp_df.astype({'parent_id' : str})

  return temp_df

def handleColdStart(df):
  temp_df = df[['parent_id', 'popularity', 'popularity_new']]
  temp_df = temp_df.astype({'parent_id': int, 'popularity': float, 'popularity_new' : float})

  product_attr = create_product_attribute()
  temp_df = pd.merge(temp_df, product_attr, how='left', left_on=['parent_id'], right_on=['product_id'])
  temp_df.drop(['product_id', 'sku_type', 'mrp', 'l3_id'], axis=1, inplace=True)

  query = """select product_id, l3_id from product_category_mapping"""
  redshift_conn = PasUtils.redshiftConnection()
  product_category_mapping = pd.read_sql(query, con=redshift_conn)

  product_data = pd.merge(temp_df, product_category_mapping, left_on=['parent_id'], right_on=['product_id'])

  def percentile(n):
      def _percentile(x):
          return numpy.percentile(x, n)
      return _percentile
  category_popularity = product_data.groupby('l3_id').agg({'popularity': percentile(95), 'popularity_new': percentile(95)}).reset_index()
  category_popularity_boosted = product_data.groupby('l3_id').agg({'popularity': percentile(99), 'popularity_new': percentile(99)}).reset_index()

  product_data = pd.merge(product_category_mapping, category_popularity, on='l3_id')
  product_popularity = product_data.groupby('product_id').agg({'popularity': 'max', 'popularity_new': 'max'}).reset_index()
  product_popularity.rename(columns={'popularity': 'median_popularity', 'popularity_new': 'median_popularity_new'}, inplace=True)
  result = pd.merge(temp_df, product_popularity, left_on='parent_id', right_on='product_id')
  
  product_data = pd.merge(product_category_mapping, category_popularity_boosted, on='l3_id')
  product_popularity = product_data.groupby('product_id').agg({'popularity': 'max', 'popularity_new': 'max'}).reset_index()
  product_popularity.rename(columns={'popularity': 'popularity_99', 'popularity_new': 'popularity_new_99'}, inplace=True)
  result = pd.merge(result, product_popularity, left_on='product_id', right_on='product_id')

  query = """select product_id, sku_created from dim_sku where sku_type != 'bundle' and sku_created > dateadd(day,-60,current_date)"""
  redshift_conn = PasUtils.redshiftConnection()
  product_creation = pd.read_sql(query, con=redshift_conn)

  result = pd.merge(result, product_creation, on='product_id')

  def calculate_new_popularity(row):
    date_diff = abs(datetime.datetime.utcnow() - (numpy.datetime64(row['sku_created']).astype(datetime.datetime))).days
    if date_diff > 0:
        med_popularity = row['median_popularity']
        med_popularity_new = row['median_popularity_new']
        if row['brand_code'] in COLDSTART_BRAND_PROMOTION_LIST:
          med_popularity = row['popularity_99']
          med_popularity_new = row['popularity_new_99']
        row['calculated_popularity'] = row['popularity'] + med_popularity*(COLD_START_DECAY_FACTOR ** date_diff)
        row['calculated_popularity_new'] = row['popularity_new'] + med_popularity_new * (COLD_START_DECAY_FACTOR_NEW ** date_diff)
    else:
        row['calculated_popularity'] = row['popularity']
        row['calculated_popularity_new'] = row['popularity_new']
    return row

  result['calculated_popularity'] = 0
  result['calculated_popularity_new'] = 0
  result = result.apply(calculate_new_popularity, axis=1)

  result = result[['product_id', 'calculated_popularity', 'calculated_popularity_new']]
  final_df = pd.merge(df.astype({'parent_id': int}), result.astype({'product_id': int}), left_on='parent_id',
                      right_on='product_id', how='left')
  # final_df['calculated_popularity'] = final_df.calculated_popularity.fillna(-1)
  final_df['popularity'] = numpy.where(final_df.calculated_popularity.notnull(), final_df.calculated_popularity, final_df.popularity)
  final_df['popularity_new'] = numpy.where(final_df.calculated_popularity_new.notnull(), final_df.calculated_popularity_new, final_df.popularity_new)
  final_df.drop(['calculated_popularity', 'calculated_popularity_new', 'product_id'], axis=1, inplace=True)
  final_df = final_df.astype({'parent_id' : str})
  return final_df

def applyOffers(df):
  start_date = datetime.datetime.now() + datetime.timedelta(minutes=330)
  end_date = start_date + datetime.timedelta(days=1)
  start_date = start_date.strftime('%Y-%m-%d 06:00:00')
  end_date = end_date.strftime('%Y-%m-%d')
  conn = PasUtils.nykaaMysqlConnection(force_production=True)

  query = """select entity_id as offer_id from nykaa_offers where enabled=1 and app = 1 and start_date < '%s' and end_date > '%s'"""%(end_date, start_date)
  offers = pd.read_sql(query, con=conn)
  offers = offers.astype({'offer_id': str})
  offers = list(offers['offer_id'])

  query = """select entity_id as product_id, value as offer_ids from catalog_product_entity_varchar where attribute_id = 678 and store_id = 0 and value is not null"""
  product_offer_mapping = pd.read_sql(query, con=conn)
  conn.close()

  product_offer_mapping['offer_ids'] = product_offer_mapping['offer_ids'].apply(lambda x: x.split(','))
  product_offer_mapping['valid'] = product_offer_mapping['offer_ids'].apply(lambda x: any(i for i in x if i in offers))
  product_offer_mapping = product_offer_mapping[product_offer_mapping.valid == True]
  product_offer_mapping = product_offer_mapping.astype({'product_id' : str, 'valid': bool})
  df = pd.merge(df, product_offer_mapping, how='left', left_on=['parent_id'], right_on=['product_id'])
  df.valid = df.valid.fillna(False)

  def calculate_new_popularity(row):
    if row['valid']:
      row['popularity_new'] = row['popularity_new'] + (row['popularity_new']/10)
    return row
  df = df.apply(calculate_new_popularity, axis=1)
  df.drop(['valid', 'product_id', 'offer_ids'], axis=1, inplace=True)
  return df

if argv['preprocess']:
  print("preprocess start: %s" % arrow.now())
  preprocess_data()
  print("preprocess end: %s" % arrow.now())

if argv['popularity']:
  print("popularity start: %s" % arrow.now())
  # calculate_popularity()
  calculate_new_popularity()
  override_popularity()
  print("popularity end: %s" % arrow.now())

