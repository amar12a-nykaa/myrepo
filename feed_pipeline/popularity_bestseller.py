import sys
import pandas as pd
import arrow
import numpy
import datetime

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils
from popularity_new import product_validity
from loopcounter import LoopCounter

client = MongoUtils.getClient()
bestseller_data = client['search']['bestseller_data']
order_table = client['search']['order_data']

NO_OF_BRANDS_TO_CONSIDER = 100
NO_OF_PRODUCTS_FROM_EACH_BRAND = 2
NO_OF_PRODUCTS_FROM_EACH_CATEGORY = 5

#_id:product_id, type:[brand,category]

def get_order_data(startdate, enddate):
  print('get order data')
  bucket_results = []
  for p in order_table.aggregate([
    {"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
    {"$group": {"_id": {"product_id": "$product_id", "parent_id": "$parent_id"},
                "orders": {"$sum": "$orders"}
                }
    }
  ]):
    p['product_id'] = p['_id']['product_id']
    p['parent_id'] = p['_id']['parent_id']
    p.pop("_id")
    bucket_results.append(p)
  
  data = pd.DataFrame(bucket_results)
  print(data.columns)
  data = data.astype({'product_id': str, 'parent_id': str})
  return data


def calculate_bestseller_category(order_data):
  query = """select product_id, l3_id from product_category_mapping"""
  redshift_conn = PasUtils.redshiftConnection()
  product_category_mapping = pd.read_sql(query, con=redshift_conn)
  redshift_conn.close()
  product_category_mapping = product_category_mapping.astype({'product_id': str})
  
  df = pd.merge(order_data, product_category_mapping, how='inner', on=['product_id'])
  df = df.sort_values('orders', ascending=False).groupby('l3_id').head(NO_OF_PRODUCTS_FROM_EACH_CATEGORY)
  ctr = LoopCounter(name='Writing popularity to db', total=len(df.index))
  for i, row in df.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    row = dict(row)
    try:
      bestseller_data.update({"_id": row['product_id']}, {"type": "category", "key": row['l3_id']}, upsert=True)
      if row['parent_id'] != row['product_id']:
        bestseller_data.update({"_id": row['parent_id']}, {"type": "category", "key": row['l3_id']}, upsert=True)
    except:
      print("[ERROR] bestseller_data.update error %s " %row['product_id'])
      raise


def calculate_bestseller_brand(order_data):
  query = """select product_id, brand_code from dim_sku where sku_type != 'bundle'"""
  redshift_conn = PasUtils.redshiftConnection()
  brand_data = pd.read_sql(query, con=redshift_conn)
  redshift_conn.close()
  brand_data = brand_data.astype({'product_id': str})

  query = """select brands_v1 as brand_code, brand_popularity from brands order by brand_popularity desc limit %s"""%(NO_OF_BRANDS_TO_CONSIDER)
  mysql_conn = PasUtils.mysqlConnection()
  brand_popularity = pd.read_sql(query, con=mysql_conn)
  mysql_conn.close()
  
  brand_data = pd.merge(brand_data, brand_popularity, on='brand_code')
  
  df = pd.merge(order_data, brand_data, how='inner', on=['product_id'])
  df = df.sort_values('orders', ascending=False).groupby('brand_code').head(NO_OF_PRODUCTS_FROM_EACH_BRAND)
  ctr = LoopCounter(name='Writing popularity to db', total=len(df.index))
  for i, row in df.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    row = dict(row)
    try:
      bestseller_data.update({"_id": row['product_id']}, {"type": "brand", "key": row['brand_code']}, upsert=True)
      if row['parent_id'] != row['product_id']:
        bestseller_data.update({"_id": row['parent_id']}, {"type": "brand", "key": row['brand_code']}, upsert=True)
    except:
      print("[ERROR] bestseller_data.update error %s " % row['product_id'])
      raise
    

if __name__ == '__main__':
  print("bestseller start: %s" % arrow.now())
  startdate = arrow.now().replace(days=-30, hour=0, minute=0, second=0, microsecond=0,
                                  tzinfo=None).datetime.replace(tzinfo=None)
  enddate = arrow.now().replace(days=0, hour=0, minute=0, second=0, microsecond=0,
                                tzinfo=None).datetime.replace(tzinfo=None)
  order_data = get_order_data(startdate, enddate)
  order_data = pd.merge(order_data, product_validity, how='left', on=['product_id', 'parent_id'])
  order_data.valid = order_data.valid.fillna(1)
  order_data.orders = order_data.apply(lambda x: 0 if not x['valid'] else x['orders'], axis=1)

  bestseller_data.remove({})
  
  calculate_bestseller_category(order_data)
  calculate_bestseller_brand(order_data)
  print("bestseller end: %s" % arrow.now())