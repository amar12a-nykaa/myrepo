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
from popularity_new import get_product_validity
from loopcounter import LoopCounter

client = MongoUtils.getClient()
bestseller_data = client['search']['bestseller_data']
order_table = client['search']['order_data']

#_id:product_id, type:[brand,category]

def get_order_data(startdate, enddate):
  print('get order data')
  bucket_results = []
  for p in order_table.aggregate([
    {"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
    {"$group": {"_id": {"product_id": "$product_id"},
                "orders": {"$sum": "$orders"}
                }
    }
  ]):
    p['product_id'] = p['_id']['product_id']
    p.pop("_id")
    bucket_results.append(p)
  
  data = pd.DataFrame(bucket_results)
  print(data.columns)
  data = data.astype({'product_id': str})
  return data


def calculate_bestseller_category(order_data):
  query = """select product_id, l3_id from product_category_mapping"""
  redshift_conn = PasUtils.redshiftConnection()
  product_category_mapping = pd.read_sql(query, con=redshift_conn)
  product_category_mapping = product_category_mapping.astype({'product_id': str})
  
  df = pd.merge(order_data, product_category_mapping, how='inner', on=['product_id'])
  df = df.sort_values('orders', ascending=False).groupby('l3_id').head(5)
  ctr = LoopCounter(name='Writing popularity to db', total=len(df.index))
  for i, row in df.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    row = dict(row)
    try:
      bestseller_data.update({"_id": row['product_id']}, {"type": "category"}, upsert=True)
    except:
      print("[ERROR] bestseller_data.update error %s " %row['product_id'])
      raise
  
  
if __name__ == '__main__':
  print("bestseller start: %s" % arrow.now())
  startdate = arrow.now().replace(days=-30, hour=0, minute=0, second=0, microsecond=0,
                                  tzinfo=None).datetime.replace(tzinfo=None)
  enddate = arrow.now().replace(days=0, hour=0, minute=0, second=0, microsecond=0,
                                tzinfo=None).datetime.replace(tzinfo=None)
  order_data = get_order_data(startdate, enddate)
  product_validity = get_product_validity()
  order_data = pd.merge(order_data, product_validity, how='left', on=['product_id'])
  order_data.valid = order_data.valid.fillna(1)
  order_data.orders = order_data.apply(lambda x: 0 if not x['valid'] else x['orders'], axis=1)
  
  
  calculate_bestseller_category(order_data)
  # calculate_bestseller_brand()
  print("bestseller end: %s" % arrow.now())