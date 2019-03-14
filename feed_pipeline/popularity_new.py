import sys
import pandas as pd
import arrow
import numpy

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

client = Utils.mongoClient()
raw_data = client['search']['raw_data']
processed_data = client['search']['processed_data']
popularity_table = client['search']['popularity']
order_data = client['search']['order_data']


def get_child_distribution_ratio(startdate, enddate):
  bucket_results = []
  for p in order_data.aggregate([
    {"$project": {"ischild": {"$cmp": ['$product_id', '$parent_id']},
                  "date": 1, "product_id": 1, "parent_id": 1, "orders": 1}},
    {"$match": {"date": {"$gte": startdate, "$lte": enddate}, "ischild": {"$ne": 0}}},
    {"$group": {"_id": {"product_id": "$product_id", "parent_id": "$parent_id"},
                "orders": {"$sum": "$orders"}}}
  ]):
    p['parent_id'] = p['_id'].get('parent_id')
    p['product_id'] = p['_id']['product_id']
    p.pop("_id")
    bucket_results.append(p)
  
  data = pd.DataFrame(bucket_results)
  parent_data = data.groupby('parent_id', as_index=False)['orders'].sum()
  parent_data.rename(columns={'orders': 'total_order'}, inplace=True)
  data = pd.merge(data, parent_data, on='parent_id', how='inner')
  data['ratio'] = data.apply(lambda x: float(x['orders']) / x['total_order'], axis=1)
  data.drop(['orders', 'total_order'], axis=1, inplace=True)
  data = data.astype({'parent_id': str, 'product_id': str, 'ratio': float})
  return data


def get_omniture_data(startdate, enddate):
  bucket_results = []
  for p in processed_data.aggregate([
    {"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
    {"$group": {"_id": "$parent_id",
                "views": {"$sum": "$views"},
                "cart_additions": {"$sum": "$cart_additions"},
                "orders_om": {"$sum": "$orders"},
                "revenue_om": {"$sum": "$revenue"}
                }},
  ]):
    p['parent_id'] = p.pop("_id")
    bucket_results.append(p)
  
  if not bucket_results:
    return None, False
  
  df = pd.DataFrame(bucket_results)
  df = df.astype({'parent_id': str})
  return df, True


def get_order_data(startdate, enddate):
  bucket_results = []
  for p in order_data.aggregate([
    {"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
    {"$group": {"_id": {"product_id": "$product_id", "parent_id": "$parent_id"},
                "orders": {"$sum": "$orders"},
                "revenue": {"$sum": "$revenue"}
                }
     }
  ]):
    p['parent_id'] = p['_id'].get('parent_id')
    p['product_id'] = p['_id']['product_id']
    p.pop("_id")
    bucket_results.append(p)
  
  data = pd.DataFrame(bucket_results)
  print(data.columns)
  data = data.astype({'parent_id': str, 'product_id': str})
  return data


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
  
  omniture_data, valid = get_omniture_data(startdate, enddate)
  if not valid:
    print("Skipping bucket:", date_bucket)
    return None
  order_data = get_order_data(startdate, enddate)
  child_distribution_ratio = get_child_distribution_ratio(startdate, enddate)
  
  # create_parent_matrix
  parent_order_data = order_data.groupby('parent_id').agg({'orders': 'sum', 'revenue': 'sum'})
  parent = pd.merge(omniture_data, parent_order_data, how='left', on='parent_id')
  parent['orders'] = numpy.where(parent.orders.notnull(), parent.orders, parent.orders_om)
  parent['revenue'] = numpy.where(parent.revenue.notnull(), parent.revenue, parent.revenue_om)
  parent.drop(['orders_om', 'revenue_om'], axis=1, inplace=True)
  print(parent)


def calculate_popularity():
  timestamp = arrow.now().datetime
  
  bucket_start_day = 0
  bucket_end_day = 180
  bucket_batch_size = 15
  date_buckets = []
  i = bucket_start_day
  while i < bucket_end_day:
    date_buckets.append((i, i + bucket_batch_size - 1))
    i += bucket_batch_size
  
  print(date_buckets)
  
  dfs = []
  for bucket_id, date_bucket in enumerate(date_buckets):
    df = get_bucket_results(date_bucket)
    if df is None:
      continue


if __name__ == '__main__':
  print("popularity start: %s" % arrow.now())
  calculate_popularity()
  print("popularity end: %s" % arrow.now())
