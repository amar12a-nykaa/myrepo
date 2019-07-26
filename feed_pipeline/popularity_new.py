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
from loopcounter import LoopCounter

client = MongoUtils.getClient()
processed_data = client['search']['processed_data']
popularity_table = client['search']['popularity']
order_data = client['search']['order_data']

POPULARITY_DECAY_FACTOR=0.5
WEIGHT_VIEWS = 10
WEIGHT_UNITS = 0
WEIGHT_ORDERS = 40
WEIGHT_CART_ADDITIONS = 10
WEIGHT_REVENUE = 50
POPULARITY_TOTAL_RATIO = 0
POPULARITY_BUCKET_RATIO = 1
PUNISH_FACTOR=0.7
BOOST_FACTOR=1.1
PRODUCT_PUNISH_FACTOR = 0.5
COLD_START_DECAY_FACTOR = 0.99

POPULARITY_DECAY_FACTOR_NEW = 0.5
WEIGHT_VIEWS_NEW = 10
WEIGHT_UNITS_NEW = 0
WEIGHT_ORDERS_NEW = 40
WEIGHT_CART_ADDITIONS_NEW = 10
WEIGHT_REVENUE_NEW = 50
POPULARITY_TOTAL_RATIO_NEW = 0.1
POPULARITY_BUCKET_RATIO_NEW = 0.9
PUNISH_FACTOR_NEW=0.7
BOOST_FACTOR_NEW=1.1
PRODUCT_PUNISH_FACTOR_NEW = 0.5
COLD_START_DECAY_FACTOR_NEW = 0.99
BRAND_PROMOTION_SCORE = .9925

BRAND_PROMOTION_LIST = ['1937', '13754', '7666', '71596']
COLDSTART_BRAND_PROMOTION_LIST = ['1937', '13754', '7666', '71596']
PRODUCT_PUNISH_LIST = []
    
def get_brand_popularity():
  print('get brand popularity')

  query = """select brands_v1 as brand_id, brand_popularity from brands"""
  mysql_conn = PasUtils.mysqlConnection()
  data = pd.read_sql(query, con=mysql_conn)
  mysql_conn.close()
  print(data.head(5))
  return data

def get_product_validity():
  print('get product validity')

  query = """select product_id, parent_id, is_in_stock, mrp, disabled
                from products"""
  mysql_conn = PasUtils.mysqlConnection()
  data = pd.read_sql(query, con=mysql_conn)
  mysql_conn.close()
  data.mrp = data.mrp.fillna(0)
  data = data[(data.is_in_stock == 0) | (data.disabled == 1) | (data.mrp < 1)]
  data['valid'] = 0
  data = data.astype({'product_id': str, 'parent_id': str})
  data.drop(['is_in_stock', 'mrp', 'disabled'], axis=1, inplace=True)
  print(data.columns)
  return data
  

def create_child_parent_map():
  print("create child parent map")
  query = """SELECT cpsl.product_id, cpsl.parent_id
              FROM catalog_product_entity e
	            JOIN catalog_product_super_link cpsl ON e.entity_id = cpsl.product_id
              JOIN catalog_product_entity_int cpei ON cpei.entity_id = cpsl.parent_id
              WHERE e.type_id = 'simple' and cpei.attribute_id = 80 AND cpei.value = 1
              GROUP BY cpsl.product_id;"""
  nykaa_conn = PasUtils.nykaaMysqlConnection()
  child_parent_map = pd.read_sql(query, con=nykaa_conn)
  child_parent_map = child_parent_map.astype({'parent_id': str, 'product_id': str})
  print(child_parent_map.columns)
  return child_parent_map
  

def get_child_distribution_ratio(startdate, enddate):
  print('get distribution ratio')
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
  print(data.columns)
  return data


def get_omniture_data(startdate, enddate):
  print("get omniture data")
  bucket_results = []
  for p in processed_data.aggregate([
    {"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
    {"$group": {"_id": "$parent_id",
                "views": {"$sum": "$views"},
                "cart_additions": {"$sum": "$cart_additions"},
                "units_om": {"$sum": "$units"},
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
  print(df.columns)
  return df, True


def get_order_data(startdate, enddate):
  print('get order data')
  bucket_results = []
  for p in order_data.aggregate([
    {"$match": {"date": {"$gte": startdate, "$lte": enddate}}},
    {"$group": {"_id": {"product_id": "$product_id", "parent_id": "$parent_id"},
                "orders": {"$sum": "$orders"},
                "units": {"$sum": "$units"},
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


def normalize(a):
  return (a-min(a))/(max(a)-min(a))


def get_bucket_results(date_bucket=None):
  global child_parent_map
  global product_validity
  
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
  
  print('getting data for %s %s'%(startdate, enddate))
  omniture_data, valid = get_omniture_data(startdate, enddate)
  if not valid:
    print("Skipping bucket:", date_bucket)
    return None
  order_data = get_order_data(startdate, enddate)
  
  #remove oos products
  order_data = pd.merge(order_data, product_validity, how='left', on=['product_id', 'parent_id'])
  order_data.valid = order_data.valid.fillna(1)
  order_data.orders = order_data.apply(lambda x: 0 if not x['valid'] else x['orders'], axis=1)
  order_data.revenue = order_data.apply(lambda x: 0 if not x['valid'] else x['revenue'], axis=1)
  order_data.units = order_data.apply(lambda x: 0 if not x['valid'] else x['units'], axis=1)
  
  # create_parent_matrix
  parent_order_data = order_data.groupby('parent_id').agg({'orders': 'sum', 'revenue': 'sum', 'units': 'sum'}).reset_index()
  parent = pd.merge(omniture_data, parent_order_data, how='left', on='parent_id')
  parent['orders'] = numpy.where(parent.orders.notnull(), parent.orders, parent.orders_om)
  parent['revenue'] = numpy.where(parent.revenue.notnull(), parent.revenue, parent.revenue_om)
  parent['units'] = numpy.where(parent.units.notnull(), parent.units, parent.units_om)
  parent.orders = parent.orders.fillna(0)
  parent.revenue = parent.revenue.fillna(0)
  parent.units = parent.units.fillna(0)
  parent.drop(['orders_om', 'revenue_om', 'units_om'], axis=1, inplace=True)
  parent.rename(columns={'parent_id': 'id'}, inplace=True)
  
  #create_child_matrix
  child_order_data = order_data.groupby('product_id').agg({'orders': 'sum', 'revenue': 'sum', 'units': 'sum'}).reset_index()
  child_distribution_ratio = get_child_distribution_ratio(startdate, enddate)
  child = pd.merge(child_parent_map, child_distribution_ratio, how='left', on=['product_id', 'parent_id'])
  child.ratio = child.ratio.fillna(1)
  child = pd.merge(child, omniture_data, how='inner', on='parent_id')
  child['views'] = child.apply(lambda x: x['views']/x['ratio'], axis=1)
  child['cart_additions'] = child.apply(lambda x: x['cart_additions']/x['ratio'], axis=1)
  child.drop(['orders_om', 'revenue_om', 'units_om', 'ratio', 'parent_id'], axis=1, inplace=True)
  child = pd.merge(child, child_order_data, how='left', on='product_id')
  child.orders = child.orders.fillna(0)
  child.units = child.units.fillna(0)
  child.revenue = child.revenue.fillna(0)
  child.rename(columns={'product_id': 'id'}, inplace=True)

  df = pd.concat([parent, child])
  df = df.groupby('id').agg({'views': 'max',
                             'cart_additions': 'max',
                             'orders': 'max',
                             'units': 'max',
                             'revenue': 'max'}).reset_index()
  df['views'] = normalize(df['views'])
  df['cart_additions'] = normalize(df['cart_additions'])
  df['orders'] = normalize(df['orders'])
  df['units'] = normalize(df['units'])
  df['revenue'] = normalize(df['revenue'])
  print(df.columns)
  return df
  

def calculate_new_popularity():
  global child_parent_map
  
  timestamp = arrow.now().datetime
  print('calculating new popularity %s'%timestamp)
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

    multiplication_factor = POPULARITY_DECAY_FACTOR ** (bucket_id + 1)
    print("date_bucket: %s bucket_id: %s multiplication_factor: %s" %(str(date_bucket),bucket_id,multiplication_factor))
    df['popularity'] = multiplication_factor * normalize(
                        numpy.log(1 + WEIGHT_VIEWS * df['views'] + WEIGHT_ORDERS * df['orders'] + WEIGHT_UNITS * df['units'] +
                          WEIGHT_CART_ADDITIONS * df['cart_additions'] + WEIGHT_REVENUE * df['revenue'])) * 100

    multiplication_factor_new = POPULARITY_DECAY_FACTOR_NEW ** (bucket_id + 1)
    df['popularity_new'] = multiplication_factor_new * normalize(
                            numpy.log(1 + WEIGHT_VIEWS_NEW * df['views'] + WEIGHT_ORDERS_NEW * df['orders'] + WEIGHT_UNITS_NEW * df['units'] +
                              WEIGHT_CART_ADDITIONS_NEW * df['cart_additions'] + WEIGHT_REVENUE_NEW * df['revenue'])) * 100

    dfs.append(df.loc[:, ['id', 'popularity', 'popularity_new']].set_index('id'))

  final_df = dfs[0]
  for i in range(1, len(dfs)):
    final_df = pd.DataFrame.add(final_df, dfs[i], fill_value=0)
  final_df.popularity = final_df.popularity.fillna(0)
  final_df.popularity_new = final_df.popularity_new.fillna(0)

  final_df['popularity_bucket'] = 100 * normalize(final_df['popularity'])
  final_df['popularity_new_bucket'] = 100 * normalize(final_df['popularity_new'])
  final_df.drop(['popularity', 'popularity_new'], axis=1, inplace=True)
  
  #get_total_popularity
  df = get_bucket_results()
  df['popularity_total'] = normalize(numpy.log(1 + WEIGHT_VIEWS * df['views'] + WEIGHT_ORDERS * df['orders'] + WEIGHT_UNITS * df['units'] +
                              WEIGHT_CART_ADDITIONS * df['cart_additions'] + WEIGHT_REVENUE * df['revenue'])) * 100
  df['popularity_new_total'] = normalize(numpy.log(1 + WEIGHT_VIEWS_NEW * df['views'] + WEIGHT_ORDERS_NEW * df['orders'] + WEIGHT_UNITS_NEW * df['units'] +
                              WEIGHT_CART_ADDITIONS_NEW * df['cart_additions'] + WEIGHT_REVENUE_NEW * df['revenue'])) * 100
  df = df.set_index("id")

  a = pd.merge(df, final_df, how='outer', left_index=True, right_index=True).reset_index()
  a['popularity'] = 100 * normalize(
    POPULARITY_TOTAL_RATIO * a['popularity_total'] + POPULARITY_BUCKET_RATIO * a['popularity_bucket'])
  a['popularity_new'] = 100 * normalize(
    POPULARITY_TOTAL_RATIO_NEW * a['popularity_new_total'] + POPULARITY_BUCKET_RATIO_NEW * a['popularity_new_bucket'])
  a.popularity = a.popularity.fillna(0)
  a.popularity_new = a.popularity_new.fillna(0)
  # business_logic
  a = applyBoost(a)
  a = handleColdStart(a)
  a.rename(columns={'popularity_new': 'popularity_recent'}, inplace=True)
  a = a.sort_values(by='popularity', ascending=True)
  a.to_csv('a.csv', index=False)
  
  ctr = LoopCounter(name='Writing popularity to db', total=len(a.index))
  for i, row in a.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    row = dict(row)
    id = row.get('id')
    popularity_table.update({"_id": id}, {"$set": {'popularity': row['popularity']}})


def applyBoost(df):
  query = """select product_id, sku_type, brand_code, mrp, l3_id from dim_sku"""
  print(query)
  redshift_conn = PasUtils.redshiftConnection()
  product_attr = pd.read_sql(query, con=redshift_conn)
  redshift_conn.close()

  dtype = dict(id=int)
  temp_df = pd.merge(df.astype(dtype), product_attr, how='left', left_on=['id'], right_on=['product_id'])

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
  temp_df.drop(['product_id', 'sku_type', 'mrp', 'l3_id'], axis=1, inplace=True)
  temp_df = temp_df.astype({'id': str})

  return temp_df


def handleColdStart(df):
  global child_parent_map
  temp_df = df[['id', 'popularity', 'popularity_new']]
  
  #remove config child product
  temp_df = pd.merge(temp_df, child_parent_map, left_on='id', right_on='product_id', how='left')
  temp_df = temp_df[temp_df['product_id'].isnull()]
  temp_df = temp_df[['id', 'popularity', 'popularity_new']]
  temp_df = temp_df.astype({'id': int, 'popularity': float, 'popularity_new': float})

  query = """select product_id, l3_id from product_category_mapping"""
  redshift_conn = PasUtils.redshiftConnection()
  product_category_mapping = pd.read_sql(query, con=redshift_conn)

  product_data = pd.merge(temp_df, product_category_mapping, left_on=['id'], right_on=['product_id'])
  
  query = """select product_id, sku_created, brand_code from dim_sku where sku_type != 'bundle' and sku_created > dateadd(day,-60,current_date)"""
  product_creation = pd.read_sql(query, con=redshift_conn)
  redshift_conn.close()

  brand_popularity.rename(columns={'brand_id': 'brand_code'}, inplace=True)
  result = pd.merge(product_data, product_creation, on='product_id')
  result = pd.merge(result, brand_popularity, on='brand_code')

  def normalize_90_to_99(a):
    return (((a - min(a))/(max(a) - min(a)))*9 + 90)/100

  result['brand_popularity'] = normalize_90_to_99(result['brand_popularity'])
  result = result.loc[result['sku_created'].notnull()]

  def update_popularity(row):
    date_diff = abs(datetime.datetime.utcnow() - (numpy.datetime64(row['sku_created']).astype(datetime.datetime))).days
    if date_diff > 0:
        percentile_value = row['brand_popularity']
        if row['brand_code'] in COLDSTART_BRAND_PROMOTION_LIST:
          percentile_value = BRAND_PROMOTION_SCORE
        med_popularity = result[result['l3_id'] == row['l3_id']].popularity.quantile(percentile_value)
        med_popularity_new = result[result['l3_id'] == row['l3_id']].popularity_new.quantile(percentile_value)
        row['calculated_popularity'] = row['popularity'] + med_popularity*(percentile_value ** date_diff)
        row['calculated_popularity_new'] = row['popularity_new'] + med_popularity_new*(percentile_value ** date_diff)
    else:
        row['calculated_popularity'] = row['popularity']
        row['calculated_popularity_new'] = row['popularity_new']
    return row

  result['calculated_popularity'] = 0
  result['calculated_popularity_new'] = 0
  result = result.apply(update_popularity, axis=1)

  result = result[['product_id', 'calculated_popularity', 'calculated_popularity_new']]
  result = result.groupby('product_id').agg({'calculated_popularity': 'max', 'calculated_popularity_new': 'max'}).reset_index()
  final_df = pd.merge(df.astype({'id': int}),  result.astype({'product_id': int}), left_on='id', right_on='product_id', how='left')
  final_df['popularity'] = numpy.where(final_df.calculated_popularity.notnull(), final_df.calculated_popularity, final_df.popularity)
  final_df['popularity_new'] = numpy.where(final_df.calculated_popularity_new.notnull(), final_df.calculated_popularity_new, final_df.popularity_new)
  final_df.drop(['calculated_popularity', 'calculated_popularity_new', 'product_id'], axis=1, inplace=True)
  final_df = final_df.astype({'id': str})
  return final_df


child_parent_map = create_child_parent_map()
product_validity = get_product_validity()
brand_popularity = get_brand_popularity()

if __name__ == '__main__':
  print("popularity start: %s" % arrow.now())
  calculate_new_popularity()
  print("popularity end: %s" % arrow.now())
