import sys
import pandas as pd
import arrow
import numpy
import datetime
import json

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
WEIGHT_ASP = 0
WEIGHT_CREATION = 0
POPULARITY_TOTAL_RATIO = 0
POPULARITY_BUCKET_RATIO = 1
PUNISH_FACTOR=0.7
BOOST_FACTOR=1.1
PRODUCT_PUNISH_FACTOR = 0.5
COLD_START_DECAY_FACTOR = 0.99
CREATION_DECAY_FACTOR = 0.999

POPULARITY_DECAY_FACTOR_NEW = 0.5
WEIGHT_VIEWS_NEW = 10
WEIGHT_UNITS_NEW = 0
WEIGHT_ORDERS_NEW = 40
WEIGHT_CART_ADDITIONS_NEW = 10
WEIGHT_REVENUE_NEW = 50
WEIGHT_ASP_NEW = 0
WEIGHT_CREATION_NEW = 5
POPULARITY_TOTAL_RATIO_NEW = 0
POPULARITY_BUCKET_RATIO_NEW = 1
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

  query = """select brand_id, brand_popularity from brands"""
  mysql_conn = PasUtils.mysqlConnection()
  data = pd.read_sql(query, con=mysql_conn)
  mysql_conn.close()
  print(data.head(5))
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


def get_product_creation_data():
  query = """select product_id, created_at, generic_attributes from solr_dump_4"""
  df = pd.read_sql(query, con=DiscUtils.nykaaMysqlConnection())
  df['product_enable_time'] = None
  today = datetime.datetime.now()
  min_enable_time = datetime.datetime.strptime("2019-06-30 00:00:00", "%Y-%m-%d %H:%M:%S")
  min_create_time = datetime.datetime.strptime("2012-02-07 00:00:00", "%Y-%m-%d %H:%M:%S")

  def extract_product_enable_time(row):
    try:
      if row.get('generic_attributes'):
        generic_attributes_raw = '{' + row.get('generic_attributes') + '}'
        ga = json.loads(generic_attributes_raw.replace('\n', ' ').replace('\\n', ' ').replace('\r', '').
                        replace('\\r', '').replace('\\\\"', '\\"'))
        if ga.get('product_enable_time'):
          enable_time = ga.get('product_enable_time').get('value')
          enable_time = datetime.datetime.strptime(enable_time, "%Y-%m-%d %H:%M:%S")
          if enable_time <= min_enable_time:
            row['product_enable_time'] = row.get('created_at')
          else:
            row['product_enable_time'] = enable_time
      else:
        row['product_enable_time'] = row.get('created_at')
      if not row.get('product_enable_time'):
        row['product_enable_time'] = min_create_time
      row['days_count'] = abs(today - row['product_enable_time']).days + 1
      row['days'] = (CREATION_DECAY_FACTOR)**row['days_count']
    except Exception as ex:
      print(ex)
      pass
    return row

  df = df.apply(extract_product_enable_time, axis=1)
  df.drop(['generic_attributes', 'created_at'], axis=1, inplace=True)
  df = df.astype({'product_id': float})
  df = df.astype({'product_id': int})
  df = df.astype({'product_id': str})
  return df


def normalize(a):
  return (a-min(a))/(max(a)-min(a))


def get_bucket_results(date_bucket=None):
  global child_parent_map
  global product_creation_data
  
  if date_bucket:
    startday = date_bucket[1] * -1
    endday = date_bucket[0] * -1
    startdate = arrow.now().replace(days=startday, hour=0, minute=0, second=0, microsecond=0,
                                    tzinfo=None).datetime.replace(tzinfo=None)
    enddate = arrow.now().replace(days=endday, hour=0, minute=0, second=0, microsecond=0,
                                  tzinfo=None).datetime.replace(tzinfo=None)
    ignore_window_start = arrow.get('2020-03-24', 'YYYY-MM-DD').datetime.replace(tzinfo=None)
    ignore_window_end = arrow.get('2020-05-03', 'YYYY-MM-DD').datetime.replace(tzinfo=None)
    if startdate >= ignore_window_start and enddate < ignore_window_end:
      print("Skipping bucket:", date_bucket)
      return None
    if enddate > ignore_window_start and enddate < ignore_window_end:
      enddate = ignore_window_start
    elif startdate < ignore_window_end and enddate > ignore_window_end:
      startdate = ignore_window_end
  
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
  order_data.orders = order_data.orders.fillna(0)
  order_data.revenue = order_data.revenue.fillna(0)
  order_data.units = order_data.units.fillna(0)
  
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
  creation = product_creation_data[['product_id', 'days']]
  creation.rename(columns={'product_id': 'id'}, inplace=True)
  df = pd.merge(df, creation, on="id", how="right")
  df = df.astype({'id': float})
  df = df.astype({'id': int})
  df.views = df.views.fillna(0)
  df.cart_additions = df.cart_additions.fillna(0)
  df.orders = df.orders.fillna(0)
  df.units = df.units.fillna(0)
  df.revenue = df.revenue.fillna(0)
  df['views'] = normalize(df['views'])
  df['cart_additions'] = normalize(df['cart_additions'])
  df['orders'] = normalize(df['orders'])
  df['units'] = normalize(df['units'])
  df['revenue'] = normalize(df['revenue'])
  df['days'] = normalize(df['days'])
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
    df['asp'] = df.apply(lambda y: (y['revenue']/y['units']) if y['units'] > 0 else 0, axis=1)
    df['popularity'] = multiplication_factor * normalize(
                        numpy.log(1 + WEIGHT_VIEWS * df['views'] + WEIGHT_ORDERS * df['orders'] +
                          WEIGHT_UNITS * df['units'] + WEIGHT_CART_ADDITIONS * df['cart_additions'] +
                          WEIGHT_REVENUE * df['revenue'] + WEIGHT_ASP * df['asp'] + WEIGHT_CREATION * df['days'])) * 100

    multiplication_factor_new = POPULARITY_DECAY_FACTOR_NEW ** (bucket_id + 1)
    df['popularity_new'] = multiplication_factor_new * normalize(
                            numpy.log(1 + WEIGHT_VIEWS_NEW * df['views'] + WEIGHT_ORDERS_NEW * df['orders'] +
                              WEIGHT_UNITS_NEW * df['units'] + WEIGHT_CART_ADDITIONS_NEW * df['cart_additions'] +
                              WEIGHT_REVENUE_NEW * df['revenue'] + WEIGHT_ASP_NEW * df['asp'] + WEIGHT_CREATION_NEW * df['days'])) * 100

    dfs.append(df.loc[:, ['id', 'popularity', 'popularity_new']].set_index('id'))

  final_df = dfs[0]
  for i in range(1, len(dfs)):
    final_df = pd.DataFrame.add(final_df, dfs[i], fill_value=0)
  final_df.popularity = final_df.popularity.fillna(0)
  final_df.popularity_new = final_df.popularity_new.fillna(0)
  
  #get_total_popularity
  if POPULARITY_TOTAL_RATIO > 0:
    final_df['popularity_bucket'] = 100 * normalize(final_df['popularity'])
    final_df['popularity_new_bucket'] = 100 * normalize(final_df['popularity_new'])
    final_df.drop(['popularity', 'popularity_new'], axis=1, inplace=True)
    
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
  else:
    a = final_df.reset_index()
    a['popularity'] = 100 * normalize(a['popularity'])
    a['popularity_new'] = 100 * normalize(a['popularity_new'])
  
  a.popularity = a.popularity.fillna(0)
  a.popularity_new = a.popularity_new.fillna(0)
  # business_logic
  a = applyBoost(a)
  a = handleColdStart(a)
  a.rename(columns={'popularity_new': 'popularity_recent'}, inplace=True)
  a = a.sort_values(by='popularity', ascending=True)
  try:
    a.views = a.views.fillna(0)
  except:
    a['views'] = 0
  a.views = a.views.fillna(0)
  a.to_csv('a.csv', index=False)
  
  try:
    ids = list(a['id'])
    ids = ids[-50:]
    ids.reverse()
    ids = ','.join(ids)
    query = """replace into static_product_widget(type, data) values('BESTSELLERS', '%s')""" %ids
    PasUtils.mysql_write(query)
  except Exception as ex:
    print(ex)
    print('Could not write in mysql')
    pass
  
  ctr = LoopCounter(name='Writing popularity to db', total=len(a.index))
  for i, row in a.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    row = dict(row)
    row['last_calculated'] = timestamp
    id = row.get('id')
    popularity_table.update({"_id": id}, row, upsert=True)
  popularity_table.remove({"last_calculated": {"$ne": timestamp}})

def applyBoost(df):
  query = """select product_id, sku_type, brand_code, mrp, l3_id, kits_combo from dim_sku"""
  print(query)
  redshift_conn = PasUtils.redshiftConnection()
  product_attr = pd.read_sql(query, con=redshift_conn)
  redshift_conn.close()

  dtype = dict(id=int)
  temp_df = pd.merge(df.astype(dtype), product_attr, how='left', left_on=['id'], right_on=['product_id'])

  #punish combo products
  def punish_combos(row):
    if (row['sku_type'] and str(row['sku_type']).lower() == 'bundle') or row['kits_combo']=='Yes':
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

def get_product_creation():
  query = """select product_id, brands_v1 as brand_code, generic_attributes from solr_dump_4"""
  df = pd.read_sql(query, con=DiscUtils.nykaaMysqlConnection())
  df['product_enable_time'] = None
  today = datetime.datetime.now()
  startdate = today - datetime.timedelta(days=30)
  
  def extract_product_enable_time(row):
    try:
      if row.get('generic_attributes'):
        generic_attributes_raw = '{' + row.get('generic_attributes') + '}'
        ga = json.loads(generic_attributes_raw.replace('\n', ' ').replace('\\n', ' ').replace('\r', '').
                        replace('\\r', '').replace('\\\\"', '\\"'))
        if ga.get('product_enable_time'):
          enable_time = ga.get('product_enable_time').get('value')
          enable_time = datetime.datetime.strptime(enable_time, "%Y-%m-%d %H:%M:%S")
          if enable_time >= startdate and enable_time <= today:
            row['product_enable_time'] = enable_time
      if row.get('brand_code'):
        row['brand_code'] = row['brand_code'].split('|')[0]
      else:
        row['product_enable_time'] = None
    except Exception as ex:
      print(ex)
      pass
    return row
  
  df = df.apply(extract_product_enable_time, axis=1)
  df = df.dropna()
  df.drop(['generic_attributes'], axis=1, inplace=True)
  query = """select product_id, parent_id, type_id from solr_dump_3 where type_id != 'bundle'"""
  df2 = pd.read_sql(query, con=DiscUtils.nykaaMysqlConnection())
  
  def extract_parent_id(row):
    if row['type_id'].strip() == 'simple' and row['parent_id'] and row['parent_id'] != 'NULL':
      return row
    else:
      row['parent_id'] = row['product_id']
      return row

  df2 = df2.apply(extract_parent_id, axis=1)
  df = pd.merge(df, df2, on="product_id", how="inner")
  return df


def handleColdStart(df):
  global child_parent_map
  global brand_popularity
  temp_df = df[['id', 'popularity', 'popularity_new', 'kits_combo']]

  temp_df = temp_df.astype({'id': int, 'popularity': float, 'popularity_new': float})

  query = """select product_id, l3_id from product_category_mapping"""
  redshift_conn = PasUtils.redshiftConnection()
  product_category_mapping = pd.read_sql(query, con=redshift_conn)
  redshift_conn.close()

  product_creation = get_product_creation()
  product_creation = product_creation.astype({'product_id': int})

  result = pd.merge(temp_df, product_creation, left_on='id', right_on='product_id', how='right')
  result.fillna(0, inplace=True)
  result = result.astype({'parent_id': int, 'brand_code': int})
  #remove child products if its parent is in the bucket
  def remove_child_products(data):
    df = data[data.parent_id == data.product_id]
    if len(data.index) > 1 and len(df.index) >= 1:
      return data[data.parent_id == data.product_id]
    return data

  result = result.groupby('parent_id', as_index=False).apply(remove_child_products)
  brand_popularity = brand_popularity.astype({'brand_id': str})
  result = result.astype({'brand_code': str})
  result = pd.merge(result, product_category_mapping, on='product_id')
  brand_popularity.rename(columns={'brand_id': 'brand_code'}, inplace=True)
  result = pd.merge(result, brand_popularity, on='brand_code', how='left')

  def normalize_brand_popularity(a):
    return (((a - min(a))/(max(a) - min(a)))*9 + 90)/100

  result['brand_popularity'] = normalize_brand_popularity(result['brand_popularity'])
  result = result.loc[result['product_enable_time'].notnull()]
  result.fillna(0, inplace=True)

  def update_popularity(data):
    count = 0
    for index,row in data.iterrows():
      if count==3:
        break
      date_diff = abs(datetime.datetime.utcnow() - (numpy.datetime64(row['product_enable_time']).astype(datetime.datetime))).days
      if date_diff >= 0 and (row.get('kits_combo','No') == 'No' or str(row.get('kits_combo', "0")) == "0"):
          percentile_value = row['brand_popularity']
          if row['brand_code'] in COLDSTART_BRAND_PROMOTION_LIST:
            percentile_value = BRAND_PROMOTION_SCORE
          med_popularity = result[result['l3_id'] == row['l3_id']].popularity.quantile(percentile_value)
          med_popularity_new = result[result['l3_id'] == row['l3_id']].popularity_new.quantile(percentile_value)
          calculated_popularity = row['popularity'] + (0.9**count)*med_popularity*(percentile_value ** date_diff)
          calculated_popularity_new = row['popularity_new'] + (0.9**count)*med_popularity_new*(percentile_value ** date_diff)
          data.at[index, 'calculated_popularity'] = calculated_popularity
          data.at[index, 'calculated_popularity_new'] = calculated_popularity_new
          data.at[index, 'cold_start_value'] = calculated_popularity - row['popularity']
          count += 1
    return data

  result['calculated_popularity'] = result['popularity']
  result['calculated_popularity_new'] = result['popularity_new']
  result['cold_start_value'] = -1
  result = result.groupby('parent_id', as_index=False).apply(update_popularity)

  result = result[['product_id', 'calculated_popularity', 'calculated_popularity_new', 'cold_start_value']]
  result = result.groupby('product_id').agg({'calculated_popularity': 'max', 'calculated_popularity_new': 'max', 'cold_start_value': 'max'}).reset_index()
  try:
    cold_start = result.sort_values(by='cold_start_value', ascending=False)
    ids = list(cold_start['product_id'])
    ids = ids[:50]
    ids = ','.join(map(str, ids))
    query = """replace into static_product_widget(type, data) values('NEW_LAUNCHES', '%s')"""%ids
    PasUtils.mysql_write(query)
  except Exception as ex:
    print(ex)
    print('Could not write in mysql')
    pass
  
  final_df = pd.merge(df.astype({'id': int}),  result.astype({'product_id': int}), left_on='id', right_on='product_id', how='outer')
  final_df['id'] = numpy.where(final_df.id.notnull(), final_df.id, final_df.product_id)
  final_df = final_df[final_df.id.notnull()]
  final_df = final_df.astype({'id': int})
  final_df['popularity'] = numpy.where(final_df.calculated_popularity.notnull(), final_df.calculated_popularity, final_df.popularity)
  final_df['popularity_new'] = numpy.where(final_df.calculated_popularity_new.notnull(), final_df.calculated_popularity_new, final_df.popularity_new)
  final_df.drop(['calculated_popularity', 'calculated_popularity_new', 'product_id'], axis=1, inplace=True)
  final_df = final_df.astype({'id': str})
  return final_df

child_parent_map = create_child_parent_map()
brand_popularity = get_brand_popularity()
product_creation_data = get_product_creation_data()

if __name__ == '__main__':
  print("popularity start: %s" % arrow.now())
  calculate_new_popularity()
  print("popularity end: %s" % arrow.now())
