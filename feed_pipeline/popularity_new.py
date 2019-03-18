import sys
import pandas as pd
import arrow
import numpy
import datetime
import json
import requests
from pipelineUtils import PipelineUtils

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter

client = Utils.mongoClient()
processed_data = client['search']['processed_data']
popularity_table = client['search']['popularity']
order_data = client['search']['order_data']

POPULARITY_DECAY_FACTOR=0.5
WEIGHT_VIEWS = 10
WEIGHT_UNITS = 0
WEIGHT_ORDERS = 30
WEIGHT_CART_ADDITIONS = 10
WEIGHT_REVENUE = 60
POPULARITY_TOTAL_RATIO = 0.1
POPULARITY_BUCKET_RATIO = 0.9
PUNISH_FACTOR=0.7
BOOST_FACTOR=1.1
PRODUCT_PUNISH_FACTOR = 0.5
COLD_START_DECAY_FACTOR = 0.95

POPULARITY_DECAY_FACTOR_NEW = 0.5
WEIGHT_VIEWS_NEW = 10
WEIGHT_UNITS_NEW = 0
WEIGHT_ORDERS_NEW = 30
WEIGHT_CART_ADDITIONS_NEW = 10
WEIGHT_REVENUE_NEW = 60
POPULARITY_TOTAL_RATIO_NEW = 0.1
POPULARITY_BUCKET_RATIO_NEW = 0.9
PUNISH_FACTOR_NEW=0.7
BOOST_FACTOR_NEW=1.1
PRODUCT_PUNISH_FACTOR_NEW = 0.5
COLD_START_DECAY_FACTOR_NEW = 0.95

BRAND_PROMOTION_LIST = ['1937', '13754', '7666', '71596']
PRODUCT_PUNISH_LIST = [303813,262768,262770,262769]
PRODUCT_POPULARITY_OVERRIDES =  { "417918": 60,
                                  "417919": 56,
                                  "417921": 55,
                                  "417926": 54,
                                  "417920": 52,
                                  "417925": 51,
                                  "417923": 49,
                                  "417922": 48,
                                  "417928": 45,
                                  "417924": 43,
                                  "37894": 60,
                                  "417927": 40
                                }


def build_product_sales_map(startdate='2018-04-01'):
  global product_sales_map
  
  where_clause = "orderdetail_dt_created >='{0}'".format(startdate)
  query = """select product_id, sum(OrderDetail_QTY) as qty_ordered from fact_order_detail_new  where {0}  group by product_id""".format(
    where_clause)
  print(query)
  redshift_conn = Utils.redshiftConnection()
  cur = redshift_conn.cursor()
  
  cur.execute(query)
  
  for row in cur.fetchall():
    product_sales_map[str(row[0])] = float(row[1])
    

def create_child_sales_map():
  startdate = arrow.now().replace(days=-31).datetime.replace(tzinfo=None)
  enddate = arrow.now().replace(days=1).datetime.replace(tzinfo=None)
  child_parent_ratio = get_child_distribution_ratio(startdate, enddate)

  query = """select product_id, parent_id, is_in_stock, mrp, disabled
                from products"""
  mysql_conn = Utils.mysqlConnection()
  data = pd.read_sql(query, con=mysql_conn)
  mysql_conn.close()
  data.mrp = data.mrp.fillna(0)
  data = data[(data.is_in_stock == 0) | (data.disabled == 1) | (data.mrp < 1)]
  data['valid'] = 0
  data = data.astype({'product_id': str, 'parent_id': str})
  data.drop(['is_in_stock', 'mrp', 'disabled'], axis=1, inplace=True)

  child_parent_ratio = pd.merge(child_parent_ratio, data, how='left', on=['product_id', 'parent_id'])
  child_parent_ratio.valid = child_parent_ratio.valid.fillna(1)
  return child_parent_ratio
  

def create_child_parent_map():
  query = """SELECT cpsl.product_id, cpsl.parent_id
              FROM catalog_product_entity e
	            JOIN catalog_product_super_link cpsl ON e.entity_id = cpsl.product_id
              JOIN catalog_product_entity_int cpei ON cpei.entity_id = cpsl.parent_id
              WHERE e.type_id = 'simple' and cpei.attribute_id = 80 AND cpei.value = 1
              GROUP BY cpsl.product_id;"""
  nykaa_conn = Utils.nykaaMysqlConnection()
  child_parent_map = pd.read_sql(query, con=nykaa_conn)
  child_parent_map = child_parent_map.astype({'parent_id': str, 'product_id': str})
  return child_parent_map
  

def get_parent_child_list():
  global child_parent_map
  parent_map = {}
  for i, row in child_parent_map.iterrows():
    row = dict(row)
    plist = parent_map.get(row['parent_id'], [])
    plist.append(row['product_id'])
    parent_map[row['parent_id']] = plist
  return parent_map
  

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
  return df, True


def get_order_data(startdate, enddate):
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
  print(parent)
  
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
  print(child)

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
  return df
  

def calculate_popularity():
  global child_parent_map
  
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
  
  #business_logic
  a = applyBoost(a)
  a = handleColdStart(a)
  a = applyOffers(a)
  a.rename(columns={'popularity_new': 'popularity_recent'}, inplace=True)
  a = a.sort_values(by='popularity', ascending=True)
  a.to_csv('a.csv', index=False)
  
  parent_child_list = get_parent_child_list()
  ctr = LoopCounter(name='Writing popularity to db', total=len(a.index))
  for i, row in a.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)

    row = dict(row)

    child_product_list = parent_child_list.get(row['id'], [])
    if len(child_product_list) > 0:
      popularity_multiplier_factor = get_popularity_multiplier(row['id'], child_product_list)
    else:
      popularity_multiplier_factor = 1

    row['last_calculated'] = timestamp
    row['popularity_multiplier_factor'] =  popularity_multiplier_factor
    row['popularity'] = row['popularity']* float(popularity_multiplier_factor)
    row['popularity_recent'] = row['popularity_recent']* float(popularity_multiplier_factor)

    id = row.get('id')
    popularity_table.update({"_id": id}, row, upsert=True)

  popularity_table.remove({"last_calculated": {"$ne": timestamp}})
  override_popularity()
  

def applyBoost(df):
  query = """select product_id, sku_type, brand_code, mrp, l3_id from dim_sku"""
  print(query)
  redshift_conn = Utils.redshiftConnection()
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
  temp_df = temp_df.apply(punish_products_by_id, axis=1)

  temp_df.drop(['product_id', 'sku_type', 'brand_code', 'mrp', 'l3_id'], axis=1, inplace=True)
  temp_df = temp_df.astype({'id': str})

  return temp_df


def handleColdStart(df):
  temp_df = df[['id', 'popularity', 'popularity_new']]
  temp_df = temp_df.astype({'id': int, 'popularity': float, 'popularity_new': float})

  query = """select product_id, l3_id from product_category_mapping"""
  redshift_conn = Utils.redshiftConnection()
  product_category_mapping = pd.read_sql(query, con=redshift_conn)

  product_data = pd.merge(temp_df, product_category_mapping, left_on=['id'], right_on=['product_id'])

  def percentile(n):
      def _percentile(x):
          return numpy.percentile(x, n)
      return _percentile
  category_popularity = product_data.groupby('l3_id').agg({'popularity': percentile(95), 'popularity_new': percentile(95)}).reset_index()

  product_data = pd.merge(product_category_mapping, category_popularity, on='l3_id')
  product_popularity = product_data.groupby('product_id').agg({'popularity': 'max', 'popularity_new': 'max'}).reset_index()
  product_popularity.rename(columns={'popularity': 'median_popularity', 'popularity_new': 'median_popularity_new'}, inplace=True)
  result = pd.merge(temp_df, product_popularity, left_on='id', right_on='product_id')

  query = """select product_id, sku_created from dim_sku where sku_type != 'bundle' and sku_created > dateadd(day,-60,current_date)"""
  product_creation = pd.read_sql(query, con=redshift_conn)
  redshift_conn.close()

  result = pd.merge(result, product_creation, on='product_id')

  def calculate_new_popularity(row):
    date_diff = abs(datetime.datetime.utcnow() - (numpy.datetime64(row['sku_created']).astype(datetime.datetime))).days
    if date_diff > 0:
        row['calculated_popularity'] = row['popularity'] + row['median_popularity']*(COLD_START_DECAY_FACTOR ** date_diff)
        row['calculated_popularity_new'] = row['popularity_new'] + row['median_popularity_new'] * (COLD_START_DECAY_FACTOR_NEW ** date_diff)
    else:
        row['calculated_popularity'] = row['popularity']
        row['calculated_popularity_new'] = row['popularity_new']
    return row

  result['calculated_popularity'] = 0
  result['calculated_popularity_new'] = 0
  result = result.apply(calculate_new_popularity, axis=1)

  result = result[['product_id', 'calculated_popularity', 'calculated_popularity_new']]
  final_df = pd.merge(df.astype({'id': int}), result.astype({'product_id': int}), left_on='id',
                      right_on='product_id', how='left')
  # final_df['calculated_popularity'] = final_df.calculated_popularity.fillna(-1)
  final_df['popularity'] = numpy.where(final_df.calculated_popularity.notnull(), final_df.calculated_popularity, final_df.popularity)
  final_df['popularity_new'] = numpy.where(final_df.calculated_popularity_new.notnull(), final_df.calculated_popularity_new, final_df.popularity_new)
  final_df.drop(['calculated_popularity', 'calculated_popularity_new', 'product_id'], axis=1, inplace=True)
  final_df = final_df.astype({'id': str})
  return final_df


def applyOffers(df):
  start_date = datetime.datetime.now() + datetime.timedelta(minutes=330)
  end_date = start_date + datetime.timedelta(days=1)
  start_date = start_date.strftime('%Y-%m-%d 06:00:00')
  end_date = end_date.strftime('%Y-%m-%d')
  conn = Utils.nykaaMysqlConnection(force_production=True)

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
  df = pd.merge(df, product_offer_mapping, how='left', left_on=['id'], right_on=['product_id'])
  df.valid = df.valid.fillna(False)

  def calculate_new_popularity(row):
    if row['valid']:
      row['popularity_new'] = row['popularity_new'] + (row['popularity_new']/10)
    return row
  df = df.apply(calculate_new_popularity, axis=1)
  df.drop(['valid', 'product_id', 'offer_ids'], axis=1, inplace=True)
  return df


def get_popularity_multiplier(parent_id, product_list):
  global child_parent_sales_map
  product_list = list(dict.fromkeys(product_list))
  popularity_multiplier = 1
  for p in product_list:
    try:
      isvalid = child_parent_sales_map[(child_parent_sales_map.parent_id == str(parent_id)) & (child_parent_sales_map.product_id == str(p))]['valid']
      if not isvalid:
        popularity_multiplier -= float(child_parent_sales_map[(child_parent_sales_map.parent_id == str(parent_id))
                                                            & (child_parent_sales_map.product_id == str(p))]['ratio'])
    except:
      pass
  return max(popularity_multiplier, 0)
  

def override_popularity():
  for id, popularity in PRODUCT_POPULARITY_OVERRIDES.items():
    popularity_table.update({"_id": id}, {"$set": {'popularity': popularity}})
    
child_parent_map = create_child_parent_map()
child_parent_sales_map = create_child_sales_map()

if __name__ == '__main__':
  print("popularity start: %s" % arrow.now())
  calculate_popularity()
  print("popularity end: %s" % arrow.now())
