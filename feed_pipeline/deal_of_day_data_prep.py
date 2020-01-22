import boto3
import arrow
import pandas as pd
import numpy as np
import re
import time
import sys
from decimal import ROUND_HALF_UP, Decimal
from datetime import datetime
from dateutil import tz
import requests
import json

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.deals_product_category_mapping import CATEGORY_MAP
from pipelineUtils import PipelineUtils
sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils

DAYS = -7
ENDDAYS = 0
WEIGHT_DELTA_DISCOUNT = 90
WEIGHT_DELTA_SP = 0
WEIGHT_POPULARITY = 10
SAMPLE_SIZE = 150
DECAY_0_30 = 0.9
DECAY_31_60 = 0.95
DECAY_61_100 = 0.99
FINAL_SIZE = 100
WINDOW_SIZE = 6

current_pos = 1
position_map = {}
assigned_positions = []
flag = True

from_zone = tz.gettz("UTC")
to_zone = tz.gettz("Asia/Kolkata")
current_datetime = datetime.utcnow()
current_datetime = current_datetime.replace(tzinfo=from_zone)
current_datetime = current_datetime.astimezone(to_zone)


def getPriceChangeData():
  pipeline = boto3.session.Session(profile_name='datapipeline')
  bucket_name = "nykaa-prod-autocomplete-feedback"
  s3_file_location = 'price_change_data'
  outputFileName = 'price_change_data.csv'
  startdate = arrow.now().replace(days=DAYS).strftime("%Y-%m-%d")
  enddate = arrow.now().replace(days=ENDDAYS).strftime("%Y-%m-%d")
  qstartdate = arrow.now().replace(days=-1).strftime("%Y-%m-%d")
  query = """select sku, date as dt, hour as hh, max(old_price) as old_price, min(new_price) as new_price
              from events_pds_processed
              where date >= '%s' and date < '%s'
                and event='price_changed' and old_price > 1
              group by sku, date, hour""" % (qstartdate, enddate)
  params = {
    'region': 'ap-south-1',
    'database': 'datapipeline',
    'bucket': bucket_name,
    'path': s3_file_location,
    'query': query
  }
  client = pipeline.client('athena', region_name=params["region"])

  def query_athena(params):
    response = client.start_query_execution(
      QueryString=params["query"],
      QueryExecutionContext={
        'Database': params['database']
      },
      ResultConfiguration={
        'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
      }
    )
    return response

  def pollForStatus(execution_id, max_execution=10):
    state = 'RUNNING'
    while (max_execution > 0 and state in ['RUNNING']):
      max_execution = max_execution - 1
      response = client.get_query_execution(QueryExecutionId=execution_id)
      
      if 'QueryExecution' in response and \
          'Status' in response['QueryExecution'] and \
          'State' in response['QueryExecution']['Status']:
        state = response['QueryExecution']['Status']['State']
        if state == 'FAILED':
          print("query execution failed")
          return False
        elif state == 'SUCCEEDED':
          s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
          outputFile = re.findall('.*\/(.*)', s3_path)[0]
          print("query execution successfull. File %s created" % outputFile)
          return outputFile
      time.sleep(6)
    return False

  def aggregate_price_data_on_date(price_data):
    hour_info_min = price_data.groupby(['sku', 'dt'], as_index=False).agg({'hh': 'min'})
    hour_info_min = price_data.merge(hour_info_min, on=['sku', 'dt', 'hh'])
    hour_info_min.drop(['hh', 'new_price'], axis=1, inplace=True)
  
    hour_info_max = price_data.groupby(['sku', 'dt'], as_index=False).agg({'hh': 'max'})
    hour_info_max = price_data.merge(hour_info_max, on=['sku', 'dt', 'hh'])
    hour_info_max.drop(['hh', 'old_price'], axis=1, inplace=True)
  
    final_data = hour_info_min.merge(hour_info_max, on=['sku', 'dt'])
    return final_data

  execution = query_athena(params)
  execution_id = execution['QueryExecutionId']
  outputFile = pollForStatus(execution_id, max_execution=20)
  s3 = pipeline.resource('s3')
  try:
    s3.Bucket(bucket_name).download_file(s3_file_location + '/' + outputFile, outputFileName)
  except:
    print("Unable to download file from s3")
  df = pd.read_csv(outputFileName)
  df = aggregate_price_data_on_date(df)
  
  client = MongoUtils.getClient()
  price_change_table = client['search']['price_change']
  #remove data older than 7 days
  price_change_table.remove({"dt": {"$lt": startdate}})
  timestamp = arrow.now().datetime
  for id, row in df.iterrows():
    row = dict(row)
    row['last_calculated'] = timestamp
    filt = {"sku": row["sku"], "dt": row['dt']}
    price_change_table.update(filt, row, upsert=True)
  
  data = price_change_table.find({"dt": {"$gte": startdate, "$lte": enddate}})
  final_df = pd.DataFrame(list(data))
  return final_df


def get_date_list():
  lastdate = arrow.now().replace(days=ENDDAYS, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  date = arrow.now().replace(days=DAYS, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  all_dates = []
  while date <= lastdate:
    all_dates.append(date.datetime.replace(tzinfo=None).strftime("%Y-%m-%d"))
    date = date.replace(days=1)
  return all_dates


def get_average_sp(price_data):
  price_data = price_data.sort_values(by=['sku', 'dt'], ascending=True)
  date_list = get_date_list()
  print(date_list)
  data = {'sku': [], 'dt': [], 'old_price': [], 'new_price': []}
  
  def append(sku, dt, op, np):
    # print('appending %s %s %s %s'%(sku, dt, op, np))
    data['sku'].append(sku)
    data['dt'].append(dt)
    data['old_price'].append(op)
    data['new_price'].append(np)
  
  prev_sku = ""
  date_index = 0
  current_price = None
  for i, row in price_data.iterrows():
    row = dict(row)
    if row['sku'] != prev_sku:
      if date_index != 0:
        while(date_index < len(date_list)):
          next_date = date_list[date_index]
          append(prev_sku, next_date, current_price, current_price)
          date_index = date_index + 1
        date_index = 0
        current_price = None
    next_date = date_list[date_index]
    if row['dt'] != next_date:
      while(row['dt'] != next_date):
        price = current_price if current_price else row['old_price']
        append(row['sku'], next_date, price, price)
        date_index = date_index + 1
        if date_index >= len(date_list):
          break
        next_date = date_list[date_index]
    append(row['sku'], row['dt'], row['old_price'], row['new_price'])
    current_price = row['new_price']
    prev_sku = row['sku']
    date_index = (date_index + 1)%(len(date_list))
  if date_index != 0:
    while (date_index < len(date_list)):
      next_date = date_list[date_index]
      append(prev_sku, next_date, current_price, current_price)
      date_index = date_index + 1
  price_data = pd.DataFrame.from_dict(data)
  price_data.to_csv('all_day_data.csv', index=False)
  price_data = price_data[price_data.dt != date_list[-1]]
  price_data = price_data.groupby('sku', as_index=False).agg({'new_price': 'mean'})
  price_data.rename(columns={'new_price': 'avg_sp'}, inplace=True)
  return price_data


def getProductValidity():
  query = """select sku, product_id, parent_id, mrp, sp, discount, is_in_stock, schedule_start, schedule_end,
                scheduled_discount from products where disabled=0"""
  mysql_conn = PasUtils.mysqlConnection()
  data = pd.read_sql(query, con=mysql_conn)
  mysql_conn.close()
  data.mrp = data.mrp.fillna(0)

  def calculateSP(row):
    global current_datetime
    schedule_start = row["schedule_start"]
    schedule_end = row["schedule_end"]
    scheduled_discount = row["scheduled_discount"]
    if schedule_start and schedule_end and scheduled_discount is not None:
      if current_datetime >= schedule_start.replace(tzinfo=to_zone) and \
        current_datetime < schedule_end.replace(tzinfo=to_zone):
        row["discount"] = scheduled_discount
        sp = Decimal(str(row["mrp"])) - Decimal(str(row["mrp"])) * Decimal(str(row["discount"])) / Decimal(str(100))
        row["sp"] = float(sp.quantize(0, ROUND_HALF_UP))
    return row
  
  data = data[(data.is_in_stock > 0) & (data.mrp >= 1)]
  data = data.apply(calculateSP, axis=1)
  data = data[data.discount > 0]
  data.parent_id = data.parent_id.fillna(-1)
  data.product_id = data.product_id.fillna(-1)
  data = data.astype({'product_id': int, 'parent_id': int})
  data = data.astype({'product_id': str, 'parent_id': str})
  data.drop(['is_in_stock', 'schedule_start', 'schedule_end', 'scheduled_discount'], axis=1, inplace=True)
  return data


def getPopularityData():
  client = MongoUtils.getClient()
  popularity_table = client['search']['popularity']
  cursor = popularity_table.find({}, {"id": 1, "popularity": 1, "_id": 0})
  data = pd.DataFrame(list(cursor))
  # data = pd.read_csv('pop_sh.csv')
  data.rename(columns={'id': 'product_id'}, inplace=True)
  data = data.astype({'product_id': str})
  return data
  

def getPrevDotdProducts():
  back_date_7_days = arrow.now().replace(days=DAYS, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  today = arrow.now().replace(days=ENDDAYS, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  redshift_conn = PasUtils.redshiftConnection()
  query = """select sku, datediff(day, date_time, current_date) as days, position as pos from deal_of_the_day_data where date_time>='{0}'
                and date_time <'{1}'""".format(back_date_7_days, today)
  data = pd.read_sql(query, con=redshift_conn)
  redshift_conn.close()
  data['days'] = data['days'].apply(lambda x: (DAYS * -1) - x)
  data['decay'] = data.apply(lambda x: DECAY_0_30 if x['pos'] <= 30 else DECAY_31_60 if x['pos'] <= 60 else DECAY_61_100, axis=1)
  data['decay'] = data.apply(lambda x: x['decay']**x['days'], axis=1)
  data.drop(['days', 'pos'], axis=1, inplace=True)
  data = data.groupby(['sku'], as_index=False).prod()
  return data


def populateBrandCategoryInfo(data):
  redshift_conn = PasUtils.redshiftConnection()
  query = """select sku, brand_code as brand, name as title from dim_sku"""
  brand_data = pd.read_sql(query, con=redshift_conn)
  data = pd.merge(data, brand_data, on='sku', how='left')

  query = """select distinct product_id, l3_id as category from product_category_mapping"""
  category_data = pd.read_sql(query, con=redshift_conn)
  category_data = category_data.astype({'product_id': str})
  data = pd.merge(data.astype({'product_id': str}), category_data, on='product_id', how='left')
  redshift_conn.close()
  return data
  

def addCategoryInfo(data):
  products = list(data.product_id.unique())
  products = [str(x) for x in products]
  products = ','.join(products)
  valid_categories = CATEGORY_MAP.values()
  valid_categories = [x.split(',') for x in valid_categories]
  valid_categories = [str(item) for sublist in valid_categories for item in sublist]
  valid_categories = ','.join(valid_categories)
  
  query = """select distinct product_id, cat_id as category_l1
              from (
                select distinct product_id, l1_id as cat_id from product_category_mapping
                  where l1_id in (%s) and product_id in (%s)
                union
                select distinct product_id, l2_id as cat_id from product_category_mapping
                  where l2_id in (%s) and product_id in (%s)
                union
                select distinct product_id, l3_id as cat_id from product_category_mapping
                  where l3_id in (%s) and product_id in (%s)
                union
                select distinct product_id, l4_id as cat_id from product_category_mapping
                  where l4_id in (%s) and product_id in (%s)
              )
  """%(valid_categories, products, valid_categories, products, valid_categories, products, valid_categories, products)
  print(query)

  redshift_conn = PasUtils.redshiftConnection()
  category_data = pd.read_sql(query, con=redshift_conn)
  category_data = category_data.astype({'category_l1': str})
  category_data['category_l1'] = category_data.groupby('product_id')['category_l1'].transform(lambda x: ','.join(x))
  category_data.drop_duplicates(subset=['product_id'], inplace=True)
  data = pd.merge(data.astype({'product_id': str}), category_data.astype({'product_id': str}), on='product_id')
  return data
  
  
def insertInDatabase(data):
  def assign_pos(row):
    global current_pos
    global flag
    # print(assigned_positions)
    if row['brand'] in position_map:
      #handle side-effect of apply(1st row gets processed twice)
      if flag:
        flag = False
        row['position'] = 1
        return row
      pos = position_map.get(row['brand'])
      if pos < current_pos:
        row['position'] = current_pos
        position_map[row['brand']] = min(current_pos + WINDOW_SIZE, FINAL_SIZE)
        assigned_positions.append(current_pos)
        current_pos = current_pos + 1
      else:
        if pos == FINAL_SIZE:
          row['position'] = current_pos
          assigned_positions.append(current_pos)
          current_pos = current_pos + 1
        else:
          while pos in assigned_positions:
            pos = pos + 1
          row['position'] = pos
          position_map[row['brand']] = min(pos + WINDOW_SIZE, FINAL_SIZE)
          assigned_positions.append(pos)
    else:
      row['position'] = current_pos
      assigned_positions.append(current_pos)
      position_map[row['brand']] = min(current_pos + WINDOW_SIZE, FINAL_SIZE)
      current_pos = current_pos + 1
    while current_pos in assigned_positions:
      current_pos = current_pos + 1
    # print("%s %s"%(row['title'], row['position']))
    return row
  
  def get_featured_products():
    inventories = list()
    query = {"page_type": "adhoc-data", "page_section": "deals-of-the-day", "page_data": "featured-products"}
    inventories.append(query)
    requestBody = {'inventories': inventories}
    url = 'https://' + PipelineUtils.getAdPlatformEndPoint() + '/inventory/data/json/'
    response = requests.post(url, json=requestBody, headers={'Content-Type': 'application/json'})
    if (response.ok):
      result = json.loads(response.content.decode('utf-8')).get('result')[0]
      for inventory in result['inventories']:
        if inventory['widget_data']['wtype'] == "DATA_WIDGET":
          product_id_list = inventory['widget_data']['parameters']["data_"]
          return product_id_list
    return ""
  
  data = data.apply(assign_pos, axis=1)
  data = data.sort_values(by='position')
  pinned_products = get_featured_products().split(',')
  
  #insert in mysql
  date_today = str(datetime.now().date())
  starttime = str(date_today) + ' 10:00:00'
  endtime = str(date_today) + ' 23:59:59'
  position = 0
  for pid in pinned_products:
    pid = str(pid)
    if not pid:
      continue
    query = """insert into deal_of_the_day_data (product_id, sku, starttime, endtime, position, category) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')
              on duplicate key update product_id ='{0}', sku='{1}', starttime='{2}', endtime = '{3}', category= '{5}' """. \
      format(pid, "", starttime, endtime, position, "")
    PasUtils.mysql_write(query)
    position = position + 1
  
  #delete today's data from redshift
  redshift_conn = PasUtils.redshiftConnection()
  cur = redshift_conn.cursor()
  delete_query = "delete from deal_of_the_day_data where date_time ='{0}'".format(date_today)
  cur.execute(delete_query)
  redshift_conn.commit()
  
  #insert remaining data in both mysql and redshift
  for id, row in data.iterrows():
    if position > FINAL_SIZE:
      break
    if row['product_id'] in pinned_products:
      continue
    row = dict(row)
    query = """insert into deal_of_the_day_data (product_id, sku, starttime, endtime, position, category) values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')
                  on duplicate key update product_id ='{0}', sku='{1}', starttime='{2}', endtime = '{3}', category = '{5}' """. \
      format(row['product_id'], row['sku'], starttime, endtime, position, row['category_l1'])
    PasUtils.mysql_write(query)

    query = "insert into deal_of_the_day_data values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')".format(
      row['product_id'], row['sku'], position, date_today, "", row['sp'], row['discount'])
    cur.execute(query)
    position = position + 1
  redshift_conn.commit()
  return data
 

def get_valid_ids():
  query = """select distinct product_id from product_category_mapping
              where l1_id in (8377, 12, 24, 9564)"""
  redshift_conn = PasUtils.redshiftConnection()
  category_data = pd.read_sql(query, con=redshift_conn)
  category_data = category_data.astype({'product_id': str})
  return category_data


def getBestDeals():
  price_data = getPriceChangeData()
  price_data = get_average_sp(price_data)
  validity = getProductValidity()
  valid_products = pd.merge(price_data, validity, on='sku')
  valid_products['delta_sp'] = valid_products.apply(lambda x: x['avg_sp'] - x['sp'], axis=1)
  valid_products = valid_products[(valid_products.delta_sp > 0) & (valid_products.avg_sp < valid_products.mrp) & (valid_products.mrp > 100)]
  popularity_data = getPopularityData()
  valid_products = pd.merge(valid_products, popularity_data, on='product_id')
  valid_products['delta_discount'] = valid_products.apply(lambda x: (x['delta_sp']*100)/x['mrp'], axis=1)
  valid_products['score'] = valid_products.apply(lambda x: (x['delta_sp']*WEIGHT_DELTA_SP) + (x['delta_discount']*WEIGHT_DELTA_DISCOUNT)
                                                           + (x['popularity']*WEIGHT_POPULARITY), axis=1)
  valid_products = valid_products.sort_values(by='score', ascending=False)
  valid_products = valid_products.drop_duplicates('parent_id', keep='first')
  valid_data = get_valid_ids()
  valid_products = pd.merge(valid_products, valid_data, on="product_id")
  # valid_products = valid_products[:SAMPLE_SIZE]
  prev_products = getPrevDotdProducts()
  valid_products = pd.merge(valid_products, prev_products, on='sku', how='left')
  valid_products.decay = valid_products.decay.fillna(1)
  valid_products['score'] = valid_products.apply(lambda x: x['score']*x['decay'], axis=1)
  valid_products = populateBrandCategoryInfo(valid_products)
  valid_products = valid_products.sort_values(by='score', ascending=False)
  valid_products = valid_products[np.isfinite(valid_products['category'])]
  valid_products = valid_products.drop_duplicates(['sku'], keep='first')
  valid_products = valid_products.drop_duplicates(['brand', 'category'], keep='first')
  valid_products = valid_products[:SAMPLE_SIZE]
  valid_products = addCategoryInfo(valid_products)
  valid_products = valid_products.sort_values(by='popularity', ascending=False)
  valid_products = valid_products[:FINAL_SIZE]
  valid_products = insertInDatabase(valid_products)
  print(valid_products.head(5))
  return

getBestDeals()