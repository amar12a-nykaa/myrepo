import boto3
import arrow
import pandas as pd
import re
import time
import sys

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils

DAYS = -7
WEIGHT_DELTA_DISCOUNT = 90
WEIGHT_DELTA_SP = 3
WEIGHT_POPULARITY = 10
SAMPLE_SIZE = 500

def getPriceChangeData():
  pipeline = boto3.session.Session(profile_name='datapipeline')
  bucket_name = "nykaa-prod-autocomplete-feedback"
  s3_file_location = 'price_change_data'
  outputFileName = 'price_change_data.csv'
  startdate = arrow.now().replace(days=DAYS).strftime("%Y-%m-%d")
  enddate = arrow.now().replace(days=0).strftime("%Y-%m-%d")
  query = """select sku, dt, hh, max(old_price) as old_price, min(new_price) as new_price
              from events_pds
              where dt >= '%s' and dt < '%s'
                and event='price_changed' and product_type='simple'
              group by sku, dt, hh""" % (startdate, enddate)
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

  execution = query_athena(params)
  execution_id = execution['QueryExecutionId']
  outputFile = pollForStatus(execution_id, max_execution=20)
  s3 = pipeline.resource('s3')
  try:
    s3.Bucket(bucket_name).download_file(s3_file_location + '/' + outputFile, outputFileName)
  except:
    print("Unable to download file from s3")
  df = pd.read_csv(outputFileName)
  return df


def aggregate_price_data_on_date(price_data):
  hour_info_min = price_data.groupby(['sku', 'dt'], as_index=False).agg({'hh': 'min'})
  hour_info_min = price_data.merge(hour_info_min, on=['sku', 'dt', 'hh'])
  hour_info_min.drop(['hh', 'new_price'], axis=1, inplace=True)

  hour_info_max = price_data.groupby(['sku', 'dt'], as_index=False).agg({'hh': 'max'})
  hour_info_max = price_data.merge(hour_info_max, on=['sku', 'dt', 'hh'])
  hour_info_max.drop(['hh', 'old_price'], axis=1, inplace=True)
  
  final_data = hour_info_min.merge(hour_info_max, on=['sku', 'dt'])
  return final_data


def get_date_list():
  lastdate = arrow.now().replace(days=0, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
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
  query = """select sku, product_id, parent_id, mrp, sp, discount, is_in_stock, disabled from products"""
  mysql_conn = PasUtils.mysqlConnection()
  data = pd.read_sql(query, con=mysql_conn)
  mysql_conn.close()
  data.mrp = data.mrp.fillna(0)
  
  data = data[(data.is_in_stock > 0) & (data.disabled != 1) & (data.mrp >= 1) & (data.discount > 0)]
  data.parent_id = data.parent_id.fillna(-1)
  data.product_id = data.product_id.fillna(-1)
  data = data.astype({'product_id': int, 'parent_id': int})
  data = data.astype({'product_id': str, 'parent_id': str})
  data.drop(['is_in_stock', 'disabled'], axis=1, inplace=True)
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
  

def getBestDeals():
  price_data = getPriceChangeData()
  price_data = aggregate_price_data_on_date(price_data)
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
  valid_products = valid_products[:SAMPLE_SIZE]
  # valid_products.to_csv('data_.csv', index=False)
  return

getBestDeals()