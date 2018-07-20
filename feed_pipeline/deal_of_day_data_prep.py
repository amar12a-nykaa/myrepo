import sys
import pandas as pd
from adobe_analytics import Client,ReportDefinition
import psycopg2
import numpy as np
import json
import requests
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import re
from datetime import datetime, timedelta
sys.path.append("/nykaa/api")
from pipelineUtils import PipelineUtils
from pas.v2.utils import Utils

quantile_lower_limit = .25
quantile_upper_limit = .75
quantile_mid_limit = .50
min_discount = 10
max_discount = 50
DAILY_CATEGORY_LIMIT = 5
DAILY_BRAND_LIMIT = 3
DAILY_CATEGORY_BRAND_LIMIT = 1
WEEKLY_CATEGORY_LIMIT = 9000
WEEKLY_BRAND_LIMIT = 9000
MAX_INT = 100000000

cat3_ids = ['233','234','228','231','4140','229','235','236','232','237','6761','239','687','240','241','242','245','1995','247','931','244','2440','1513','1514','4036','249','263','5079','250','251','252','253','254','255','256','257','259','260','3277','267','269','268','270','266','271','272','273','274','947','4037','283','282','222','224','284','285','933','227','5486','2063','521','1649','376','691','380','379','372','1650','1644','2084','2069','288','536','2080','289','287','226','281','225','2065','2066','2067','2068','632','286','316','317','319','2040','320','2041','2048','361','362','364','363','2049','2444','2043','332','331','329','346','1214','1222','2046','2746','2045','7305','2089','525','583','545','550','367','551','370','1495','554','555','2458','6168','2110','572','3859','2092','364','1543','1399','1400','1401','1404','1405','41','1517','43','44','368','369','367','370','316','319','691','374','376','283','224','584','385','2084','382','1650','1644','975','2057','1673','1676','60','746','1666','1675','1546','4874','1664','636','637','635','638','1274','430','6919','6917','6922', '223', '1411', '1650','962']

global back_date_7_days  ##default
global date_today 
daily_categorywise_limits = {}
weekly_categorywise_limits = {}

daily_brandwise_limits = {}
weekly_brandwise_limits = {}

def load_category_limits():
  global daily_categorywise_limits
  global weekly_categorywise_limits

  with open('category_limits', r) as fp:

    for line in fp:
      d =  line.strip().split()
      if len(d)<3 :
        continue
      category =  d[0]
      dl =  d[1]
      wl = d[2]

      daily_categorywise_limits[category] = dl
      weekly_categorywise_limits[category] = wl
def load_brand_limits():
  global daily_brandwise_limits
  global weekly_brandwise_limits

  with open('brand_limits', r) as fp: 
    for line in fp:
      d =  line.strip().split()
      if len(d)<3 :
        continue
      brand =  d[0]
      dl =  d[1]
      wl = d[2]

      daily_brandwise_limits[brand] = dl
      weekly_brandwise_limits[brand] = wl

client = Utils.mongoClient()
deal_of_the_day_table = client['search']['deal_of_the_day']

client = Client('harsh.karnawat:FSN E-Commerce', '98b91427ba974dd0268a14e789922b53')
suite = client.suites()['fsnecommercemobileappprod']

"""Intent Set"""
report_def = ReportDefinition(
         dimensions = [{
        "id": "product" , "top" : 15000
        
        }],
        metrics=[
            "cm200000815_5b05121841c44bb4bea94cec",
            "cartadditions",
            "orders"
        ],
        last_days = 7
        )

def get_intent_set():    
  report = suite.download(report_def)
  report.columns = ['entity_id' , 'pv' , 'ca' , 'o']
  report[['pv','ca','o']] = report[['pv','ca','o']].apply(pd.to_numeric)
  
  report['order_conversion_flag'] = ((( report['o'] / report['ca']) >=  (( report['o'] / report['ca']).quantile(0.2) ) ) & 
        ( ( report['o'] / report['ca']) <=  (( report['o'] / report['ca']).quantile(quantile_upper_limit) ) ) )
  
  report['cart_conversion_flag'] = ( ( ( report['ca'] / report['pv']) >=  (( report['ca'] / report['pv']).quantile(quantile_lower_limit) ) ) & 
      ((report['ca'] / report['pv']) <=  (( report['ca'] / report['pv']).quantile(quantile_upper_limit) )))
  report['flag'] = report['order_conversion_flag'] | report['cart_conversion_flag'] 
  intent_set = pd.DataFrame(report[report['flag']==True]['entity_id'])
  intent_set['entity_id'] = pd.to_numeric(intent_set['entity_id'])
  return intent_set

def get_rating_set():
  global back_date_7_days
  redshift_conn =  Utils.redshiftConnection()
  query = """select product_sku, product_id, rating_star, date(created_at), status_code from reviews_qna where status_code in ('Approved','Pending')  and created_at >='{0}'""".format(back_date_7_days)

  rating_raw  =  pd.read_sql(query, redshift_conn)
  temp = rating_raw.groupby(['product_sku','product_id'])['rating_star'].agg(['count',np.average]).reset_index()
  temp.columns = ['sku' , 'entity_id' , 'count' , 'avg']
  temp['flag'] = ( temp['count'] >= temp['count'].quantile(quantile_lower_limit)) 
  rating_set = pd.DataFrame(temp[temp['flag']==True]['entity_id'])
  rating_set['entity_id'] = pd.to_numeric(rating_set['entity_id'])
  return rating_set 

def get_sales_data():
  global back_date_7_days
  
  redshift_conn =  Utils.redshiftConnection()
  query  = """select ds.product_id as entity_id , count( distinct fodn.nykaa_orderno) from fact_order_detail_new fodn
  left join fact_order_new fon on fon.nykaa_orderno = fodn.nykaa_orderno
  left join dim_sku ds on ds.sku = fodn.product_sku  
  where fon.order_date >= '{0}'
  and fon.order_date < '{1}'
  and fon.eretail_orderno is not null
  and fon.eretail_orderno not in ('')
  and ds.product_id is not null
  group by 1;""".format(back_date_7_days, date_today)
 
  product_sales_data =  pd.read_sql(query, redshift_conn)
  product_sales_data['flag'] = (product_sales_data['count'] >= product_sales_data['count'].quantile(quantile_mid_limit))
  product_sales_data_set =  pd.DataFrame(product_sales_data[product_sales_data['flag'] == True]['entity_id'])
  product_sales_data_set['entity_id'] =  pd.to_numeric(product_sales_data_set['entity_id'])

  return product_sales_data_set

def get_product_sku_data():
  redshift_conn =  Utils.redshiftConnection()
  product_sku_query = """select  distinct sku, sku_type, product_id from dim_sku  where sku is not NULL and sku_type is not Null and product_id is not NULL"""

  product_sku_raw_data = pd.read_sql(product_sku_query, redshift_conn)
  product_sku_raw_data.columns = ['sku', 'sku_type', 'product_id']

  product_id_sku_details_map = {}
  sku_details_map = {}
  
  for index, row in product_sku_raw_data.iterrows():
    product_id_sku_details_map[str(row['product_id'])] =  {"sku": row['sku'], "type": (row['sku_type']).lower()}
    sku_details_map[str(row['sku'])] = {"sku": row["sku"], "type": (row['sku_type']).lower(), 'product_id': row['product_id']}
  return product_id_sku_details_map, sku_details_map 


def get_pas_data(product_details):
  request_url = "http://" + PipelineUtils.getAPIHost() + "/apis/v2/pas.get"
  #print (request_url)
  request_data = json.dumps({'products': product_details}).encode('utf-8')
  #print (json.loads(request_data.decode('utf-8')))

  req = Request(request_url, data = request_data, headers = {'content-type': 'application/json'})
  pas_object = json.loads(urlopen(req).read().decode('utf-8'))
  pas_object = pas_object['skus']

  return pas_object

def get_product_detail(product_id):
  request_url = "http://" + PipelineUtils.getAPIHost() + "/apis/v2/product.list?id={0}".format(product_id)
  r =  requests.get(request_url, headers = {'content-type': 'application/json'})
  req_fields = ['category_levels', 'brand_name', 'brand_ids', 'is_luxe']
  data = (json.loads(r.content.decode('utf-8')).get('result'))
  row  = {}
  for f in req_fields:
    if data.get(f):
      row[f] = data.get(f)
  return row

def helper_function(weekly_category_wise_count_map, weekly_category_brand_count_map, weekly_brand_count_map, docs, already_dotd_products):
  new_docs  = sorted(docs, key=lambda x: float(x.get('discount', 0)), reverse=True)

  daily_category_count_map = {}
  daily_brand_count_map = {}
  daily_category_brand_count_map = {}

  final_list = []

  for doc in new_docs:
    if doc.get('product_id'): 
      if (already_dotd_products.get(str(doc.get('product_id')))):
        continue
      product_id  = doc.get('product_id')
    else:
      continue

    l_list = []
    l1 = None
    l2 = None
    l3 = None
    brand_ids = None

    if doc.get('category_levels'):
      cat_levels =  doc.get('category_levels')

      for key, value in cat_levels.items():
        if value == "2":
          l3 =  int(key)
      if str(l3) not in cat3_ids:
        continue

      if l3:
        l_list.append(l3)
      else:
        l_list.append('UNKNOWN')

    if doc.get('brand_ids'):
      brand_ids = doc.get('brand_ids')
    else:
      brand_ids =  'UNKNOWN'

    
    cat_brand = None
    if l3 and brand_ids:
      cat_brand =  str(l3)+ str(brand_ids)
    
    is_eligible = True
    for l in l_list:
      if ((daily_category_count_map.get(l, 0) >= DAILY_CATEGORY_LIMIT) and (daily_category_count_map.get(l,0 ) >=daily_categorywise_limits.get(l,0))) or ((weekly_category_wise_count_map.get(l, 0) >= WEEKLY_CATEGORY_LIMIT) and (weekly_category_wise_count_map.get(l, 0)>= weekly_categorywise_limits.get(l,0))):
        is_eligible =  False
    if brand_ids and ((daily_brand_count_map.get(brand_ids, 0) >= DAILY_BRAND_LIMIT and (daily_brand_count_map.get(brand_ids, 0)>= daily_brandwise_limits.get(brand_ids, 0))) or(weekly_brand_count_map.get(brand_ids, 0) >= WEEKLY_BRAND_LIMIT and weekly_brand_count_map.get(brand_ids,0) >= weekly_brandwise_limits.get(brand_ids, 0))):
      is_eligible = False
    if cat_brand  and daily_category_brand_count_map.get(cat_brand, 0) >= DAILY_CATEGORY_BRAND_LIMIT:
      is_eligible =  False
      
    if is_eligible:
      for l in l_list:
        if daily_category_count_map.get(l):
          daily_category_count_map[l] +=1
        else:
          daily_category_count_map[l] =1
      if brand_ids:
        if daily_brand_count_map.get(brand_ids):
          daily_brand_count_map[brand_ids] += 1
        else:
          daily_brand_count_map[brand_ids] = 1
      if daily_category_brand_count_map.get(cat_brand):
        daily_category_brand_count_map[cat_brand] += 1
      else:
        daily_category_brand_count_map[cat_brand] = 1
      final_list.append(doc)
  return final_list

def get_dotd_products(dt):
  redshift_conn = Utils.redshiftConnection()
  query =  "select distinct product_id,sku from deal_of_the_day_data where date_time >={0} and date_time <'{1}'".format( back_date_7_days, dt)
  cur  = redshift_conn.cursor()
  cur.execute(query)

  product_list = []
  for row in cur.fetchall():
    product_id = row[0]
    sku =  row[1]
    prod_det = get_product_detail(product_id)
    row  = {}
    row['product_id'] =  product_id
    row['sku'] =  sku

    for key, value in prod_det.items():
      row[key] =  value
    product_list.append(row)

  return product_list


def get_final_eligible_product_list(docs, dt):

  all_data = get_dotd_products(dt)
  already_dotd_products = {}
  for row in all_data:
    if row.get('product_id'):
      already_dotd_products[row.get('product_id')] = 1

  weekly_category_wise_count_map = {}
  weekly_category_brand_count_map = {}
  weekly_brand_count_map = {}
  
  for d in all_data:
    l_list = []
    l1 = None
    l2 = None
    l3 = None
    brand_ids = None
    
    if d.get('category_levels'):
      cat_levels =  d.get('category_levels')

      for key, value in cat_levels.items():
        if value == "2":
          l3 =  int(key)

      #if l1:
      #  l_list.append(l1)
      #if l2: 
      #  l_list.append(l2)
      if l3:
        l_list.append(l3)

      for l in l_list:
        if weekly_category_wise_count_map.get(l):
          weekly_category_wise_count_map[l] +=1
        else:
          weekly_category_wise_count_map[l] =1
    
    if d.get('brand_ids'):
      brand_ids = d.get('brand_ids')
      if weekly_brand_count_map.get(brand_ids):
        weekly_brand_count_map[brand_ids] += 1
      else:
        weekly_brand_count_map[brand_ids] = 1
      
      for l in l_list:
        cat_brand =  str(l)+ str(brand_ids)
        if weekly_category_brand_count_map.get(cat_brand):
          weekly_category_brand_count_map[cat_brand] +=1
        else:
          weekly_category_brand_count_map[cat_brand] = 1


  final_list = helper_function(weekly_category_wise_count_map, weekly_category_brand_count_map, weekly_brand_count_map, docs, already_dotd_products)
  return final_list

def dump_to_redshift(docs, dt):
  redshift_conn = Utils.redshiftConnection()

  cur =  redshift_conn.cursor()
  delete_query  =  "delete from deal_of_the_day_data where date_time ='{0}'".format(dt)
  cur.execute(delete_query)
  redshift_conn.commit()

  for counter, doc in enumerate(new_docs[:30]):
    if doc.get('product_id'):
      product_id =  doc.get('product_id')
      sku =  doc.get('sku')
      position =  counter
      query  =  "insert into deal_of_the_day_data values ('{0}', '{1}', '{2}', '{3}')".format(product_id, sku, position, dt)

      cur.execute(query)
  redshift_conn.commit()

if __name__ == '__main__':
  global back_date_7_days
  global date_today
  dt = str(datetime.now().date())
  back_date_7_days = str((datetime.now()-timedelta(days=8)).date())
  date_today =dt

  intent_set =  get_intent_set()
  rating_set =  get_rating_set()
  sales_set =  get_sales_data()
  product_id_sku_details_map, sku_details_map =  get_product_sku_data()

  #print( len( (intent_set.append(rating_set.append(sales_set))).reset_index() ))
  #all_selected_product =  list((intent_set.append(rating_set.append(sales_set))).reset_index()['entity_id'].unique())
  all_selected_product = []
  for index, row in rating_set.iterrows():
    all_selected_product.append(row['entity_id'])

  for index, row in intent_set.iterrows():
    all_selected_product.append(row['entity_id'])

  for index, row in sales_set.iterrows():
    all_selected_product.append(row['entity_id'])

  product_list =  list(set(all_selected_product))

  product_details = []
  for product in product_list:
    if product_id_sku_details_map.get(str(product)):
      product_details.append(product_id_sku_details_map.get(str(product)))

  pas_object =  get_pas_data(product_details)

  docs = []
  for p in product_details:
    sku =  str(p['sku'])
    if pas_object.get(sku) and sku_details_map.get(sku):
      pas_data = pas_object[sku]
      sku_details =  sku_details_map[sku]
  
      row = {}
      for key, value in pas_data.items():
        row[key] =  value

      for key, value in sku_details.items():
        row[key] = value
    
      if row.get("is_in_stock", 0) and (not row.get('disabled', False)) and row.get('discount', 0)>=min_discount and row.get('discount', 0)<= max_discount and row.get('product_id'):
        p_details = get_product_detail(row.get('product_id'))
        for key, value in p_details.items():
          row[key] = value
        docs.append(row)

  new_docs=  sorted(docs, key=lambda x: float(x.get('discount', 0)), reverse=True)

  new_docs = get_final_eligible_product_list(new_docs, dt)

  #csv_data  = []
  #for counter, doc in enumerate(new_docs):
  #  row  = []
  #  row  = [doc.get('product_id'), doc.get('discount'), doc.get('sku')]
  #  csv_data.append(row)

  #myfile  =  open('discount_data.csv', 'w')

  #import csv
  #with myfile:
  #  writer = csv.writer(myfile)
  #  writer.writerows(csv_data)

  #for counter, doc in enumerate(new_docs[:30]):
  #  if doc.get('product_id'):
  #    doc['position'] = counter
  #    doc['_id'] = counter
  #    deal_of_the_day_table.replace_one({"_id": counter},doc, upsert=True)
  
  dump_to_redshift(new_docs[:30], dt)
