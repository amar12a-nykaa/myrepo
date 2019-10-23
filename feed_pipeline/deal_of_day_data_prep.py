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
sys.path.append("/var/www/pds_api")
from pipelineUtils import PipelineUtils
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
from popularity_api import get_popularity_for_id, validate_popularity_data_health

quantile_lower_limit = .25
quantile_upper_limit = .75
quantile_mid_limit = .50
min_discount = 25
max_discount = 60
DAILY_CATEGORY_LIMIT = 5
DAILY_BRAND_LIMIT = 3
DAILY_CATEGORY_BRAND_LIMIT = 1
WEEKLY_CATEGORY_LIMIT = 1500
WEEKLY_BRAND_LIMIT = 9
MAX_INT = 100000000

cat3_ids = ['233','234','228','231','4140','229','235','236','232','237','6761','239','687','240','241','242','245','1995','247','931','244','2440','1513','1514','4036','249','263','5079','250','251','252','253','254','255','256','257','259','260','3277','267','269','268','270','266','271','272','4037','283','282','222','224','284','285','933','227','5486','2063','521','1649','376','691','380','379','372','1650','1644','2084','2069','288','536','2080','289','287','226','281','225','2065','2066','2067','2068','632','286','316','317','319','2040','320','2041','364','363','2444','2043','332','331','329','346','1214','1222','2046','2746','2045','7305','2089','525','583','545','550','367','551','370','1495','554','555','572','3859','2092','364','1543','1399','1400','1401','1404','1405','41','1517','43','44','368','369','367','370','316','319','691','374','376','283','224','2084','1650','1644','975','2057','1673','1676','1675','1546','4874','1664','636','637','635','638','1274','6919','6917','6922', '223', '1411', '1650','962','1392','1388','1387','377','371', '527','547','1393', '979', '1323','7336', '971', '539', '571', '531', '1301', '529', '7313', '541', '1302','7306','524','526', '528','532', '533', '530', '544', '549', '552', '2108', '2109', '2111', '1559', '2096', '2107','581', '535', '2095', '574', '573', '5746']

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
  redshift_conn =  PasUtils.redshiftConnection()
  print (back_date_7_days)
  try:
    query = """select product_sku, product_id, rating_star, date(created_at), status_code from reviews_qna where status_code in ('Approved','Pending')  and created_at >='{0}'""".format(back_date_7_days)

    rating_raw  =  pd.read_sql(query, redshift_conn)
    temp = rating_raw.groupby(['product_sku','product_id'])['rating_star'].agg(['count',np.average]).reset_index()
    temp.columns = ['sku' , 'entity_id' , 'count' , 'avg']
    temp['flag'] = ( temp['count'] >= temp['count'].quantile(quantile_lower_limit)) 
    rating_set = pd.DataFrame(temp[temp['flag']==True]['entity_id'])
    rating_set['entity_id'] = pd.to_numeric(rating_set['entity_id'])
    return rating_set 
  except:
    print ("NOT ABLE TO GET RATING DATA")
  return pd.DataFrame()

def get_sales_data():
  global back_date_7_days
  
  redshift_conn =  PasUtils.redshiftConnection()
  query  = """select ds.product_id as entity_id , count( distinct fodn.nykaa_orderno) from fact_order_detail_new fodn
  left join fact_order_view fon on fon.nykaa_orderno = fodn.nykaa_orderno
  left join dim_sku ds on ds.sku = fodn.product_sku  
  where fon.order_date >= '{0}'
  and fon.order_date < '{1}'
  and fon.eretail_orderno is not null
  and fon.eretail_orderno not in ('')
  and ds.product_id is not null
  group by 1;""".format(back_date_7_days, date_today)
  product_sales_data =  pd.read_sql(query, redshift_conn)
  product_sales_data['flag'] = (product_sales_data['count'] >= product_sales_data['count'].quantile(quantile_lower_limit))
  product_sales_data_set =  pd.DataFrame(product_sales_data[product_sales_data['flag'] == True]['entity_id'])
  product_sales_data_set['entity_id'] =  pd.to_numeric(product_sales_data_set['entity_id'])
  return product_sales_data_set

def get_product_sku_data():
  redshift_conn =  PasUtils.redshiftConnection()
  product_sku_query = """select  distinct sku, sku_type, product_id from dim_sku  where sku is not NULL and sku_type is not Null and product_id is not NULL"""

  product_sku_raw_data = pd.read_sql(product_sku_query, redshift_conn)
  product_sku_raw_data.columns = ['sku', 'sku_type', 'product_id']

  product_id_sku_details_map = {}
  sku_details_map = {}
  
  for index, row in product_sku_raw_data.iterrows():
    if (row['sku_type'].lower()) in ['simple']:
      product_id_sku_details_map[str(row['product_id'])] =  {"sku": row['sku'], "type": (row['sku_type']).lower()}
      sku_details_map[str(row['sku'])] = {"sku": row["sku"], "type": (row['sku_type']).lower(), 'product_id': row['product_id']}
  return product_id_sku_details_map, sku_details_map 


def get_pas_data(product_details):
  request_url = "http://" + PipelineUtils.getAPIHost() + "/apis/v2/pas.get"
  print (request_url)
  old_dict = {}
  n =  len(product_details)
  temp_product_details = []
  for counter, p in enumerate(product_details):
    temp_product_details.append(p)
    if (counter + 1)%100 == 0 or counter == n-1:
      request_data = json.dumps({'products': temp_product_details}).encode('utf-8')
      #print (json.loads(request_data.decode('utf-8')))
      try:
        req = Request(request_url, data = request_data, headers = {'content-type': 'application/json'})
        pas_object = json.loads(urlopen(req).read().decode('utf-8'))
        #print (pas_object)
        pas_object = pas_object['skus']
        old_dict.update(pas_object)
        temp_product_details = []
      except:
        pass

  return old_dict

def get_product_detail(product_id):
  request_url = "http://" + PipelineUtils.getAPIHost() + "/apis/v2/product.list?id={0}".format(product_id)

  r =  requests.get(request_url, headers = {'content-type': 'application/json'})
  req_fields = ['category_levels', 'brand_name', 'brand_ids', 'is_luxe', 'catalog_tag', 'parent_id']
  try:
    data = (json.loads(r.content.decode('utf-8')).get('result'))
    row  = {}
    for f in req_fields:
      if data.get(f):
        row[f] = data.get(f)
    return row
  except:
    return {}

def helper_function(weekly_category_wise_count_map, weekly_category_brand_count_map, weekly_brand_count_map, docs, already_dotd_products):
  # new_docs  = sorted(docs, key=lambda x: float(x.get('discount', 0)), reverse=True)

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
    brand_id_list = []
    l1 = None
    l2 = None
    l3 = None

    is_not_cat3_ids = False
    if doc.get('category_levels'):
      cat_levels =  doc.get('category_levels')

      for key, value in cat_levels.items():
        if value == "2":
          l3 =  int(key)
          l_list.append(l3)
          if str(l3) not in cat3_ids:
            is_not_cat3_ids = True
            
    if is_not_cat3_ids:
      continue
    
    if not l3:
      l_list.append('UNKNOWN')
      continue

    if doc.get('brand_ids'):
      brand_ids = doc.get('brand_ids').strip().split(',')
      for bid in brand_ids:
        brand_id_list.append(bid)
      
    else:
      brand_ids =  'UNKNOWN'
      brand_id_list.append(brand_ids)
      continue
    
    is_eligible = True
    for l in l_list:
      for brand_ids in brand_id_list:

        cat_brand = None
        if l3 and brand_ids:
          cat_brand =  str(l)+ str(brand_ids)

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
      for  brand_ids in brand_id_list:
        if daily_brand_count_map.get(brand_ids):
          daily_brand_count_map[brand_ids] += 1
        else:
          daily_brand_count_map[brand_ids] = 1
      for l in l_list:
        for brand_ids  in brand_id_list:
          cat_brand = str(l)+  str(brand_ids)

          if daily_category_brand_count_map.get(cat_brand):
            daily_category_brand_count_map[cat_brand] += 1
          else:
            daily_category_brand_count_map[cat_brand] = 1
      final_list.append(doc)
  return final_list

def get_dotd_products(dt):
  redshift_conn = PasUtils.redshiftConnection()
  query =  "select distinct product_id,sku from deal_of_the_day_data where date_time >='{0}' and date_time <'{1}'".format( back_date_7_days, dt)
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
          l_list.append(l3)

      #if l1:
      #  l_list.append(l1)
      #if l2: 
      #  l_list.append(l2)

      for l in l_list:
        if weekly_category_wise_count_map.get(l):
          weekly_category_wise_count_map[l] +=1
        else:
          weekly_category_wise_count_map[l] =1
    
    brand_ids = []
    if d.get('brand_ids'):
      brand_ids = d.get('brand_ids').strip().split(',')
      for brand_id  in brand_ids:
        if weekly_brand_count_map.get(brand_id):
          weekly_brand_count_map[brand_id] += 1
        else:
          weekly_brand_count_map[brand_id] = 1
      
      for l in l_list:
        for brand_id in brand_ids: 
          cat_brand =  str(l)+ str(brand_id)
          if weekly_category_brand_count_map.get(cat_brand):
            weekly_category_brand_count_map[cat_brand] +=1
          else:
            weekly_category_brand_count_map[cat_brand] = 1


  final_list = helper_function(weekly_category_wise_count_map, weekly_category_brand_count_map, weekly_brand_count_map, docs, already_dotd_products)
  return final_list

def dump_to_redshift(docs, dt):
  redshift_conn = PasUtils.redshiftConnection()

  cur =  redshift_conn.cursor()
  delete_query  =  "delete from deal_of_the_day_data where date_time ='{0}'".format(dt)
  cur.execute(delete_query)
  redshift_conn.commit()

  for counter, doc in enumerate(new_docs[:30]):
    if doc.get('product_id'):
      product_id =  doc.get('product_id')
      sku =  doc.get('sku')
      position =  counter
      sets =  doc.get('sets')
      price  =  doc.get('mrp')
      discount =  doc.get('discount')
      query  =  "insert into deal_of_the_day_data values ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}')".format(product_id, sku, position, dt, sets, price, discount)

      cur.execute(query)
  redshift_conn.commit()

def get_featured_products():
  inventories = list()

  query = {"page_type": "adhoc-data", "page_section": "deals-of-the-day", "page_data": "featured-products"}
  inventories.append(query)

  requestBody = {'inventories': inventories}

  url = 'https://' + PipelineUtils.getAdPlatformEndPoint() + '/inventory/data/json/'
  response = requests.post(url, json=requestBody, headers={'Content-Type':'application/json'})

  if (response.ok):
    result = json.loads(response.content.decode('utf-8')).get('result')[0]
    for inventory in result['inventories']:
      if inventory['widget_data']['wtype'] == "DATA_WIDGET":
        product_id_list = inventory['widget_data']['parameters']["data_"]
        return product_id_list

  return ""

if __name__ == '__main__':
  global back_date_7_days
  global date_today
  dt = str(datetime.now().date())
  back_date_7_days = str((datetime.now()-timedelta(days=8)).date())
  date_today =dt

  print ("intent set")
  #intent_set =  get_intent_set()
  print ("rating set")
  rating_set =  get_rating_set()
  sales_set =  get_sales_data()
  product_id_sku_details_map, sku_details_map =  get_product_sku_data()

  #print( len( (intent_set.append(rating_set.append(sales_set))).reset_index() ))
  #all_selected_product =  list((intent_set.append(rating_set.append(sales_set))).reset_index()['entity_id'].unique())
  all_selected_product = []
  for index, row in rating_set.iterrows():
    all_selected_product.append(str(row['entity_id']))

  #for index, row in intent_set.iterrows():
  #  all_selected_product.append(str(row['entity_id']))

  for index, row in sales_set.iterrows():
    all_selected_product.append(str(row['entity_id']))

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
      if row.get("is_in_stock", 0) and (not row.get('disabled', False)) and row.get('discount', 0)>=min_discount and row.get('discount', 0)<= max_discount and row.get('product_id') and float(row.get('quantity', 0))> 10:
        p_details = get_product_detail(row.get('product_id'))
        if len(p_details) < 1:
          continue
        for key, value in p_details.items():
          row[key] = value
        if 'men' in row.get('catalog_tag'):
          continue
        docs.append(row)
 
  #print (docs[:20])
  for i, doc in enumerate(docs):
    pop_obj = get_popularity_for_id(product_id  = doc.get('parent_id'), parent_id =  doc.get('parent_id'))
    print (pop_obj)
    popularity = 0 
    if pop_obj.get(doc.get('parent_id')):
      popularity = float(pop_obj[doc.get('parent_id')].get('popularity', .01))

    score  =  float(doc.get('discount',0))* popularity
    doc['pop_score'] = score
    doc['popularity'] = popularity
    docs[i]= doc
    if (i+1)%500 ==0:
      import time
      time.sleep(2)

  new_docs=  sorted(docs, key=lambda x: float(x.get('pop_score', 0)), reverse=True)
  print (len(new_docs))
  print (new_docs[:10])
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
  print (len(new_docs))
  if len(new_docs) < 20:
    print("Selected product are not sufficient, Please check")

  starttime = str(date_today) + ' 10:00:00'
  endtime = str(date_today) + ' 23:59:59'
  position = 0
  featured_list = get_featured_products().split(',')
  for pid in featured_list:
    pid = str(pid)
    if pid in product_id_sku_details_map:
      query = """insert into deal_of_the_day_data (product_id, sku, starttime, endtime, position) values ('{0}', '{1}', '{2}', '{3}', '{4}') 
                on duplicate key update product_id ='{0}', sku='{1}', starttime='{2}', endtime = '{3}' """.\
                format(pid, product_id_sku_details_map.get(pid).get('sku'), starttime, endtime, position)
      PasUtils.mysql_write(query)
      position = position + 1
  remaining_length = 30-position
  for counter, doc in enumerate(new_docs[:30]):
    if(position >= 30):
      break
    if doc.get('product_id'):
      if doc.get('product_id') in featured_list:
        continue
      print (doc) 
      mysql_conn =  PasUtils.mysqlConnection('w')
      product_id = str(doc.get('product_id'))
      sku = doc.get('sku')
      query =  """insert into deal_of_the_day_data (product_id, sku, starttime, endtime, position) values ('{0}', '{1}', '{2}', '{3}', '{4}') 
                  on duplicate key update product_id ='{0}', sku='{1}', starttime='{2}', endtime = '{3}' """.\
                  format(product_id, sku, starttime, endtime,position)
      ans  = PasUtils.mysql_write(query)
      position = position + 1
      sets_list = []
      if int(product_id) in list(rating_set['entity_id']):
        print ( str(product_id)+ " in rating set")
        sets_list.append('rating set')

      #if int(product_id) in list(intent_set['entity_id']):
      #  print ( str(product_id)+ " in intent set")
      #  sets_list.append('intent set')
    
      if int(product_id) in list(sales_set['entity_id']):
        print ( str(product_id)+  " in sales_data set")
        sets_list.append('sales set')
      doc['sets'] =  ','.join(sets_list)
      new_docs[counter] = doc

  dump_to_redshift(new_docs[:remaining_length], dt)
