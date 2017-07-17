#!/usr/bin/python
import traceback
import IPython
import sys
import csv
import json
import pprint
import operator
import mysql.connector

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils
from contextlib import closing
from pymongo import MongoClient 
from collections import defaultdict

popularity_index = {}
cat_name_id = {}
cat_id_url = {}
brand_name_id = {}
brand_popularity = defaultdict(int)

## MySQL Connections 
host = "nykaa-analytics.nyk00-int.network"
user = "analytics"
password = "P1u8Sxh7kNr"
db = "analytics" 
nykaa_analytics_db_conn = mysql.connector.connect(host=host, user=user, password=password, database=db)  

#Connection to 'read-replica' host
nykaa_replica_db_conn = Utils.nykaaMysqlConnection()



def create_popularity_index():
  client = MongoClient("52.221.96.159")
  global popularity_index
  popularity_table = client['search']['popularity']
  for prod in popularity_table.find():
    popularity_index[prod['_id']] = prod['popularity']


def update_category_table():
  global cat_name_id
  global cat_id_url
  cat_id_index = defaultdict(dict)

  #Category id - name mapping
  query = "select distinct l4, l4_id from analytics.sku_l4_updated_final;"
  results = Utils.fetchResults(nykaa_analytics_db_conn, query)
  for row in results:
    _id = row['l4_id']
    name = row['l4'].strip()

    cat_name_id[name] = _id
    cat_id_index[_id]['name'] = name

  #print("cat_name_id: %s" % sorted(list(cat_name_id.keys())))

  #Category name-url mapping
  query = "SELECT DISTINCT category_id, request_path AS url FROM nykaalive1.core_url_rewrite WHERE product_id IS NULL AND category_id IS NOT NULL"
  results = Utils.fetchResults(nykaa_analytics_db_conn, query)
  for row in results:
    if row['category_id'] is not None:
      _id = row['category_id']
      url = "http://www.nykaa.com/" + row['url']
      if _id in cat_id_index:
        cat_id_url[_id] = url
        cat_id_index[_id]['url'] = url
  print("cat_id_url: %s" % sorted(list(cat_id_url.keys())))


  print("Populating categories table ... ")
  mysql_conn = Utils.mysqlConnection('w')
  cursor = mysql_conn.cursor()

  query = "REPLACE INTO categories(id, name, url) VALUES (%s, %s, %s) "

  print("cat_id_index: %s" % cat_id_index)
  for _id, d in cat_id_index.items():
    values = (_id, d.get('name', ""), d.get('url', ""))
    cursor.execute(query, values)
    mysql_conn.commit()

  cursor.close()
  mysql_conn.close()


def getProducts():
  products = []
  global brand_name_id

  #Brand id - name mapping
  query = """SELECT nkb.name AS name, nkb.brand_id AS id, cur.request_path AS url, ci.brand_id AS brands_v1 
             FROM nk_brands nkb INNER JOIN nykaalive1.core_url_rewrite cur ON nkb.brand_id=cur.category_id 
             INNER JOIN nykaalive1.category_information ci ON cur.category_id=ci.cat_id
             WHERE cur.product_id IS NULL
          """
  results = Utils.fetchResults(nykaa_replica_db_conn, query)
  for brand in results:
    brand_name = brand['name']
    brand_id = brand['id']
    brand_url = "http://www.nykaa.com/" + brand['url']
    brands_v1 = brand['brands_v1']
    if brand_name is not None:
      brand_name_id[brand_name.strip()] = {'brand_id': brand_id, 'brand_url': brand_url, 'brands_v1': brands_v1}
  print("brand_name_id: %s" % sorted(list(brand_name_id.keys())))

  print("Fetching products from Nykaa DB..")
  query = "SELECT sl.entity_id AS simple_id, sl.sku AS simple_sku,\
           sl.key AS parent_id, sl.key_sku AS parent_sku, cd.brand, sl.l2 AS category_l1, sl.l3 AS category_l2, sl.l4 AS category_l3 \
           FROM analytics.sku_l4_updated_final sl\
           JOIN analytics.catalog_dump cd ON cd.entity_id=sl.entity_id\
           WHERE sl.l2 NOT LIKE '%Luxe%'\
           "
  results = Utils.fetchResults(nykaa_analytics_db_conn, query)
  for row in results:
    products.append(row)

  return products


def getMappings(products):
  print("Generating brand category mappings..")
  brand_category_mappings = {}
  for product in products:
    if not product.get('brand'):
      print("cont1")
      continue

    brand = product['brand'].strip()
    category = product.get('L3', "").strip() or product.get('category_l3', "").strip()
    if not category:
      continue
      
    categories = {}
    if brand in brand_category_mappings:
      categories = brand_category_mappings[brand]

    if category not in categories:
      categories[category] = 0

    product['simple_id'] = str(product['simple_id'])

    categories[category] += popularity_index.get(product['simple_id'], 0)
    brand_popularity[brand] += popularity_index.get(product['simple_id'], 0)
    brand_category_mappings[brand] = categories
  return brand_category_mappings


def saveMappings(brand_category_mappings):
  print("Saving brand category mappings in DB..")
  mysql_conn = Utils.mysqlConnection('w')
  cursor = mysql_conn.cursor()

  query = "INSERT INTO brands(brand, brand_id, brands_v1, brand_popularity, top_categories, brand_url) VALUES (%s, %s, %s, %s, %s, %s) "
  query += "ON DUPLICATE KEY UPDATE top_categories = %s, brands_v1 = %s"

  for brand, categories in brand_category_mappings.items():
    sortkey = operator.itemgetter(1)
    sorted_categories = sorted(categories.items(), key=sortkey, reverse=True)
    top_categories = sorted_categories[:5]
    try:
      top_categories_str = json.dumps([{"category":k[0], "category_id": cat_name_id[k[0]], "category_url": brand_name_id[brand]['brand_url'] + "?cat=%s" % cat_name_id[k[0]] } for k in top_categories])
    except:
      print("Skipping brand %s"% brand)
      #print(traceback.format_exc())
      continue

    if brand not in brand_name_id:
      print("Skipping %s"% brand)
      continue
    values = (brand, brand_name_id[brand]['brand_id'], brand_name_id[brand]['brands_v1'], brand_popularity[brand], top_categories_str, brand_name_id[brand]['brand_url'], top_categories_str, brand_name_id[brand]['brands_v1'])
    cursor.execute(query, values)
    mysql_conn.commit()

  cursor.close()
  mysql_conn.close()


create_popularity_index()
update_category_table()
products = getProducts()
brand_category_mappings = getMappings(products)
print("brand_category_mappings: %s" % brand_category_mappings)
saveMappings(brand_category_mappings)
print(brand_popularity)
