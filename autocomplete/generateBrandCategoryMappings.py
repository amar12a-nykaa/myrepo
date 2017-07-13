#!/usr/bin/python
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
client = MongoClient("52.221.96.159")

popularity_index = {}
cat_name_id = {}
cat_id_url = {}
brand_name_id = {}

brand_popularity = defaultdict(int)

def create_popularity_index():
  global popularity_index
  popularity_table = client['search']['popularity']
  for prod in popularity_table.find():
    #print(prod)
    popularity_index[prod['_id']] = prod['popularity']

create_popularity_index()
#print(popularity_index)


def getProducts():
  products = []
  global cat_name_id
  global brand_name_id

  # Connection to 'analytics' host
  host = "nykaa-analytics.nyk00-int.network"
  user = "analytics"
  password = "P1u8Sxh7kNr"
  db = "analytics" 
  nykaa_analytics_db_conn = mysql.connector.connect(host=host, user=user, password=password, database=db)  

  #Connection to 'read-replica' host
  nykaa_replica_db_conn = Utils.nykaaMysqlConnection()

  #Category id - name mapping
  query = "select distinct l4, l4_id from analytics.sku_l4_updated_final;"
  results = Utils.fetchResults(nykaa_analytics_db_conn, query)
  for row in results:
    cat_name_id[row['l4'].strip()] = row['l4_id']
  print("cat_name_id: %s" % sorted(list(cat_name_id.keys())))

  #Category name-url mapping
  query = "SELECT DISTINCT category_id, request_path AS url FROM nykaalive1.core_url_rewrite WHERE product_id IS NULL AND category_id IS NOT NULL"
  results = Utils.fetchResults(nykaa_analytics_db_conn, query)
  for row in results:
    if row['category_id'] is not None:
      cat_id_url[row['category_id']] = row['url']
  print("cat_id_url: %s" % sorted(list(cat_id_url.keys())))

  #Brand id - name mapping
  query = "select nkb.name, nkb.brand_id, tsb.brand_id as brands_v1, tsb.link from nk_brands nkb INNER JOIN top_selling_brands tsb ON nkb.name=tsb.brand"
  results = Utils.fetchResults(nykaa_replica_db_conn, query)
  for brand in results:
    brand_name = brand['name']
    brand_id = brand['brand_id']
    brands_v1 = brand['brands_v1']
    brand_url = "http://www.nykaa.com/" + brand['link']
    if brand_name is not None:
      brand_name_id[brand_name.strip()] = {'brand_id': brand_id, 'brands_v1': brands_v1, 'brand_url': brand_url}
  print("brand_name_id: %s" % sorted(list(brand_name_id.keys())))

  query = "SELECT sl.entity_id AS simple_id, sl.sku AS simple_sku,\
           sl.key AS parent_id, sl.key_sku AS parent_sku, cd.brand,sl.l2 AS category_l1,sl.l3 AS category_l2,sl.l4 AS category_l3 \
           FROM analytics.sku_l4_updated_final sl\
           JOIN analytics.catalog_dump cd ON cd.entity_id=sl.entity_id\
           WHERE sl.l2 NOT LIKE '%Luxe%'\
           "
  results = Utils.fetchResults(nykaa_analytics_db_conn, query)
  print("Fetching products from Nykaa DB..")
  for row in results:
    products.append(row)

  return products


def getMappings(products):
  print("Generating brand category mappings..")
  brand_category_mappings = {}
  for product in products:
    #print("-- %s" % product)
    if not product.get('brand'):
      print("cont1")
      continue

    brand = product['brand'].strip()
#    if not brand or not product.get('L3'):
#      continue

    category = product.get('L3', "").strip() or product.get('category_l3', "").strip()
    if not category:
      continue
      
    categories = {}
    if brand in brand_category_mappings:
      categories = brand_category_mappings[brand]

    if category not in categories:
      categories[category] = 0

    product['simple_id'] = str( product['simple_id'])
    #if product['simple_id']  =='254':
      #IPython.embed()
    categories[category] += popularity_index.get(product['simple_id'], 0)
    brand_popularity[brand] += popularity_index.get(product['simple_id'], 0)
    brand_category_mappings[brand] = categories
  return brand_category_mappings


def saveMappings(brand_category_mappings):
  print("Saving brand category mappings in DB..")
  mysql_conn = Utils.mysqlConnection('w')
  cursor = mysql_conn.cursor()

  query = "INSERT INTO brand_category_mappings (brand, brand_id, brands_v1, brand_popularity, categories, brand_url) VALUES (%s, %s, %s, %s, %s, %s) "
  query += "ON DUPLICATE KEY UPDATE categories = %s"

  top_brand_category_mapping = {}                     
  for brand, categories in brand_category_mappings.items():
    sortkey = operator.itemgetter(1)
    sorted_categories = sorted(categories.items(), key=sortkey, reverse=True)
    top_categories = sorted_categories[:5]
    try:
      top_categories_str = json.dumps([{"category":k[0], "category_id": cat_name_id[k[0]], "category_url": "http://www.nykaa.com/" + cat_id_url[cat_name_id[k[0]]]} for k in top_categories])
    except:
      print("Skipping brand %s"% brand)
      continue
      pass

    if brand not in brand_name_id:
      print("Skipping %s"% brand)
      continue
    values = (brand, brand_name_id[brand]['brand_id'], brand_name_id[brand]['brands_v1'], brand_popularity[brand], top_categories_str, brand_name_id[brand]['brand_url'], top_categories_str)
    cursor.execute(query, values)
    mysql_conn.commit()

  cursor.close()
  mysql_conn.close()

products = getProducts()
#print("products: %s" % products)
#print("pop: %s" % popularity_table.find_one())

brand_category_mappings = getMappings(products)
print("brand_category_mappings: %s" % brand_category_mappings)
saveMappings(brand_category_mappings)
print(brand_popularity)
