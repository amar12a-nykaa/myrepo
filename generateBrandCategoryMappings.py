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

client = MongoClient("52.221.96.159")
popularity_index = {}

def create_popularity_index():
  global popularity_index
  popularity_table = client['search']['popularity']
  for prod in popularity_table.find():
    #print(prod)
    popularity_index[prod['_id']] = prod['popularity']

create_popularity_index()
#print(popularity_index)
#sys.exit()

def getProducts():
  products = []

  host = "nykaa-analytics.nyk00-int.network"
  user = "analytics"
  password = "P1u8Sxh7kNr"
  db = "analytics"

  nykaadb = mysql.connector.connect(host=host, user=user, password=password, database=db)  
  cursor = nykaadb.cursor()
  query = "SELECT DISTINCT cpe.entity_id AS product_id, cpe.type_id AS type, cpe.sku, " \
          "l4.l2 AS L1, l4.l3 AS L2, l4.l4 AS L3, eaov.value AS brand " \
          "FROM nykaalive1.catalog_product_entity cpe " \
          "LEFT JOIN analytics.sku_l4 l4 ON l4.entity_id = cpe.entity_id " \
          "LEFT JOIN nykaalive1.catalog_product_entity_varchar cpev " \
          "ON cpev.entity_id = cpe.entity_id AND cpev.attribute_id = 668 " \
          "JOIN nykaalive1.eav_attribute_option_value eaov ON eaov.option_id = cpev.value " \
          "LIMIT 99999999"

  query = "SELECT sl.entity_id AS simple_id, sl.sku AS simple_sku,\
           sl.key AS parent_id, sl.key_sku AS parent_sku, cd.brand,sl.l2 AS category_l1,sl.l3 AS category_l2,sl.l4 AS category_l3 \
           FROM analytics.sku_l4_updated_final sl\
           JOIN analytics.catalog_dump cd ON cd.entity_id=sl.entity_id\
           WHERE sl.l2 NOT LIKE '%Luxe%'\
           "

  print("Fetching products from Nykaa DB..")
  with closing(nykaadb.cursor()) as cursor:
    cursor.execute(query)
    for row in cursor.fetchall():
      products.append(dict(zip([col[0] for col in cursor.description], row)))

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
      print("cont2")
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
    brand_category_mappings[brand] = categories

  return brand_category_mappings


def saveMappings(brand_category_mappings):
  print("Saving brand category mappings in DB..")
  mysql_conn = Utils.mysqlConnection('w')
  cursor = mysql_conn.cursor()

  query = "INSERT INTO brand_category_mappings (brand, categories) VALUES (%s, %s) "
  query += "ON DUPLICATE KEY UPDATE categories = %s"

  top_brand_category_mapping = {}                     
  for brand, categories in brand_category_mappings.items():
    sortkey = operator.itemgetter(1)
    sorted_categories = sorted(categories.items(), key=sortkey, reverse=True)
    top_categories = sorted_categories[:5]
    top_categories_str = json.dumps([k[0] for k in top_categories])
    values = (brand, top_categories_str, top_categories_str)
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
