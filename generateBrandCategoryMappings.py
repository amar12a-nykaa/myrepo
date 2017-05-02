#!/usr/bin/python
import sys
import csv
import json
import pprint
import operator
import mysql.connector

from pas.v1.utils import Utils
from contextlib import closing


def getProducts():
  products = []

  host = "analytics-3.ciel4c1bqlwh.ap-southeast-1.rds.amazonaws.com"
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
    if not product.get('brand'):
      continue

    brand = product['brand'].strip()
    if not brand or not product.get('L3'):
      continue

    category = product['L3'].strip()
    if not category:
      continue
      
    categories = {}
    if brand in brand_category_mappings:
      categories = brand_category_mappings[brand]

    if category not in categories:
      categories[category] = 0

    categories[category] += 1
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
brand_category_mappings = getMappings(products)
saveMappings(brand_category_mappings)
