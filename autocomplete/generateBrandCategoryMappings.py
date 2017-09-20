#!/usr/bin/python
import IPython
import pprint
import traceback
import sys
import json
import operator
import mysql.connector

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils
from pymongo import MongoClient 
from collections import defaultdict

popularity_index = {}
cat_id_index = defaultdict(dict)
brand_name_id = {}
brand_name_name = {}
brand_popularity = defaultdict(float)
category_popularity = defaultdict(float)

## MySQL Connections 
host = "nykaa-analytics.nyk00-int.network"
user = "analytics"
password = "P1u8Sxh7kNr"
db = "analytics" 
nykaa_analytics_db_conn = mysql.connector.connect(host=host, user=user, password=password, database=db)  

#Connection to 'read-replica' host
nykaa_replica_db_conn = Utils.nykaaMysqlConnection()



def build_product_popularity_index():
  client = MongoClient("52.221.96.159")
  global popularity_index
  popularity_table = client['search']['popularity']
  #max_popularity = popularity_table.aggregate([{"$group":{ "_id": "max", "max":{"$max": "$popularity"}}}])

  for prod in popularity_table.find():
    popularity_index[prod['_id']] = prod['popularity']

def get_category_details():
  global cat_id_index

  #Category id - name mapping
  query = "SELECT DISTINCT l4, l4_id FROM analytics.sku_l4;"
  results = Utils.fetchResults(nykaa_analytics_db_conn, query)
  for row in results:
    _id = str(row['l4_id'])
    name = row['l4'].strip()

    cat_id_index[_id]['name'] = name

  #Category name-url mapping
  query = "SELECT DISTINCT category_id, request_path AS url FROM nykaalive1.core_url_rewrite WHERE product_id IS NULL AND category_id IS NOT NULL"
  results = Utils.fetchResults(nykaa_replica_db_conn, query)
  for row in results:
    _id = str(row['category_id'])
    url = "http://www.nykaa.com/" + row['url']
    if _id in cat_id_index:
      cat_id_index[_id]['url'] = url


def update_category_table(products):
  """
    Creates category popularity and stores in a global variable "category_popularity"
    Stores the category popularity in database as well.
  """
  print("Populating categories table ... ")

  # Add up popularity of products per category
  for product in products:
    category_id = str(product.get('category_l3_id', ""))
    if not category_id:
      continue
    product_simple_id = str(product['simple_id'])
    category_popularity[category_id] += popularity_index.get(product_simple_id, 0)

  #Normalize category_popularity
  max_category_popularity = 0
  for k,v in category_popularity.items():
    max_category_popularity = max(max_category_popularity, v)
  for k,v in category_popularity.items():
    category_popularity[k] = category_popularity[k] / max_category_popularity * 100  + 100

  mysql_conn = Utils.mysqlConnection('w')
  cursor = mysql_conn.cursor()

  Utils.mysql_write("delete from l3_categories", connection = mysql_conn)
  query = "REPLACE INTO l3_categories(id, name, url, category_popularity) VALUES (%s, %s, %s, %s) "
  #print("cat_id_index: %s" % cat_id_index)
  for _id, d in cat_id_index.items():
    cat_name = d.get('name')
    cat_url = d.get('url')
    if cat_name and cat_url:
      values = (_id, cat_name, cat_url, category_popularity.get(_id, 0))
      cursor.execute(query, values)
      mysql_conn.commit()

  cursor.close()
  mysql_conn.close()
  

def getProducts():
  products = []
  global brand_name_id
  global brand_name_name

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
      brand_upper = brand_name.strip()
      brand_lower = brand_upper.lower()
      brand_name_name[brand_lower] = brand_upper

      brand_name_id[brand_lower] = {'brand_id': brand_id, 'brand_url': brand_url, 'brands_v1': brands_v1}

  print("Fetching products from Nykaa DB..")
  query = "SELECT sl.entity_id AS simple_id, sl.sku AS simple_sku,\
           sl.key AS parent_id, sl.key_sku AS parent_sku, sl.l2 AS category_l1, sl.l3 AS category_l2, sl.l4 AS category_l3, sl.l4_id AS category_l3_id, cd.brand \
           FROM analytics.sku_l4 sl\
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
      continue

    brand = product['brand'].strip().lower()
    category_id = str(product.get('category_l3_id', ""))
    if not category_id:
      continue
      
    categories = {}
    if brand in brand_category_mappings:
      categories = brand_category_mappings[brand]

    if category_id not in categories:
      categories[category_id] = 0

    product_simple_id = str(product['simple_id'])

    categories[category_id] += popularity_index.get(product_simple_id, 0)
    brand_popularity[brand] += popularity_index.get(product_simple_id, 0)
    brand_category_mappings[brand] = categories


  #Normalize brand_popularity
  max_brand_popularity = 0
  for k,v in brand_popularity.items():
    if k =='Nykaa Cosmetics':
      continue
    max_brand_popularity = max(max_brand_popularity, v)
  for k,v in brand_popularity.items():
    brand_popularity[k] = brand_popularity[k] / max_brand_popularity * 100  + 200

  return brand_category_mappings


def saveMappings(brand_category_mappings):
  print("Saving brand category mappings in DB..")
  mysql_conn = Utils.mysqlConnection('w')
  cursor = mysql_conn.cursor()

  Utils.mysql_write("delete from brands", connection = mysql_conn)

  query = "REPLACE INTO brands (brand, brand_id, brands_v1, brand_popularity, top_categories, brand_url) VALUES (%s, %s, %s, %s, %s, %s) "

  num_brands_skipped = 0
  num_brands_processed= 0
  for brand, categories in brand_category_mappings.items():
    sortkey = operator.itemgetter(1)
    sorted_categories = sorted(categories.items(), key=sortkey, reverse=True)
    top_categories = []
    top_categories_str = ""
    try:
      for k in sorted_categories:
        if cat_id_index.get(k[0]):
          top_categories.append({"category": cat_id_index[k[0]]['name'], "category_id": k[0], "category_url": brand_name_id[brand]['brand_url'] + "?cat=%s" % k[0]})

        if len(top_categories) <= 5:
          top_categories_str = json.dumps(top_categories)
        if len(top_categories) == 5:
          break
    except:
      #print(traceback.format_exc())
      print("Skipping brand %s"% brand)
      num_brands_skipped += 1; 
      continue

    if brand not in brand_name_id:
      print("Skipping %s"% brand)
      num_brands_skipped += 1; 
      continue

    values = (brand_name_name[brand], brand_name_id[brand]['brand_id'], brand_name_id[brand]['brands_v1'], brand_popularity[brand], top_categories_str, brand_name_id[brand]['brand_url'])
    cursor.execute(query, values)
    mysql_conn.commit()
    num_brands_processed += 1

  cursor.close()
  mysql_conn.close()

  print("Number of Brands Skipped: %s" % num_brands_skipped)
  print("Number of Brands processed successfully: %s" % num_brands_processed)


build_product_popularity_index()
products = getProducts()
get_category_details()
update_category_table(products)
brand_category_mappings = getMappings(products)
#print("brand_category_mappings: %s" % sorted(list(brand_category_mappings.keys())))
saveMappings(brand_category_mappings)
