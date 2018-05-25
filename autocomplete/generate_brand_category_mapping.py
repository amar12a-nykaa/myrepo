#!/usr/bin/python
import IPython
import json
import mysql.connector
import operator
import pprint
import sys
import traceback

from IPython import embed
from collections import defaultdict
from pymongo import MongoClient 

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/nykaa/scripts/sharedutils")
from esutils import EsUtils


popularity_index = {}
cat_id_index = defaultdict(dict)
brand_name_id = {}
brand_name_name = {}
brand_popularity = defaultdict(float)
category_popularity = defaultdict(float)
brand_cat_popularity = defaultdict(lambda : defaultdict(float))

## MySQL Connections 
host = "nykaa-analytics.nyk00-int.network"
user = "analytics"
password = "P1u8Sxh7kNr"
db = "analytics" 
nykaa_analytics_db_conn = mysql.connector.connect(host=host, user=user, password=password, database=db)

#Connection to 'read-replica' host
nykaa_replica_db_conn = Utils.nykaaMysqlConnection(force_production=True)

es = EsUtils.get_connection()


#WEIGHT_BRAND= [200, 300]
#WEIGHT_CATEGORY = [0, 200]
WEIGHT_CATEGORY_FACET = 100
#WEIGHT_BRAND_CATEGORY = [0, 150]
#WEIGHT_PRODUCT= [0, 100]

BLACKLISTED_FACETS = ['old_brand_facet', ]
POPULARITY_THRESHOLD = 0.1

def build_product_popularity_index():
  client = Utils.mongoClient()
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
    category_popularity[k] = category_popularity[k] / max_category_popularity * 100 + 100

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

  Utils.mysql_write("create or replace view l3_categories_clean as select * from l3_categories where url not like '%luxe%' and url not like '%shop-by-concern%' and category_popularity>0;")

  

def getProducts():
  products = []
  global brand_name_id
  global brand_name_name

  #Brand id - name mapping
  query = """
          select distinct name, id, url, brands_v1 
          FROM(
             SELECT nkb.name AS name, nkb.brand_id AS id, cur.request_path AS url, ci.brand_id AS brands_v1 
             FROM nk_brands nkb 

             INNER JOIN nykaalive1.core_url_rewrite cur 
             ON nkb.brand_id=cur.category_id 

             INNER JOIN nykaalive1.category_information ci 
             ON cur.category_id=ci.cat_id

             WHERE cur.product_id IS NULL
          )A
          """
  results = Utils.fetchResults(nykaa_replica_db_conn, query)
  for brand in results:
    brand_name = brand['name'].replace("’", "'")
    brand_id = brand['id']
    brand_url = "http://www.nykaa.com/" + brand['url']
    brands_v1 = brand['brands_v1']
    if brand_name is not None:
      brand_upper = brand_name.strip()
      brand_lower = brand_upper.lower()
      brand_name_name[brand_lower] = brand_upper

      brand_name_id[brand_lower] = {'brand_id': brand_id, 'brand_url': brand_url, 'brands_v1': brands_v1}

  query = "show indexes in analytics.sku_l4"
  index_on_entity_id__sku_l4 = [ x for x in Utils.mysql_read(query, connection=nykaa_analytics_db_conn) if x['Column_name'] == 'entity_id']
  assert index_on_entity_id__sku_l4, "Index missing on sku_l4"

  query = "show indexes in analytics.catalog_dump"
  index_on_entity_id__catalog_dump = [ x for x in Utils.mysql_read(query, connection=nykaa_analytics_db_conn) if x['Column_name'] == 'entity_id']
  #assert index_on_entity_id__catalog_dump, "Index Missing on catalog_dump"

  print("Fetching products from Nykaa DB..")
  query = "SELECT sl.entity_id AS simple_id, sl.sku AS simple_sku,\
           sl.key AS parent_id, sl.key_sku AS parent_sku, sl.l2 AS category_l1, sl.l3 AS category_l2, sl.l4 AS category_l3, sl.l4_id AS category_l3_id, cd.brand \
           FROM analytics.sku_l4 sl\
           JOIN analytics.catalog_dump cd ON cd.entity_id=sl.entity_id\
           WHERE sl.l2 NOT LIKE '%Luxe%'\
           "
  results = Utils.fetchResults(nykaa_analytics_db_conn, query)
  if not results:
    raise Exception("Could not fetch data from magento databases")
  for row in results:
    if row['brand']:
      row['brand'] = row['brand'].replace("’", "'")
    products.append(row)

  print("# products found: %s" % len(products))
  return products


def getMappings(products):
  print("Generating brand category mappings..")
  global brand_cat_popularity 
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
    brand_cat_popularity[brand][category_id] += popularity_index.get(product_simple_id, 0)
    brand_category_mappings[brand] = categories


  #Normalize brand_popularity
  max_brand_popularity = 0
  for k,v in brand_popularity.items():
    if k =='Nykaa Cosmetics':
      continue
    max_brand_popularity = max(max_brand_popularity, v)
  for k,v in brand_popularity.items():
    brand_popularity[k] = brand_popularity[k] / max_brand_popularity * 100 * 2 + 100

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
    brand = brand.replace("’", "'")
    sortkey = operator.itemgetter(1)
    sorted_categories = sorted(categories.items(), key=sortkey, reverse=True)
    top_categories = []
    top_categories_str = ""
    try:
      category_names_added_yet = set()
      for k in sorted_categories:
        if cat_id_index.get(k[0]):
          name = cat_id_index[k[0]]['name']
          _id = k[0]
          url = brand_name_id[brand]['brand_url'] + "?cat=%s" % _id
          if name not in category_names_added_yet:
            top_categories.append({"category": name, "category_id": _id, "category_url": url})
          category_names_added_yet.add(name)

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

def update_brand_category_table():
  assert brand_cat_popularity
  if not Utils.mysql_read("SHOW TABLES LIKE 'brand_category'"):
    Utils.mysql_write("create table brand_category(brand varchar(30), brand_id varchar(32), category_id varchar(32), category_name varchar(32), popularity float)")
  Utils.mysql_write("delete from brand_category")

  max_pop = 0
  for brand, catinfo in brand_cat_popularity.items():
    for category_id, pop in sorted(catinfo.items(), key=lambda x: -x[1]):
      max_pop = max(pop, max_pop)
      break

  query = "REPLACE INTO brand_category (brand, brand_id, category_id, category_name, popularity) VALUES ('%s', '%s', '%s', '%s', %s) "
  for brand, catinfo in brand_cat_popularity.items():
    for  category_id, pop in sorted(catinfo.items(), key=lambda x: -x[1] ):
      try:
        pop = round(pop / max_pop * 100, 2 )
        q = query % (brand, brand_name_id[brand]['brand_id'], category_id, cat_id_index[category_id]['name'], pop)
        print(q)
        Utils.mysql_write(q)
      except:
        print(traceback.format_exc())

def update_brand_category_facets_table():
#  assert brand_cat_popularity
  if not Utils.mysql_read("SHOW TABLES LIKE 'brand_category_facets'"):
    Utils.mysql_write("create table brand_category_facets(brand varchar(30), brand_id varchar(32), category_id varchar(32), category_name varchar(32), facet varchar(20), popularity float)")
  Utils.mysql_write("delete from brand_category_facets")

  
  def agg_query(category_id, brand_id):
    return {
      "aggs": {
 #         "benefits_facet": { "terms": { "field": "benefits_facet.keyword", "size": 100 } },
 #         "brand_facet": { "terms": { "field": "brand_facet.keyword", "size": 100 } },
 #         "category_facet": { "terms": { "field": "category_facet.keyword", "size": 100 } },
          "color_facet": { "terms": { "field": "color_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
 #         "concern_facet": { "terms": { "field": "concern_facet.keyword", "size": 100 } },
 #         "coverage_facet": { "terms": { "field": "coverage_facet.keyword", "size": 100 } },
 #         "discount_interval": { "range": { "field": "discount", "ranges": [ { "from": "0", "to": "10.001" }, { "from": "10" }, { "from": "20" }, { "from": "30" }, { "from": "40" } ] } },
 #         "discount_stats": { "extended_stats": { "field": "discount" } },
 #         "finish_facet": { "terms": { "field": "finish_facet.keyword", "size": 100 } },
 #         "formulation_facet": { "terms": { "field": "formulation_facet.keyword", "size": 100 } },
 #         "gender_facet": { "terms": { "field": "gender_facet.keyword", "size": 100 } },
 #         "hair_type_facet": { "terms": { "field": "hair_type_facet.keyword", "size": 100 } },
 #         "old_brand_facet": { "terms": { "field": "old_brand_facet.keyword", "size": 100 } },
 #         "preference_facet": { "terms": { "field": "preference_facet.keyword", "size": 100 } },
#          "price_interval": {
#              "range": {
#                  "field": "price",
#                  "ranges": [ { "from": "0", "to": "499.001" }, { "from": "500", "to": "999.001" }, { "from": "1000", "to": "1999.001" }, { "from": "2000", "to": "3999.001" }, { "from": "4000" } ]
#              }
#          },
 #         "price_stats": { "extended_stats": { "field": "price" } },
 #         "skin_tone_facet": { "terms": { "field": "skin_tone_facet.keyword", "size": 100 } },
 #         "skin_type_facet": { "terms": { "field": "skin_type_facet.keyword", "size": 100 } },
 #         "spf_facet": { "terms": { "field": "spf_facet.keyword", "size": 100 } }
      },
      "query": {
          "bool": {
              "filter": [
                  { "term": { "visibility.keyword": "visible" } },
                  { "terms": { "category_ids.keyword": [ str(category_id) ] } },
                  { "terms": { "brand_ids.keyword": [ str(brand_id) ] } }
              ]
          }
      },
      "size": 0
    }

  max_pop = 0 
  arr = []
  for res in Utils.mysql_read("select * from brand_category"):
    category_id = res['category_id']
    brand_id = res['brand_id']
    brand = res['brand']
    category_name = res['category_name']
    #category_id = '249' 
    #brand_id = '604'
    query = agg_query(category_id=category_id, brand_id=brand_id)
    results = es.search(index='livecore', body=query)
    for row in results['aggregations']['color_facet']['buckets']:
      row['key'] = json.loads(row['key'])
      facet = row['key']['name'].lower()
      popularity = row['popularity_sum']['value']
      #print(facet, popularity)
      #print(query)
      max_pop = max(max_pop, popularity)
      arr.append((brand, brand_id, category_id, category_name, facet, popularity))
  
  print("Writing into mysql .. ")
  query = "REPLACE INTO brand_category_facets (brand, brand_id, category_id, category_name, facet, popularity) VALUES ('%s', '%s', '%s', '%s', '%s', %s) "
  for brand, brand_id, category_id, category_name, facet, popularity in arr:
    popularity = popularity/ max_pop * 50 
    q = query % (brand, brand_id, category_id, category_name, facet, popularity)
    Utils.mysql_write(q)
    print(q)


def update_category_facets_table():
  if not Utils.mysql_read("SHOW TABLES LIKE 'category_facets'"):
    Utils.mysql_write("create table category_facets(category_id varchar(32), category_name varchar(32), facet_name varchar(20), facet_val varchar(20), popularity float)")
  Utils.mysql_write("delete from category_facets")

  
  query = {
    "aggs": {
      "categories": {
        "terms": {
          "field": "category_ids.keyword",
          "size": 200
        },
        "aggs":{
            "benefits_facet": { "terms": { "field": "benefits_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            #"brand_facet": { "terms": { "field": "brand_facet.keyword", "size": 100 } },
            #"category_facet": { "terms": { "field": "category_facet.keyword", "size": 100 } },
            "color_facet": { "terms": { "field": "color_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "concern_facet": { "terms": { "field": "concern_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "coverage_facet": { "terms": { "field": "coverage_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "discount_interval": { "range": { "field": "discount", "ranges": [ { "from": "0", "to": "10.001" }, { "from": "10" }, { "from": "20" }, { "from": "30" }, { "from": "40" } ] } },
            "discount_stats": { "extended_stats": { "field": "discount" } },
            "finish_facet": { "terms": { "field": "finish_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "formulation_facet": { "terms": { "field": "formulation_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "gender_facet": { "terms": { "field": "gender_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "hair_type_facet": { "terms": { "field": "hair_type_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "old_brand_facet": { "terms": { "field": "old_brand_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "preference_facet": { "terms": { "field": "preference_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
#          "price_interval": {
#              "range": {
#                  "field": "price",
#                  "ranges": [ { "from": "0", "to": "499.001" }, { "from": "500", "to": "999.001" }, { "from": "1000", "to": "1999.001" }, { "from": "2000", "to": "3999.001" }, { "from": "4000" } ]
#              }
#          },
            "price_stats": { "extended_stats": { "field": "price" } },
            "skin_tone_facet": { "terms": { "field": "skin_tone_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "skin_type_facet": { "terms": { "field": "skin_type_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}},
            "spf_facet": { "terms": { "field": "spf_facet.keyword", "size": 100 }, "aggs": { "popularity_sum": { "sum": {"field": "popularity"}}}}
        }
      }
    },
    "size": 0
  }

  results = es.search(index='livecore', body=query, request_timeout=120)
  max_pop = 0
  arr = []
  is_good_facet = False
  for catbucket in results['aggregations']['categories']['buckets']:
    facet_names = [x for x in catbucket.keys() if '_facet' in x]
    for facet_name in facet_names:
      if facet_name in ['color_facet']:
        is_good_facet = True

      facet = catbucket[facet_name]
      for facet_bucket in facet['buckets']:
        facet_bucket['key'] = json.loads(facet_bucket['key'])
        coverage_percentage = facet_bucket['doc_count'] / catbucket['doc_count'] * 100 
        if 5 < coverage_percentage < 95:
          is_good_facet = True
        name = facet_bucket['key']['name'].lower()
        popularity = facet_bucket['popularity_sum']['value']
        max_pop = max(max_pop, popularity)
        if facet_name in BLACKLISTED_FACETS or popularity < POPULARITY_THRESHOLD:
          is_good_facet = False
        if is_good_facet:
          arr.append({'category_id': catbucket['key'], 'facet_name':facet_name, 'facet_val': name, 'popularity': popularity})

  for row in arr:
    try:
      category_name = cat_id_index[row['category_id']]['name']
    except:
      print("Skipping category_id %s in category_facets" % row['category_id'])
      continue

    query = "REPLACE INTO category_facets (category_id, category_name, facet_name, facet_val, popularity) VALUES ('%s', '%s', '%s', '%s', %s) "
    popularity = row['popularity']/ max_pop * WEIGHT_CATEGORY_FACET 
    query = query % (row['category_id'], category_name, row['facet_name'], row['facet_val'].strip().replace("'", "''"), popularity)
    Utils.mysql_write(query)


def generate_brand_category_mapping():
  build_product_popularity_index()
  products = getProducts()
  get_category_details()
  update_category_table(products)
  brand_category_mappings = getMappings(products)
  update_brand_category_table()
  ##update_brand_category_facets_table()
  update_category_facets_table()
  saveMappings(brand_category_mappings)


if __name__ == "__main__":
  generate_brand_category_mapping()
  #embed()
