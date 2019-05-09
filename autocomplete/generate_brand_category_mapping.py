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

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils

sys.path.append("/nykaa/scripts/sharedutils")
from esutils import EsUtils
from idutils import strip_accents
from mongoutils import MongoUtils

from ensure_mongo_indexes import ensure_mongo_indices_now
ensure_mongo_indices_now()

popularity_index = {}
cat_id_index = defaultdict(dict)
brand_name_id = {}
brand_name_name = {}
brand_popularity = defaultdict(lambda : defaultdict(float))
# CATEGORY_POPULARITY = defaultdict(float)
category_popularity = defaultdict(lambda  : defaultdict(float))
brand_cat_popularity = defaultdict(lambda : defaultdict(lambda : defaultdict(float)))

NYKAA = "nykaa"
MEN = "men"

es = EsUtils.get_connection()

#WEIGHT_BRAND= [200, 300]
#WEIGHT_CATEGORY = [0, 200]
WEIGHT_CATEGORY_FACET = 100
#WEIGHT_BRAND_CATEGORY = [0, 150]
#WEIGHT_PRODUCT= [0, 100]

BLACKLISTED_FACETS = ['old_brand_facet', ]
POPULARITY_THRESHOLD = 0.1

def build_product_popularity_index():
  client = MongoUtils.getClient()
  global popularity_index
  popularity_table = client['search']['popularity']
  #max_popularity = popularity_table.aggregate([{"$group":{ "_id": "max", "max":{"$max": "$popularity"}}}])

  for prod in popularity_table.find():
    popularity_index[prod['_id']] = prod['popularity']

def get_category_details():
  global cat_id_index

  #Category id - name mapping
  nykaa_redshift_connection = Utils.redshiftConnection()
  query = """select distinct l3_id, l3_name as primary_l3 from product_category_mapping 
              where l1_id not in (194,7287) and l2_id not in (345,734,2058,2064,2091,3962,652,3049,3050,8440)
              and l3_id not in (4036,3746,3745,3819,6612);"""
  results = Utils.fetchResults(nykaa_redshift_connection, query)
  for row in results:
    _id = str(row['l3_id'])
    name = row['primary_l3'].strip()
    cat_id_index[_id]['name'] = name
  nykaa_redshift_connection.close()

  #Category name-url mapping
  nykaa_replica_db_conn = Utils.nykaaMysqlConnection(force_production=True)
  query = "SELECT DISTINCT category_id, request_path AS url FROM nykaalive1.core_url_rewrite WHERE product_id IS NULL AND category_id IS NOT NULL"
  results = Utils.fetchResults(nykaa_replica_db_conn, query)
  for row in results:
    _id = str(row['category_id'])
    url = row['url']
    men_url = row['url']
    if _id in cat_id_index:
      cat_id_index[_id]['url'] = url
      cat_id_index[_id]['men_url'] = men_url
  nykaa_replica_db_conn.close()

def update_category_table(products):
  """
    Creates category popularity and stores in a global variable "CATEGORY_POPULARITY"
    Stores the category popularity in database as well.
  """
  print("Populating categories table ... ")

  # Add up popularity of products per category
  for product in products:
    category_id = str(product.get('category_l3_id', ""))
    if not category_id:
      continue
    product_simple_id = str(product['simple_id'])
    category_popularity[category_id][NYKAA] += popularity_index.get(product_simple_id, 0)
    if(product.get('is_men') == 'Yes'):
      category_popularity[category_id][MEN] += popularity_index.get(product_simple_id, 0)

  #Normalize CATEGORY_POPULARITY
  max_category_popularity_nykaa = 0
  max_category_popularity_men = 0

  for k,v in category_popularity.items():
    max_category_popularity_nykaa = max(max_category_popularity_nykaa, v.get(NYKAA))
    if(MEN in v):
      max_category_popularity_men = max(max_category_popularity_men, v.get(MEN))

  for k,v in category_popularity.items():
    category_popularity[k][NYKAA] = category_popularity[k][NYKAA] / max_category_popularity_nykaa * 100 + 100
    if(MEN in v):
      category_popularity[k][MEN] = category_popularity[k][MEN] / max_category_popularity_men * 100 + 100

  mysql_conn = Utils.mysqlConnection('w')
  cursor = mysql_conn.cursor()

  Utils.mysql_write("delete from l3_categories", connection = mysql_conn)
  query = "REPLACE INTO l3_categories(id, name, url, men_url, category_popularity, category_popularity_men) VALUES (%s, %s, %s, %s, %s, %s) "
  #print("CAT_ID_INDEX: %s" % CAT_ID_INDEX)
  for _id, d in cat_id_index.items():
    cat_name = d.get('name')
    cat_url = d.get('url')
    cat_men_url = d.get('men_url')
    if cat_name and cat_url and category_popularity.get(_id):
      values = (_id, cat_name, cat_url, cat_men_url, category_popularity.get(_id).get(NYKAA, 0), category_popularity.get(_id).get(MEN, 0))
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
  nykaa_replica_db_conn = Utils.nykaaMysqlConnection(force_production=True)
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
    brand_name = strip_accents(brand_name)
    brand_id = brand['id']
    brand_url = brand['url']
    brand_men_url = brand['url']
    brands_v1 = brand['brands_v1']
    if brand_name is not None:
      brand_upper = brand_name.strip()
      brand_lower = brand_upper.lower()
      brand_name_name[brand_lower] = brand_upper

      brand_name_id[brand_lower] = {'brand_id': brand_id, 'brand_url': brand_url, 'brand_men_url': brand_men_url, 'brands_v1': brands_v1}
  nykaa_replica_db_conn.close()

  print("Fetching products from DWH")
  nykaa_redshift_connection = Utils.redshiftConnection()
  query = """SELECT 
                ds.product_id AS simple_id, pcm.l3_id AS category_l3_id, ds.brand_name as brand, ds.is_men 
             FROM dim_sku ds JOIN product_category_mapping pcm ON ds.product_id = pcm.product_id 
             WHERE pcm.l3_id != 0"""
  results = Utils.fetchResults(nykaa_redshift_connection, query)
  if not results:
    raise Exception("Could not fetch data from dwh databases")
  for row in results:
    if row['brand']:
      row['brand'] = row['brand'].replace("’", "'")
      row['brand'] = strip_accents(row['brand'])
    products.append(row)

  nykaa_redshift_connection.close()

  print("# products found: %s" % len(products))
  return products


def getMappings(products):
  print("Generating brand category mappings..")
  global brand_cat_popularity
  global brand_popularity
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
      categories[category_id] = {}
      categories[category_id][NYKAA] = 0
      categories[category_id][MEN] = 0

    product_simple_id = str(product['simple_id'])

    categories[category_id][NYKAA] += popularity_index.get(product_simple_id, 0)
    brand_popularity[brand][NYKAA] += popularity_index.get(product_simple_id, 0)
    brand_cat_popularity[brand][category_id][NYKAA] += popularity_index.get(product_simple_id, 0)
    if(product.get('is_men') == 'Yes'):
      categories[category_id][MEN] += popularity_index.get(product_simple_id, 0)
      brand_popularity[brand][MEN] += popularity_index.get(product_simple_id, 0)
      brand_cat_popularity[brand][category_id][MEN] += popularity_index.get(product_simple_id, 0)

    brand_category_mappings[brand] = categories


  #Normalize BRAND_POPULARITY
  max_brand_popularity_Nykaa = 0
  max_brand_popularity_Men = 0
  for k,v in brand_popularity.items():
    if k =='Nykaa Cosmetics':
      continue
    max_brand_popularity_Nykaa = max(max_brand_popularity_Nykaa, v.get(NYKAA))
    if v.get(MEN):
      max_brand_popularity_Men = max(max_brand_popularity_Men, v.get(MEN))

  for k,v in brand_popularity.items():
    brand_popularity[k][NYKAA] = brand_popularity[k][NYKAA] / max_brand_popularity_Nykaa * 100 * 2 + 100
    if v.get(MEN):
      brand_popularity[k][MEN] = brand_popularity[k][MEN] / max_brand_popularity_Men * 100 * 2 + 100

  return brand_category_mappings


def saveMappings(brand_category_mappings):
  print("Saving brand category mappings in DB..")
  mysql_conn = Utils.mysqlConnection('w')
  cursor = mysql_conn.cursor()

  Utils.mysql_write("delete from brands", connection = mysql_conn)

  query = "REPLACE INTO brands (brand, brand_id, brands_v1, brand_popularity, brand_popularity_men, top_categories, top_categories_men, brand_url, brand_men_url) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "

  num_brands_skipped = 0
  num_brands_processed= 0
  for brand, categories in brand_category_mappings.items():
    brand = brand.replace("’", "'")
    sorted_categories = sorted(categories.items(), key=lambda x : -x[1][NYKAA])
    sorted_categories_men = sorted(categories.items(), key=lambda x : -x[1][MEN])
    top_categories = []
    top_categories_str = ""
    top_categories_men = []
    top_categories_men_str = ""
    try:
      category_names_added_yet_nykaa = set()
      for k in sorted_categories:
        if cat_id_index.get(k[0]):
          name = cat_id_index[k[0]]['name']
          _id = k[0]
          url = brand_name_id[brand]['brand_url'] + "?cat=%s" % _id
          if name not in category_names_added_yet_nykaa:
            top_categories.append({"category": name, "category_id": _id, "category_url": url})
            category_names_added_yet_nykaa.add(name)

        if len(top_categories) <= 5:
          top_categories_str = json.dumps(top_categories)
        if len(top_categories) == 5:
          break

      category_names_added_yet_men = set()
      for k in sorted_categories_men:
        if cat_id_index.get(k[0]):
          name = cat_id_index[k[0]]['name']
          _id = k[0]
          url = brand_name_id[brand]['brand_men_url'] + "?cat=%s" % _id
          if name not in category_names_added_yet_men:
            top_categories_men.append({"category": name, "category_id": _id, "category_url": url})
            category_names_added_yet_men.add(name)

        if len(top_categories_men) <= 5:
          top_categories_men_str = json.dumps(top_categories_men)
        if len(top_categories_men) == 5:
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

    values = (brand_name_name[brand], brand_name_id[brand]['brand_id'], brand_name_id[brand]['brands_v1'],
              brand_popularity[brand][NYKAA], brand_popularity[brand][MEN], top_categories_str, top_categories_men_str,
              brand_name_id[brand]['brand_url'], brand_name_id[brand]['brand_men_url'])
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
    Utils.mysql_write("create table brand_category(brand varchar(30), brand_id varchar(32), category_id varchar(32), category_name varchar(32), popularity float, popularity_men float)")
  Utils.mysql_write("delete from brand_category")

  max_pop_Nykaa = 0
  max_pop_Men = 0
  for brand, catinfo in brand_cat_popularity.items():
    for category_id, pop in sorted(catinfo.items(), key=lambda x: -x[1][NYKAA]):
      max_pop_Nykaa = max(pop[NYKAA], max_pop_Nykaa)
      break
    for category_id, pop in sorted(catinfo.items(), key=lambda x: -x[1][MEN]):
      max_pop_Men = max(pop[MEN], max_pop_Men)
      break

  query = "REPLACE INTO brand_category (brand, brand_id, category_id, category_name, popularity, popularity_men) VALUES ('%s', '%s', '%s', '%s', %s, %s) "
  for brand, catinfo in brand_cat_popularity.items():
    for category_id, pop in catinfo.items():
      try:
        pop[NYKAA] = round(pop[NYKAA] / max_pop_Nykaa * 100, 2)
        pop[MEN] = round(pop[MEN] / max_pop_Men * 100, 2) if max_pop_Men > 0 else 0
        brand_id = None
        if brand in brand_name_id:
          brand_id = brand_name_id[brand]['brand_id']
        if cat_id_index[category_id]:
          category_name = cat_id_index[category_id]['name']
          brand_name = brand_name_name[brand] if brand in brand_name_name else brand
          q = query % (brand_name.replace("'", "''"), brand_id, category_id, category_name.replace("'", "''"), pop[NYKAA], pop[MEN])
          # print(q)
          Utils.mysql_write(q)
      except:
        print(traceback.format_exc())

def update_brand_category_facets_table():
#  assert BRAND_CAT_POPULARITY
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
      arr.append((brand.replace("'", "''"), brand_id, category_id, category_name.replace("'", "''"), facet, popularity))
  
  print("Writing into mysql .. ")
  query = "REPLACE INTO brand_category_facets (brand, brand_id, category_id, category_name, facet, popularity) VALUES ('%s', '%s', '%s', '%s', '%s', %s) "
  for brand, brand_id, category_id, category_name, facet, popularity in arr:
    popularity = popularity/ max_pop * 50 
    q = query % (brand, brand_id, category_id, category_name, facet, popularity)
    Utils.mysql_write(q)
    print(q)

def getAggQueryResult(facet1, facet2):
  key1 = facet1 + ".keyword"
  key2 = facet2 + ".keyword"

  query = {
    "aggs": {
      "categories": {
        "terms": {
          "field": "category_ids.keyword",
          "size": 200
        },
        "aggs": {
          facet1: {"terms": {"field": key1, "size": 100}, "aggs": {
            "catalog": {"terms": {"field": "catalog_tag.keyword", "size": 100},
                        "aggs": {"popularity_sum": {"sum": {"field": "popularity"}}}}}},
          facet2: {"terms": {"field": key2, "size": 100}, "aggs": {
            "catalog": {"terms": {"field": "catalog_tag.keyword", "size": 100},
                        "aggs": {"popularity_sum": {"sum": {"field": "popularity"}}}}}}
        }
      }
    },
    "size": 0
  }
  results = es.search(index='livecore', body=query, request_timeout=120)
  return results

def getFacetPopularityArray(results, max_pop, max_pop_men):
  tempArr = []
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
        popularity = 0
        popularity_men = 0
        for catalog in facet_bucket['catalog']['buckets']:
          if (catalog['key'] == 'nykaa'):
            popularity = catalog['popularity_sum']['value']
          elif (catalog['key'] == 'men'):
            popularity_men = catalog['popularity_sum']['value']
        max_pop = max(max_pop, popularity)
        max_pop_men = max(max_pop_men, popularity_men)
        if facet_name in BLACKLISTED_FACETS or popularity < POPULARITY_THRESHOLD:
          is_good_facet = False
        if is_good_facet:
          tempArr.append(
            {'category_id': catbucket['key'], 'facet_name': facet_name, 'facet_val': name, 'popularity': popularity,
             'popularity_men': popularity_men})
  return tempArr, max_pop, max_pop_men

def update_category_facets_table():
  if not Utils.mysql_read("SHOW TABLES LIKE 'category_facets'"):
    Utils.mysql_write("create table category_facets(category_id varchar(32), category_name varchar(32), facet_name varchar(20), facet_val varchar(20), popularity float)")
  Utils.mysql_write("delete from category_facets")

  arr = []
  max_pop = 0
  max_pop_men = 0

  results = getAggQueryResult("benefits_facet", "color_facet")
  tempArr, max_pop, max_pop_men = getFacetPopularityArray(results, max_pop, max_pop_men)
  arr.extend(tempArr)

  results = getAggQueryResult("concern_facet", "coverage_facet")
  tempArr, max_pop, max_pop_men = getFacetPopularityArray(results, max_pop, max_pop_men)
  arr.extend(tempArr)

  results = getAggQueryResult("finish_facet", "formulation_facet")
  tempArr, max_pop, max_pop_men = getFacetPopularityArray(results, max_pop, max_pop_men)
  arr.extend(tempArr)

  results = getAggQueryResult("gender_facet", "hair_type_facet")
  tempArr, max_pop, max_pop_men = getFacetPopularityArray(results, max_pop, max_pop_men)
  arr.extend(tempArr)

  results = getAggQueryResult("filter_size_facet", "speciality_search_facet")
  tempArr, max_pop, max_pop_men = getFacetPopularityArray(results, max_pop, max_pop_men)
  arr.extend(tempArr)

  results = getAggQueryResult("filter_product_facet", "usage_period_facet")
  tempArr, max_pop, max_pop_men = getFacetPopularityArray(results, max_pop, max_pop_men)
  arr.extend(tempArr)

  results = getAggQueryResult("spf_facet", "preference_facet")
  tempArr, max_pop, max_pop_men = getFacetPopularityArray(results, max_pop, max_pop_men)
  arr.extend(tempArr)

  results = getAggQueryResult("skin_tone_facet", "skin_type_facet")
  tempArr, max_pop, max_pop_men = getFacetPopularityArray(results, max_pop, max_pop_men)
  arr.extend(tempArr)

  for row in arr:
    try:
      category_name = cat_id_index[row['category_id']]['name']
    except:
      print("Skipping category_id %s in category_facets" % row['category_id'])
      continue

    query = "REPLACE INTO category_facets (category_id, category_name, facet_name, facet_val, popularity, popularity_men) VALUES ('%s', '%s', '%s', '%s', %s, %s) "
    popularity = row['popularity']/ max_pop * WEIGHT_CATEGORY_FACET
    popularity_men = row['popularity_men']/max_pop_men * WEIGHT_CATEGORY_FACET if max_pop_men > 0 else 0
    query = query % (row['category_id'], category_name, row['facet_name'], row['facet_val'].strip().replace("'", "''"), popularity, popularity_men)
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
