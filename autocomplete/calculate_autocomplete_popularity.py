import sys
import pandas as pd
import json

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/nykaa/scripts/sharedutils")
from esutils import EsUtils
from loopcounter import LoopCounter
from idutils import strip_accents

VALID_CATALOG_TAGS = ['nykaa', 'men', 'luxe', 'pro']
PRIVATE_LABEL_BRANDS = ['1937','7666','9127']
BOOST_FACTOR = 1.1
BLACKLISTED_FACETS = ['old_brand_facet', ]
BRAND_EXCLUDE_LIST = ['1172', '1257']
POPULARITY_THRESHOLD = 0.1
base_aggregation = {
    "tags": {
      "terms": {
        "field": "catalog_tag.keyword",
        "include": VALID_CATALOG_TAGS,
        "size": 10
      },
      "aggs": {
        "popularity_sum": {
          "sum": {"field": "popularity"}
        }
      }
    }
  }


def normalize(a):
  if max(a) == 0:
    return a
  return (a-min(a))/(max(a)-min(a))


def check_base_popularity(row):
  for tag in VALID_CATALOG_TAGS:
    if not row["valid_"+tag]:
      row[tag] = 0
  return row
  

def get_store_popularity_str(row):
  data = {}
  for tag in VALID_CATALOG_TAGS:
    data[tag] = row.get(tag, 0)
  return json.dumps(data)


def get_category_data():
  query = """select distinct l3_id as category_id, l3_name as category_name from product_category_mapping
              where ( l1_id not in (77,194,9564,7287,3048, 5926)
                and lower(l2_name) not like '%shop by%'
                and l3_id not in (4036,3746,3745,3819,6620,6621)
                  or l2_id in (9614, 1286))"""
  nykaa_redshift_connection = PasUtils.redshiftConnection()
  valid_categories = pd.read_sql(query, con=nykaa_redshift_connection)

  query = """SELECT DISTINCT category_id, request_path AS category_url FROM nykaalive1.core_url_rewrite
                WHERE product_id IS NULL AND category_id IS NOT NULL"""
  nykaa_replica_db_conn = PasUtils.nykaaMysqlConnection()
  category_url_info = pd.read_sql(query, con=nykaa_replica_db_conn)
  category_url_info = category_url_info.drop_duplicates(subset=['category_id'], keep='first', inplace=False)
  
  category_data = pd.merge(valid_categories, category_url_info, on="category_id")
  category_data = category_data.astype({'category_id': str})
  category_data.to_csv('category_data.csv', index=False)
  return category_data


def get_brand_data():
  brand_info = {}
  nykaa_replica_db_conn = PasUtils.nykaaMysqlConnection(force_production=True)
  query = """select distinct name, id, url, brands_v1
              FROM(
                 SELECT nkb.name AS name, nkb.brand_id AS id, cur.request_path AS url, ci.brand_id AS brands_v1
                 FROM nk_brands nkb
                 INNER JOIN nykaalive1.core_url_rewrite cur ON nkb.brand_id=cur.category_id
                 INNER JOIN nykaalive1.category_information ci ON cur.category_id=ci.cat_id
                 WHERE cur.product_id IS NULL
              )A"""
  results = PasUtils.fetchResults(nykaa_replica_db_conn, query)
  for brand in results:
    brand_name = brand['name'].replace("’", "'")
    brand_name = strip_accents(brand_name)
    if brand_name is not None:
      brand_upper = brand_name.strip()
      brand_info[str(brand['id'])] = {'brand_url': brand['url'], 'brands_v1': brand['brands_v1'], 'brand_name': brand_upper}
  nykaa_replica_db_conn.close()
  return brand_info


def get_popularity_data_from_es(valid_category_list):
  global base_aggregation
  query = {
    "size": 0,
    "aggs": {
      "category_data": {
        "terms": {
          "field": "category_ids.keyword",
          "include": valid_category_list,
          "size": 10000
        },
        "aggs": base_aggregation
      },
      "brand_data": {
        "terms": {
          "field": "brand_ids.keyword",
          "exclude": BRAND_EXCLUDE_LIST,
          "size": 10000
        },
        "aggs": base_aggregation
      },
      "brand_category_data": {
        "terms": {
          "field": "category_ids.keyword",
          "include": valid_category_list,
          "size": 10000
        },
        "aggs" : {
          "brands" : {
            "terms": {
              "field": "brand_ids.keyword",
              "exclude": BRAND_EXCLUDE_LIST,
              "size": 10000
            },
            "aggs": base_aggregation
          }
        }
      }
    }
}
  es = EsUtils.get_connection()
  results = es.search(index='livecore', body=query, request_timeout=120)
  aggregation = results["aggregations"]
  return aggregation["category_data"]["buckets"], aggregation["brand_data"]["buckets"], aggregation["brand_category_data"]["buckets"]


def process_category_facet_popularity(valid_category_list):
  global category_info
  
  data = {}
  data['category_id'] = []
  data['facet_name'] = []
  data['facet_val'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []
  
  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "color_facet"), data)
  
  facet_popularity = pd.DataFrame.from_dict(data)
  for tag in VALID_CATALOG_TAGS:
    facet_popularity[tag] = 100 * normalize(facet_popularity[tag])
  facet_popularity.to_csv('facet_pop.csv', index=False)
  
  print("writing facet popularity to db")
  mysql_conn = PasUtils.mysqlConnection('w')
  cursor = mysql_conn.cursor()
  if not PasUtils.mysql_read("SHOW TABLES LIKE 'category_facets'"):
    PasUtils.mysql_write("""create table category_facets(brand varchar(30), brand_id varchar(32), category_id varchar(32),
                                category_name varchar(32), facet varchar(20), popularity float, store_popularity varchar(255))""")
  PasUtils.mysql_write("delete from category_facets", connection=mysql_conn)
  
  query = """REPLACE INTO category_facets (category_id, category_name, facet_name, facet_val, popularity, store_popularity) VALUES (%s, %s, %s, %s, %s, %s) """
  data = pd.merge(facet_popularity, category_info, on='category_id')
  print(data)
  ctr = LoopCounter(name='Writing category popularity to db', total=len(data.index))
  for id, row in data.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    
    row = dict(row)
    values = (row['category_id'], row['category_name'], row['facet_name'], row['facet_val'], row['nykaa'], get_store_popularity_str(row))
    cursor.execute(query, values)
    mysql_conn.commit()
  
  cursor.close()
  mysql_conn.close()
  return facet_popularity


def process_category(category_data):
  global category_info
  data = {}
  data['category_id'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []
    data["valid_"+tag] = []
  
  for category in category_data:
    popularity_data = {'category_id': category.get('key', 0)}
    for bucket in category.get('tags', {}).get('buckets', []):
      popularity_data[bucket.get('key')] = round(bucket.get('popularity_sum', {}).get('value', 0), 4)
    
    data['category_id'].append(popularity_data.get('category_id'))
    for tag in VALID_CATALOG_TAGS:
      popularity = popularity_data.get(tag, -1)
      if popularity < 0:
        data[tag].append(0)
        data["valid_"+tag].append(False)
      else:
        data[tag].append(popularity)
        data["valid_" + tag].append(True)
  category_popularity = pd.DataFrame.from_dict(data)
  for tag in VALID_CATALOG_TAGS:
    category_popularity[tag] = 100 * normalize(category_popularity[tag]) + 100
  category_popularity = category_popularity.apply(check_base_popularity, axis=1)
  category_popularity.to_csv('category_pop.csv', index=False)
  
  print("inserting category data in db")
  mysql_conn = PasUtils.mysqlConnection('w')
  cursor = mysql_conn.cursor()
  PasUtils.mysql_write("delete from l3_categories", connection=mysql_conn)
  query = """REPLACE INTO l3_categories(id, name, url, category_popularity, store_popularity)
                  VALUES (%s, %s, %s, %s, %s)"""
  
  data = pd.merge(category_popularity, category_info, on='category_id')
  print(data)
  ctr = LoopCounter(name='Writing category popularity to db', total=len(data.index))
  for id, row in data.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    
    row = dict(row)
    values = (row['category_id'], row['category_name'], row['category_url'], row['nykaa'], get_store_popularity_str(row))
    cursor.execute(query, values)
    mysql_conn.commit()
  
  cursor.close()
  mysql_conn.close()
  
  PasUtils.mysql_write("""create or replace view l3_categories_clean as select * from l3_categories
                              where url not like '%luxe%' and url not like '%shop-by-concern%';""")
  return


def getAggQueryResult(valid_category_list, facet1):
  global base_aggregation
  key1 = facet1 + ".keyword"

  query = {
    "aggs": {
      "categories": {
        "terms": {
          "field": "category_ids.keyword",
          "include": valid_category_list,
          "size": 200
        },
        "aggs": {
          facet1: {"terms": {"field": key1, "size": 100}, "aggs": base_aggregation}
        }
      }
    },
    "size": 0
  }
  es = EsUtils.get_connection()
  results = es.search(index='livecore', body=query, request_timeout=120)
  return results['aggregations']['categories']['buckets']


def getFacetPopularityArray(results, data):
  is_good_facet = False
  for catbucket in results:
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
        popularity_data = {}
        for bucket in facet_bucket.get('tags', {}).get('buckets', []):
          popularity_data[bucket.get('key')] = round(bucket.get('popularity_sum', {}).get('value', 0), 4)
        
        if is_good_facet:
          data['category_id'].append(catbucket['key'])
          data['facet_name'].append(facet_name)
          data['facet_val'].append(name)
          for tag in VALID_CATALOG_TAGS:
            data[tag].append(popularity_data.get(tag, 0))
  return data


def process_brand(brand_data):
  data = {}
  data['brand_id'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []
    data["valid_"+tag] = []
  
  for brand in brand_data:
    popularity_data = {'brand_id': brand.get('key', 0)}
    for bucket in brand.get('tags', {}).get('buckets', []):
      popularity_data[bucket.get('key')] = round(bucket.get('popularity_sum', {}).get('value', 0), 4)
    
    data['brand_id'].append(popularity_data.get('brand_id'))
    for tag in VALID_CATALOG_TAGS:
      popularity = popularity_data.get(tag, -1)
      if popularity < 0:
        data[tag].append(0)
        data["valid_" + tag].append(False)
      else:
        data[tag].append(popularity)
        data["valid_" + tag].append(True)
  
  brand_popularity = pd.DataFrame.from_dict(data)
  for tag in VALID_CATALOG_TAGS:
    brand_popularity[tag] = 200 * normalize(brand_popularity[tag]) + 100
  brand_popularity = brand_popularity.apply(check_base_popularity, axis=1)
  brand_popularity.to_csv('brand_pop.csv', index=False)
  return brand_popularity


def process_brand_category(brand_category_data):
  global category_info
  global brand_info
  data = {}
  data['brand_id'] = []
  data['category_id'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []
  
  for category in brand_category_data:
    for brand in category.get('brands', {}).get('buckets', []):
      popularity_data = {'category_id': category.get('key', 0), 'brand_id': brand.get('key', 0)}
      for bucket in brand.get('tags', {}).get('buckets', []):
        popularity_data[bucket.get('key')] = round(bucket.get('popularity_sum', {}).get('value', 0), 4)
      data['category_id'].append(popularity_data.get('category_id'))
      data['brand_id'].append(popularity_data.get('brand_id'))
      for tag in VALID_CATALOG_TAGS:
        data[tag].append(popularity_data.get(tag, 0))
  
  brand_category_popularity = pd.DataFrame.from_dict(data)
  for tag in VALID_CATALOG_TAGS:
    brand_category_popularity[tag] = (50 * normalize(brand_category_popularity[tag])) + 50
    brand_category_popularity[tag] = brand_category_popularity[tag].apply(lambda x: x if x > 50.0 else 0)
  brand_category_popularity.to_csv('brand_category_popularity.csv', index=False)
  
  # promote private label
  def boost_brand(row):
    if str(row['brand_id']) in PRIVATE_LABEL_BRANDS:
      for tag in VALID_CATALOG_TAGS:
        row[tag] = BOOST_FACTOR * row[tag]
    return row
  brand_category_popularity = brand_category_popularity.apply(boost_brand, axis=1)
  
  print("writing brand category popularity to db")
  mysql_conn = PasUtils.mysqlConnection('w')
  cursor = mysql_conn.cursor()
  if not PasUtils.mysql_read("SHOW TABLES LIKE 'brand_category'"):
    PasUtils.mysql_write("""create table brand_category(brand varchar(30), brand_id varchar(32), category_id varchar(32),
                            category_name varchar(32), popularity float, store_popularity varchar(255))""")
  PasUtils.mysql_write("delete from brand_category", connection=mysql_conn)
  
  query = """REPLACE INTO brand_category (brand, brand_id, category_id, category_name, popularity, store_popularity)
              VALUES (%s, %s, %s, %s, %s, %s)"""
  brand_category_popularity = pd.merge(brand_category_popularity, category_info, on='category_id')
  print(brand_category_popularity)
  ctr = LoopCounter(name='Writing brand category popularity to db', total=len(brand_category_popularity.index))
  for id, row in brand_category_popularity.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    
    row = dict(row)
    if row['brand_id'] not in brand_info:
      print("brand %s not found in brand_info"%row['brand_id'])
      continue
    values = (brand_info[row['brand_id']]['brand_name'], row['brand_id'], row['category_id'], row['category_name'], row['nykaa'], get_store_popularity_str(row))
    cursor.execute(query, values)
    mysql_conn.commit()

  cursor.close()
  mysql_conn.close()
  
  return brand_category_popularity
  

def db_insert_brand(brand_popularity, top_category):
  global category_info
  global brand_info
  
  top_category_brand = {}
  for id, row in top_category.iterrows():
    row= dict(row)
    if row['brand_id'] not in brand_info:
      continue
    if row['brand_id'] not in top_category_brand:
      top_category_brand[row['brand_id']] = []
    url = brand_info[row['brand_id']]['brand_url'] + "?cat=%s" % row['category_id']
    top_category_brand[row['brand_id']].append({"category": row['category_name'], "category_id": row['category_id'], "category_url": url})
  
  print("writing brand popularity in db")
  mysql_conn = PasUtils.mysqlConnection('w')
  cursor = mysql_conn.cursor()
  PasUtils.mysql_write("delete from brands", connection=mysql_conn)
  query = """REPLACE INTO brands (brand, brand_id, brands_v1, brand_popularity, store_popularity,
              top_categories, brand_url) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
  ctr = LoopCounter(name='Writing brand popularity to db', total=len(brand_popularity.index))
  for id, row in brand_popularity.iterrows():
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
  
    row = dict(row)
    if row['brand_id'] not in brand_info:
      print("Skipping brand %s" % row['brand_id'])
      continue
    
    values = (brand_info[row['brand_id']]['brand_name'], row['brand_id'], brand_info[row['brand_id']]['brands_v1'], row["nykaa"],
              get_store_popularity_str(row), json.dumps(top_category_brand.get(row['brand_id'], [])), brand_info[row['brand_id']]['brand_url'])
    cursor.execute(query, values)
    mysql_conn.commit()

  cursor.close()
  mysql_conn.close()
    
  
def calculate_popularity_autocomplete():
  global category_info
  valid_category_list = list(category_info.category_id.values)
  valid_category_list = [int(_id) for _id in valid_category_list]
  print("getting popularity from es")
  category_data, brand_data, brand_category_data = get_popularity_data_from_es(valid_category_list)
  print("processing category")
  process_category(category_data)
  print("processing brand")
  brand_popularity = process_brand(brand_data)
  print("processing brand category")
  brand_category_popularity = process_brand_category(brand_category_data)
  print("getting top category")
  top_category = brand_category_popularity.sort_values('nykaa', ascending=False).groupby('brand_id').head(5)
  top_category.to_csv('top_category.csv', index=False)
  db_insert_brand(brand_popularity, top_category)
  print("processing category facet")
  process_category_facet_popularity(valid_category_list)
  return
  

category_info = get_category_data()
brand_info = get_brand_data()
if __name__ == "__main__":
  calculate_popularity_autocomplete()