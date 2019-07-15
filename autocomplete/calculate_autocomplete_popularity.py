import sys
import pandas as pd
from pandas.io import sql

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/nykaa/scripts/sharedutils")
from esutils import EsUtils

VALID_CATALOG_TAGS = ['nykaa', 'men', 'luxe', 'pro']

def create_category_info():
  print("retrieving data")
  magento_db = PasUtils.nykaaMysqlConnection()
  category_data = pd.read_sql("select category_id, parent_id, name, depth, catalog_tag from nk_categories", con=magento_db)
  print("retrieved")
  product_l1 = category_data.loc[category_data['depth'] == 0]
  product_l2 = category_data.loc[category_data['depth'] == 1]
  product_l3 = category_data.loc[category_data['depth'] == 2]
  product_l4 = category_data.loc[category_data['depth'] == 3]

  product_l1.rename(columns={'category_id': 'l1_id', 'parent_id': 'l1_parent_id', 'name': 'l1_name', 'catalog_tag': 'l1_catalog_tag'}, inplace=True)
  product_l2.rename(columns={'category_id': 'l2_id', 'parent_id': 'l2_parent_id', 'name': 'l2_name', 'catalog_tag': 'l2_catalog_tag'}, inplace=True)
  product_l3.rename(columns={'category_id': 'l3_id', 'parent_id': 'l3_parent_id', 'name': 'l3_name', 'catalog_tag': 'l3_catalog_tag'}, inplace=True)
  product_l4.rename(columns={'category_id': 'l4_id', 'parent_id': 'l4_parent_id', 'name': 'l4_name', 'catalog_tag': 'l4_catalog_tag'}, inplace=True)
  print("merging")
  df = pd.merge(product_l1, product_l2, how='left', left_on=['l1_id'],right_on=['l2_parent_id'])
  df = pd.merge(df, product_l3, how='left', left_on=['l2_id'], right_on=['l3_parent_id'])
  df = pd.merge(df, product_l4, how='left', left_on=['l3_id'], right_on=['l4_parent_id'])
  print(df)
  # df = df[['l1_name', 'l2_name', 'l3_name', 'l4_name', 'l1_id', 'l2_id', 'l3_id', 'l4_id', 'catalog_tag']]
  df.l1_id = df.l1_id.fillna(0)
  df.l2_id = df.l2_id.fillna(0)
  df.l3_id = df.l3_id.fillna(0)
  df.l4_id = df.l4_id.fillna(0)

  df = df.astype({'l2_id': int, 'l3_id': int, 'l4_id': int})
  print("writing")
  df.to_csv('category_mapping.csv', index=False)
  magento_db.close()
  return
  
  
def get_category_data():
  query = """select distinct l3_id as category_id, l3_name as category_name from product_category_mapping
              where ( l1_id not in (77,194,9564,7287,3048)
                and lower(l2_name) not like '%shop by%'
                and l3_id not in (4036,3746,3745,3819,6620,6621)
                  or l2_id in (9614, 1286))"""
  nykaa_redshift_connection = PasUtils.redshiftConnection()
  valid_categories = pd.read_sql(query, con=nykaa_redshift_connection)

  query = """SELECT DISTINCT category_id, request_path AS category_url FROM nykaalive1.core_url_rewrite
                WHERE product_id IS NULL AND category_id IS NOT NULL"""
  nykaa_replica_db_conn = PasUtils.nykaaMysqlConnection()
  category_url_info = pd.read_sql(query, con=nykaa_replica_db_conn)
  
  category_data = pd.merge(valid_categories, category_url_info, on="category_id")
  return category_data


def get_popularity_data_from_es(valid_category_list):
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
  

def process_category(category_data):
  data = {}
  data['category_id'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []
  
  for category in category_data:
    popularity_data = {'category_id': category.get('key', 0)}
    for bucket in category.get('tags', {}).get('buckets', []):
      popularity_data[bucket.get('key')] = bucket.get('popularity_sum', {}).get('value', 0)
    
    data['category_id'].append(popularity_data.get('category_id'))
    for tag in VALID_CATALOG_TAGS:
      data[tag].append(popularity_data.get(tag, 0))
  
  category_popularity = pd.DataFrame.from_dict(data)
  category_popularity.to_csv('category_pop.csv', index=False)
  return category_popularity


def process_brand(brand_data):
  data = {}
  data['brand_id'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []
  
  for brand in brand_data:
    popularity_data = {'brand_id': brand.get('key', 0)}
    for bucket in brand.get('tags', {}).get('buckets', []):
      popularity_data[bucket.get('key')] = bucket.get('popularity_sum', {}).get('value', 0)
    
    data['brand_id'].append(popularity_data.get('brand_id'))
    for tag in VALID_CATALOG_TAGS:
      data[tag].append(popularity_data.get(tag, 0))
  
  brand_popularity = pd.DataFrame.from_dict(data)
  brand_popularity.to_csv('brand_pop.csv', index=False)
  return brand_popularity


def process_brand_category(brand_category_data):
  data = {}
  data['brand_id'] = []
  data['category_id'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []
  
  for category in brand_category_data:
    for brand in category.get('brands', {}).get('buckets', []):
      popularity_data = {'category_id': category.get('key', 0), 'brand_id': brand.get('key', 0)}
      for bucket in brand.get('tags', {}).get('buckets', []):
        popularity_data[bucket.get('key')] = bucket.get('popularity_sum', {}).get('value', 0)
      data['category_id'].append(popularity_data.get('category_id'))
      data['brand_id'].append(popularity_data.get('brand_id'))
      for tag in VALID_CATALOG_TAGS:
        data[tag].append(popularity_data.get(tag, 0))
  
  brand_category_popularity = pd.DataFrame.from_dict(data)
  brand_category_popularity.to_csv('brand_category_popularity.csv', index=False)
  return brand_category_popularity
  

def calculate_popularity_autocomplete():
  global category_info
  valid_category_list = list(category_info.category_id.values)
  valid_category_list = [int(id) for id in valid_category_list]
  category_data, brand_data, brand_category_data = get_popularity_data_from_es(valid_category_list)
  category_popularity = process_category(category_data)
  brand_popularity = process_brand(brand_data)
  brand_category_popularity = process_brand_category(brand_category_data)
  
  
  

category_info = get_category_data()
if __name__ == "__main__":
  # create_category_info()
  calculate_popularity_autocomplete()