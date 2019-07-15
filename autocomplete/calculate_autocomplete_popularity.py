import sys
import pandas as pd
import json

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/nykaa/scripts/sharedutils")
from esutils import EsUtils
from loopcounter import LoopCounter

VALID_CATALOG_TAGS = ['nykaa', 'men', 'luxe', 'pro']
BLACKLISTED_FACETS = ['old_brand_facet', ]
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
  

def normalize(a):
  return (a-min(a))/(max(a)-min(a))


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


def getAggQueryResult(valid_category_list, facet1, facet2):
  global base_aggregation
  key1 = facet1 + ".keyword"
  key2 = facet2 + ".keyword"

  query = {
    "aggs": {
      "categories": {
        "terms": {
          "field": "category_ids.keyword",
          "include": valid_category_list,
          "size": 200
        },
        "aggs": {
          facet1: {"terms": {"field": key1, "size": 100}, "aggs": base_aggregation},
          facet2: {"terms": {"field": key2, "size": 100}, "aggs": base_aggregation}
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


def get_category_facet_popularity(valid_category_list):
  data = {}
  data['category_id'] = []
  data['facet_name'] = []
  data['facet_val'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []

  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "benefits_facet", "color_facet"), data)
  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "concern_facet", "coverage_facet"), data)
  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "finish_facet", "formulation_facet"), data)
  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "gender_facet", "hair_type_facet"), data)
  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "filter_size_facet", "speciality_search_facet"), data)
  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "filter_product_facet", "usage_period_facet"), data)
  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "spf_facet", "preference_facet"), data)
  data = getFacetPopularityArray(getAggQueryResult(valid_category_list, "skin_tone_facet", "skin_type_facet"), data)
  
  facet_popularity = pd.DataFrame.from_dict(data)
  for tag in VALID_CATALOG_TAGS:
    facet_popularity[tag] = 100 * normalize(facet_popularity[tag])
  facet_popularity.to_csv('facet_pop.csv', index=False)
  return facet_popularity
  
  
def process_category(category_data):
  global category_info
  data = {}
  data['category_id'] = []
  for tag in VALID_CATALOG_TAGS:
    data[tag] = []
  
  for category in category_data:
    popularity_data = {'category_id': category.get('key', 0)}
    for bucket in category.get('tags', {}).get('buckets', []):
      popularity_data[bucket.get('key')] = round(bucket.get('popularity_sum', {}).get('value', 0), 4)
    
    data['category_id'].append(popularity_data.get('category_id'))
    for tag in VALID_CATALOG_TAGS:
      data[tag].append(popularity_data.get(tag, 0))
  
  category_popularity = pd.DataFrame.from_dict(data)
  for tag in VALID_CATALOG_TAGS:
    category_popularity[tag] = 100 * normalize(category_popularity[tag]) + 100
    category_popularity[tag] = category_popularity[tag].apply(lambda x: x if x > 100.0 else 0)
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
      popularity_data[bucket.get('key')] = round(bucket.get('popularity_sum', {}).get('value', 0), 4)
    
    data['brand_id'].append(popularity_data.get('brand_id'))
    for tag in VALID_CATALOG_TAGS:
      data[tag].append(popularity_data.get(tag, 0))
  
  brand_popularity = pd.DataFrame.from_dict(data)
  for tag in VALID_CATALOG_TAGS:
    brand_popularity[tag] = 200 * normalize(brand_popularity[tag]) + 100
    brand_popularity[tag] = brand_popularity[tag].apply(lambda x: x if x > 100.0 else 0)
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
        popularity_data[bucket.get('key')] = round(bucket.get('popularity_sum', {}).get('value', 0), 4)
      data['category_id'].append(popularity_data.get('category_id'))
      data['brand_id'].append(popularity_data.get('brand_id'))
      for tag in VALID_CATALOG_TAGS:
        data[tag].append(popularity_data.get(tag, 0))
  
  brand_category_popularity = pd.DataFrame.from_dict(data)
  for tag in VALID_CATALOG_TAGS:
    brand_category_popularity[tag] = 100 * normalize(brand_category_popularity[tag])
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
  category_facet_popularity = get_category_facet_popularity(valid_category_list)

  
  

category_info = get_category_data()
if __name__ == "__main__":
  # create_category_info()
  calculate_popularity_autocomplete()