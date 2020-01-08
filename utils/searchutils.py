import json

VALID_CATALOG_TAGS = ['nykaa', 'men', 'luxe', 'pro', 'ultra_lux']
PRIVATE_LABEL_BRANDS = ['1937','7666','9127']
AUTOCOMPLETE_BRAND_BOOST_FACTOR = 1.1
BLACKLISTED_FACETS = ['old_brand_facet', ]
BRAND_EXCLUDE_LIST = ['9817']
POPULARITY_THRESHOLD = 0.1
BASE_AGGREGATION = {
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
BASE_AGGREGATION_TOP_HITS = {
  "tags": {
    "terms": {
      "field": "catalog_tag.keyword",
      "include": VALID_CATALOG_TAGS,
      "size": 10
    },
    "aggs": {
      "top_popularity": {
        "top_hits": {
          "size": 10,
          "sort": [{"popularity": {"order": "desc"}}],
          "_source": ["popularity"]
        }
      }
    }
  }
}

def normalize(a):
  if max(a) == 0:
    return a
  return (a - min(a)) / (max(a) - min(a))


def get_avg_bucket_popularity(bucket):
  data = bucket.get('top_popularity', {}).get('hits', {}).get('hits', [])
  cnt = 0
  pop_sum = 0
  for popularity in data:
    pop_sum = pop_sum + round(popularity.get('_source', {}).get('popularity', 0), 4)
    cnt = cnt + 1
  if cnt > 0:
    return pop_sum/cnt
  else:
    return 0
  
  
class StoreUtils(object):
  
  def check_base_popularity(row):
    for tag in VALID_CATALOG_TAGS:
      if not row["valid_" + tag]:
        row[tag] = 0
    return row
  
  def get_store_popularity_str(row):
    data = {}
    for tag in VALID_CATALOG_TAGS:
      data[tag] = row.get(tag, 0)
    return json.dumps(data)
    