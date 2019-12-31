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


def normalize(a):
  if max(a) == 0:
    return a
  return (a - min(a)) / (max(a) - min(a))


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
    