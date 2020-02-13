import json

# STORE_MAP = {'nykaa': {}, 'men': {'l1_id': 7287}, 'pro': {'l1_id': 5926}, 'ultra_lux': {'l1_id': 11723}, 'ngs': {'l1_id': 12390}}
STORE_MAP = {
  "nykaa": {
    "leaf_query": """(select distinct l4_name as category_name,l4_id as category_id 
    from ( 
      select * from product_category_mapping
        where l4_id <> 0 and ( l1_id not in (194,7287,5926,11723,12390)
            and lower(l2_name) not like '%shop by%'
            and lower(l2_name) not like '%trending%'
            and l3_id not in (4036,3746,3745,3819) 
        )and l3_id <> 1597
    ))
    union
    (
    select  distinct l3_name as category_name,l3_id as category_id from
    ( select * from product_category_mapping
        where l4_id = 0 and l3_id <> 0 and ( l1_id not in (194,7287,5926,11723,12390)
        and lower(l2_name) not like '%shop by%'
        and lower(l2_name) not like '%trending%'
        and l3_id not in (4036,3746,3745,3819)) and l3_id not in (44,3104,3055,3110,1415,328,6790,7010,8437,8404,1546,1306) and l2_id <>9633 
        and l3_id not in (select distinct l3_id from product_category_mapping where l4_id <>0 )
    ))
    union
    (
    select  distinct l2_name as category_name,l2_id as category_id from
    ( select *from product_category_mapping
    where l2_id <>0 and l3_id =0 and( l1_id not in (194,7287,5926,11723,12390)
    and lower(l2_name) not like '%shop by%'
    and lower(l2_name) not like '%trending%'
    and l3_id not in (4036,3746,3745,3819)
    )and l2_id not in (11111,9640,9639,9638)
     and
    l2_id not in
    (select distinct l2_id from product_category_mapping
    where l3_id <> 0
     ))
    )""",
    
    "non_leaf_query": """select distinct  l2_ID as category_id,l2_name as category_name  from
      (
      select * from product_category_mapping
      where (l1_id not in (77,194,9564,7287,3048,5926,11723,12390) 
            and lower(l2_name) not like '%shop by%' 
            and l3_id not in (4036,3746,3745,3819,1387)
            or l2_id in (9614,1286,6619,3053,3049,3050,9788,3054,3057,3052,1921))
      )
      where l3_id not in (0) and l2_id not in (735)
      union
      select distinct  l1_ID as category_id,l1_name as category_name  from
      (
      select * from product_category_mapping
      where l1_id in (24,3048,12,9564,671,1390,53,4362,8377,2313,77,59)
      )
      union
      select distinct  l3_ID as category_id,l3_name as category_name  from
      (
      select * from product_category_mapping
      where l3_id in (1597)
      )"""
  },
  
  "men": {
    "leaf_query": """select distinct  l2_ID as category_id,l2_name as category_name  from
        (
        select * from product_category_mapping
        where (l1_id not in (77,194,9564,7287,3048,5926,11723,12390) 
              and lower(l2_name) not like '%shop by%' 
              and l3_id not in (4036,3746,3745,3819,1387)
              or l2_id in (9614,1286,6619,3053,3049,3050,9788,3054,3057,3052,1921))
        )
        where l3_id not in (0) and l2_id not in (735)
        union
        select distinct  l1_ID as category_id,l1_name as category_name  from
        (
        select * from product_category_mapping
        where l1_id in (24,3048,12,9564,671,1390,53,4362,8377,2313,77,59)
        )
        union
        select distinct  l3_ID as category_id,l3_name as category_name  from
        (
        select * from product_category_mapping
        where l3_id in (1597)
        )""",
    
    "non_leaf_query": """(
        select distinct l2_ID as category_id,l2_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '7287'
        and lower(l3_name)  not like '%shop%'
        and lower(l2_name)  not like '%luxe%'
        and l4_id = 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l3_ID as category_id,l3_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '7287'
        and lower(l3_name)  not like '%shop%'
        and lower(l2_name)  not like '%luxe%'
        and l4_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))""",
    
    "l1_id": 7287
  },
  
  "pro": {
    "leaf_query": """(
        select distinct l3_ID as category_id,l3_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '5926'
        and l4_id = 0
        and l3_id<>0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l4_ID as category_id,l4_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '5926'
        and l4_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))""",
    
    "non_leaf_query": """(
        select distinct l2_ID as category_id,l2_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '5926'
        and l4_id = 0
        and l3_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l3_ID as category_id,l3_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '5926'
        and l4_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))""",
    
    "l1_id": 5926
  },
  
  "ngs": {
    "leaf_query": """(
        select distinct l3_ID as category_id,l3_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '12390'
        and l4_id = 0
        and l3_id<>0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        )
        union
        ( select distinct l4_ID as category_id,l4_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '12390'
        and l4_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        )))""",
    
    "non_leaf_query": """(
        select distinct l2_ID as category_id,l2_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '12390'
        and l4_id = 0
        and l3_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l3_ID as category_id,l3_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '12390'
        and l4_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l2_ID as category_id,l2_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '12390'
        and l4_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ));""",
    
    "l1_id": 12390
  },
  
  "ultra_lux": {
    "leaf_query": """(
        select distinct l3_ID as category_id,l3_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '11723'
        and lower(l2_name)  not like '%shop%'
        and lower(l2_name)  not like '%tren%'
        and l4_id = 0
        and l3_id<>0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l4_ID as category_id,l4_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '11723'
        and lower(l2_name)  not like '%shop%'
        and lower(l2_name)  not like '%tren%'
        and l4_id <> 0
        and l4_id <> 11747
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l2_ID as category_id,l2_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '11723'
        and l2_id in (11817,11819,11818)
        and lower(l2_name)  not like '%tren%'
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))""",
    
    "non_leaf_query": """(
        select distinct l2_ID as category_id,l2_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '11723'
        and lower(l2_name)  not like '%shop%'
        and lower(l2_name)  not like '%trending%'
        and l4_id = 0
        and l3_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l3_ID as category_id,l3_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '11723'
        and lower(l2_name)  not like '%shop%'
        and lower(l2_name)  not like '%trending%'
        and l4_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))
        union
        ( select distinct l2_ID as category_id,l2_name as category_name from
        (
        select L1_NAME,L2_NAME,l3_name,l4_name,L1_ID,L2_ID,l3_ID,l4_ID,count(*) from product_category_mapping 
        where l1_id = '11723'
        and lower(l2_name)  not like '%shop%'
        and lower(l2_name)  not like '%trending%'
        and l4_id <> 0
        group by L1_ID,L2_ID,l3_ID,l4_ID,L1_NAME,L2_NAME,l3_name,l4_name
        ))""",
    
    "l1_id": 11723
    
  }
}

VALID_CATALOG_TAGS = list(STORE_MAP.keys())
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
  
  def get_store_popularity_str(row, is_brand=False):
    data = {}
    store = row.get('store', 'nykaa')
    for tag in VALID_CATALOG_TAGS:
      data[tag] = row.get(tag, 0)
      if not is_brand and store != tag:
        data[tag] = 0
    return json.dumps(data)
    