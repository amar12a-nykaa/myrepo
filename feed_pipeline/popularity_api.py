import sys
from pymongo import MongoClient
from IPython import embed

client = MongoClient()
popularity_table = client['search']['popularity']

def get_popularity_for_id(product_id, parent_id=None):
  if product_id != parent_id:
    return {}
  
  res = list(popularity_table.find({"_id": product_id}))
  ret = {}
  for obj in res:
    views = 0
    if obj.get('views'):
      conversion =  1.0 * obj.get('cart_additions', 0) / obj['views'] * 100 
      views = 0 if conversion < 0.05 else obj['views']

    ret[obj['_id']] = {
      "popularity":obj['popularity'], 
      "views": views,
    }

  parent_obj = ret.pop(parent_id, None)

  if not ret.get(product_id) and parent_obj:
    ret[product_id] = parent_obj

  return ret

def validate_popularity_data_health():
  count = popularity_table.count()
  assert count > 55000, "Number of products is less than 55000." 

  count_non_zero_popularity = popularity_table.count({"popularity": {"$gt": 0}})
  assert count_non_zero_popularity > 55000, "Number of products is less than 55000." 

  print("Popularity data looks good. Validation successful")
  
if __name__ == '__main__':
  print(get_popularity_for_id('111405', parent_id='111417'))
  print(get_popularity_for_id('7410', parent_id='7412'))
  print(get_popularity_for_id('157885', parent_id='157885'))
  print(get_popularity_for_id('157885', parent_id='157885'))
  print(get_popularity_for_id('31074', parent_id='31074'))
  validate_popularity_data_health()
