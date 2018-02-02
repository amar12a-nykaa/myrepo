import sys
from pymongo import MongoClient

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

if __name__ == '__main__':
  print(get_popularity_for_id('111405', parent_id='111417'))
  
  print(get_popularity_for_id('7410', parent_id='7412'))
  print(get_popularity_for_id('157885', parent_id='157885'))
  print(get_popularity_for_id('157885', parent_id='157885'))
  print(get_popularity_for_id('31074', parent_id='31074'))
