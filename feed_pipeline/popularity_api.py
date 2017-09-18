import sys
from pymongo import MongoClient

client = MongoClient()
popularity_table = client['search']['popularity']

def get_popularity_for_id(product_id, parent_id=None):
  """Returns popularity of product. If product is not a parent product, parent product can be passed to get its popularity """
  res = list(popularity_table.find({"_id": {"$in": [product_id, parent_id]}}))
  ret = {}
  for obj in res:
    views = 0
    if obj.get('views'):
      conversion =  1.0 * obj.get('cart_additions', 0) / obj['views'] * 100 
      views = 0 if conversion < 0.05 else obj['views']

    ret[obj['_id']] = {"popularity":obj['popularity'], "views": views}

  if parent_id and parent_id in ret:
    ret[product_id] = ret.pop(parent_id)

  return ret
def get_popularity_for_ids(product_ids):
  assert isinstance(product_ids, list)
  res = list(popularity_table.find({"_id": {"$in": product_ids}}))
  ret = {}
  for obj in res:
    views = 0
    if obj.get('views'):
      conversion =  1.0 * obj.get('cart_additions', 0) / obj['views'] * 100 
      views = 0 if conversion < 0.05 else obj['views']

    ret[obj['_id']] = {"popularity":obj['popularity'], "views": views}

  #ret = {obj['_id']:{"popularity":obj['popularity'], "views": views} for obj in res}
  return ret

if __name__ == '__main__':
  print(get_popularity_for_id('111405', parent_id='111417'))
  print(get_popularity_for_ids(['58544', '14862','14927', '7352']))
  #print(get_views_for_ids(['58544', '14862','14927', '7352']))
  
