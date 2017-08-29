import sys
from pymongo import MongoClient

client = MongoClient()
popularity_table = client['search']['popularity']

def get_popularity_for_ids(product_ids):
  assert isinstance(product_ids, list)
  res = list(popularity_table.find({"_id": {"$in": product_ids}}))
  ret = {}
  for obj in res:
    if obj.get('views'):
      conversion =  1.0 * obj.get('cart_additions', 0) / obj['views'] * 100 
      views = 0 if conversion < 0.05 else views
    else:
      views = 0
    ret[obj['_id']] = {"popularity":obj['popularity'], "views": views}

  #ret = {obj['_id']:{"popularity":obj['popularity'], "views": views} for obj in res}
  return ret

if __name__ == '__main__':
  print(get_popularity_for_ids(['58544', '14862','14927', '7352']))
  #print(get_views_for_ids(['58544', '14862','14927', '7352']))
  
