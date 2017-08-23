import sys
from pymongo import MongoClient

client = MongoClient()
popularity_table = client['search']['popularity']

def get_popularity_for_ids(product_ids):
  assert isinstance(product_ids, list)
  res = list(popularity_table.find({"_id": {"$in": product_ids}}, {"popularity":1}))

  ret = {obj['_id']:{"popularity":obj['popularity']} for obj in res}
  return ret

def get_views_for_ids(product_ids):
  assert isinstance(product_ids, list)
  res = list(popularity_table.find({"_id": {"$in": product_ids}}, {"views":1}))

  ret = {obj['_id']:{"views":obj['views']} for obj in res}
  return ret

if __name__ == '__main__':
  print(get_popularity_for_ids(['58544', '14862','14927', '7352']))
  print(get_views_for_ids(['58544', '14862','14927', '7352']))
  
