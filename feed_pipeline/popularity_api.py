import sys

from IPython import embed

sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

client = MongoUtils.getClient()
popularity_table = client['search']['popularity']
bestseller_data = client['search']['bestseller_data ']

def get_popularity_for_id(product_id, parent_id=None):
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
      "popularity_recent":obj['popularity_recent'],
    }

  # parent_obj = ret.pop(parent_id, None)
  #
  # if not ret.get(product_id) and parent_obj:
  #   ret[product_id] = parent_obj

  return ret

def get_bestseller_products():
  product_list = list(bestseller_data.find({},{"_id": 1}))
  res = []
  for obj in product_list:
    res.append(obj.get('_id'))
  return res


def validate_popularity_data_health():
  count = popularity_table.count()
  assert count > 40000, "Number of products is less than 40000." 

  count_non_zero_popularity = popularity_table.count({"popularity": {"$gt": 0}})
  assert count_non_zero_popularity > 40000, "Number of products is less than 40000." 

  print("Popularity data looks good. Validation successful")
  
if __name__ == '__main__':
  print(get_popularity_for_id('111405', parent_id='111417'))
  print(get_popularity_for_id('7410', parent_id='7412'))
  print(get_popularity_for_id('157885', parent_id='157885'))
  print(get_popularity_for_id('157885', parent_id='157885'))
  print(get_popularity_for_id('31074', parent_id='31074'))
  validate_popularity_data_health()
