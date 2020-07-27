import argparse
import sys
import arrow
import datetime

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter
from mongoutils import MongoUtils
from esutils import EsUtils

sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

client = MongoUtils.getClient()

def ensure_mongo_index():
  collection = 'rating_data'
  existing_list = client['search'].collection_names()
  if collection not in existing_list:
    print("creating collection: ", collection)
    client['search'].create_collection(collection)
    indexname = "product_1"
    indexdef = [('product_id', 1)]
    print("Creating index '%s' in collection '%s' " % (indexname, collection))
    client['search'][collection].create_index(indexdef)
    indexdef = [('last_calculated', 1)]
    client['search'][collection].create_index(indexdef)


def fetch_products_from_es(size):
  query = {
    "size": size,
    "query": {
      "match_all": {}
    },
    "_source": ["product_id", "review_splitup"]
  }
  results = EsUtils.scrollESForProducts(index='livecore',query=query)
  return results


def get_avg_review(reviews):
  total_count = reviews.get("total_count", 0)
  if not total_count:
    return 0
  total_count += 5
  param1 = 0
  param2 = 0
  z = 1.65
  for i in range(1,5):
    vote_i = reviews.get(str(i), {}).get('count', 0)
    ratio = (vote_i+1)/total_count
    param1 += i*ratio
    param2 += i*param1
  rating_value = param1 - z * pow((param2-pow(param1,2))/(total_count+1), 0.5)
  return rating_value


def calculate_rating():
  results = fetch_products_from_es(size=10000)
  timestamp = arrow.now().datetime
  ensure_mongo_index()
  rating_data = client['search']['rating_data']
  ctr = LoopCounter("Writing Data: ", total=len(results))
  for docs in results:
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    if docs:
      try:
        product_id = docs['_source']['product_id']
        reviews = docs['_source'].get('review_splitup', {})
        rating_value = get_avg_review(reviews)
        filt = {"product_id": product_id}
        ret = rating_data.update_one(filt, {"$set": {'rating_value': rating_value, 'last_calculated': timestamp}}, upsert=True)
      except Exception as e:
        print(e)

    rating_data.remove({"last_calculated": {"$ne": timestamp}})

if __name__ == '__main__':
  calculate_rating()
