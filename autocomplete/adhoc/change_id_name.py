import pprint
import time
from pymongo import MongoClient 
import sys

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter
def create_product_id_index():
  start = time.time()
  product_id_index = {}
  client = Utils.mongoClient()
  master_feed = client['feed_pipeline']['master_feed']
  for p in master_feed.find({}, {"parent_id":1, 'product_id':1, "sku":1, "psku":1, "_id": 0}).limit(0) :
    product_id_index[p['product_id']] = p
  delta = time.time() - start
  print("Time taken to create product_id index: %s seconds" % round(delta))
  if len(product_id_index.keys()) < 50000:
    print("Failed to create product_id_index. Master feed might be missing.")
    exit()
  return product_id_index

product_id_index = create_product_id_index()

raw_data = Utils.mongoClient()['search']['raw_data']
#raw_data = Utils.mongoClient()['search']['processed_data']

#clause = {"product_id": None}
ctr = LoopCounter("Reading CSV")#, total=raw_data.count(clause))
for p in raw_data.find({"parent_id":  None}).limit(0):
  ctr += 1
  if ctr.should_print():
    print(ctr.summary)

  _id = p['_id']
  product_id = p['product_id']
  #p['product_id'] = p.pop("productid")
  update_doc = product_id_index.get(product_id, {})
  #update_doc.update({'product_id': product_id})
  #print(update_doc)

  if not update_doc:
    print("Empty update_doc for product_id: '%s'" % product_id)
    continue
  #print(raw_data.find_one({"_id": _id}))
  raw_data.update({"_id": _id}, {"$set": update_doc, "$unset": {"productid": ""}})
  #pprint.pprint(raw_data.find_one({"_id": _id}))
  #break

