import sys
import argparse
from contextlib import closing
from collections import defaultdict
from itertools import islice

sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
from elasticsearch import helpers, Elasticsearch

sys.path.append('/nykaa/scripts/sharedutils/')
from esutils import EsUtils


def chunks(data, SIZE=10000):
  it = iter(data)
  for i in range(0, len(data), SIZE):
    yield {k: data[k] for k in islice(it, SIZE)}

def update_info(docs, collection):
  try:
    es = DiscUtils.esConn()
    actions = []
    for sku, sizes in docs.items():
      actions.append({'_op_type': 'update',
                      '_index': collection,
                      '_id': sku,
                      'doc': {'instock_size_ids': sizes}
                    })
    kwargs = {'stats_only': False, 'raise_on_error': False}
    stats = helpers.bulk(es, actions, **kwargs)
    return stats
  except Exception as e:
    print(e)
    return

def updateInStockInformation(collection, batch_size=500):
  querydsl = {}
  querydsl["_source"] = ["sku", "psku", "size_ids"]
  must = []
  must.append({"exists": {"field": "size_ids"}})
  must.append({"term": {"type": "simple"}})
  must.append({"term": {"in_stock": "true"}})
  querydsl["query"] = {"bool": {"must": must}}
  data = EsUtils.scrollESForProducts(collection, querydsl)
  instock_size_info = defaultdict(list)
  for doc in data:
    product = doc["_source"]
    instock_size_info[product["sku"]].append(product["size_ids"])
    instock_size_info[product["psku"]].append(product["size_ids"])
  
  for docs in chunks(instock_size_info, batch_size):
    update_info(docs, collection)
  
  return {"result": "success"}

if __name__=="__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-b", "--batchsize", default=500, help='insert batch size', type=int)
  parser.add_argument("-c", "--collection", help='insert collection', type=str)
  argv = vars(parser.parse_args())
  batch_size = argv.get('batchsize')
  index = argv.get('collection')
  updateInStockInformation(index, batch_size)
