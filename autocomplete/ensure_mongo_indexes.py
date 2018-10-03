from IPython import embed
import sys
import pymongo 
sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

client = Utils.mongoClient()


def ensure_mongo_collection(collection_list, db):
  existing_list = client['search'].collection_names()
  for collection in collection_list:
    if collection not in existing_list:
      print("creating collection: ", collection)
      col = client[db].create_collection(collection)

def ensure_mongo_indices_now():
  collection_list = ["search_terms_daily", "search_terms_normalized_daily", "processed_data", "raw_data", "corrected_search_query"]
  ensure_mongo_collection(collection_list, "search")
  indexes = [
          {
                  "v" : 2,
                  "key" : {
                          "_id" : 1
                  },
                  "name" : "_id_",
                  "ns" : "search.search_terms_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "date" : 1,
                          "platform" : 1,
                          "term" : 1
                  },
                  "name" : "date_1_platform_1_term_1",
                  "ns" : "search.search_terms_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "query" : 1
                  },
                  "name" : "query_1",
                  "ns" : "search.search_terms_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "date" : 1,
                          "internal_search_term_conversion_instance" : -1
                  },
                  "name" : "date_1_internal_search_term_conversion_instance_-1",
                  "ns" : "search.search_terms_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "internal_search_term_conversion_instance" : -1
                  },
                  "name" : "internal_search_term_conversion_instance_-1",
                  "ns" : "search.search_terms_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "_id" : 1
                  },
                  "name" : "_id_",
                  "ns" : "search.search_terms_normalized_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "query" : 1
                  },
                  "name" : "query_1",
                  "ns" : "search.search_terms_normalized_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "date" : 1
                  },
                  "name" : "date_1",
                  "ns" : "search.search_terms_normalized_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "popularity" : -1
                  },
                  "name" : "popularity_-1",
                  "ns" : "search.search_terms_normalized_daily"
          },
          {
                  "v" : 2,
                  "key" : {
                          "parent_id" : 1,
                          "date" : 1
                  },
                  "name" : "parent_id_1_date_1",
                  "ns" : "search.processed_data"
          },
          {
                  "v" : 2,
                  "key" : {
                          "date" : 1
                  },
                  "name" : "date_1",
                  "ns" : "search.processed_data"
          },
          {
                  "v" : 2,
                  "key" : {
                          "parent_id" : 1
                  },
                  "name" : "parent_id_1",
                  "ns" : "search.raw_data"
          },
          {
                  "v" : 2,
                  "key" : {
                          "product_id" : 1
                  },
                  "name" : "product_id_1",
                  "ns" : "search.raw_data"
          },
          {
                  "v" : 2,
                  "key" : {
                          "date" : 1
                  },
                  "name" : "date_1",
                  "ns" : "search.raw_data"
          },
          {
                  "v" : 2,
                  "key" : {
                          "date" : 1,
                          "platform" : 1,
                          "product_id" : 1
                  },
                  "name" : "date_1_platform_1_product_id_1",
                  "ns" : "search.raw_data"
          },
          {
                  "v": 2,
                  "key": {
                          "_id": 1
                  },
                  "name": "_id_",
                  "ns": "search.corrected_search_query"
          },
          {
                  "v": 2,
                  "key": {
                          "query": 1,
                          "algo": 1
                  },
                  "name": "query_1_algo_1",
                  "ns": "search.corrected_search_query"
          }]


  print("Ensuring Indexes ... ")
  for index in indexes:
    collname = index['ns'].split(".")[1]
    indexname = index['name']
    indexdef = [(k,v) for k,v in index['key'].items()]
    coll = client['search'][collname]
    indices = coll.list_indexes()
    if indexname not in [x['name'] for x in indices]:
      print("Creating index '%s' in collection '%s' " % (indexname, collname))
      client['search'][collname].create_index(indexdef)
if __name__ == '__main__':
  ensure_mongo_indices_now()
