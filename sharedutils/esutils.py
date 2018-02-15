import json
import socket
import sys
import traceback
import urllib.parse
import urllib.request
from datetime import datetime

import requests
from IPython import embed
from elasticsearch import helpers, Elasticsearch

sys.path.append('/home/apis/nykaa/')
from pas.v1.exceptions import SolrError
from pas.v1.utils import CATALOG_COLLECTION_ALIAS, MemcacheUtils, Utils


index_alias_config = {
  "livecore": # This must be changed to 'catalog' 
    { 
      "collections": ['yin', 'yang'],
      "config" : "livecore",
      "unique_field" : "sku",
      "type" : "product"
    },
  "autocomplete":
    { 
      "collections": ['autocomplete_yin', 'autocomplete_yang'],
      "config" : "autocomplete",
      "unique_field" : "entity",
      "type" : "entity"
    },
}

class EsUtils:
  def get_active_inactive_indexes(index_alias):
    return {'active_index' : 'yin', 'inactive_index' : 'yang'}

  def clearIndexData(index):
    return 'All Clear'

  def get_index_data(index):
    for key, value in index_alias_config.items():
      if key == index or index in value['collections']:
        return value

    return None

  def indexDocs(docs, index):
    indexData = EsUtils.get_index_data(index)
    if indexData == None:
      print("Unable to find the unique field in the index "+index)
      return

    uniqueField = indexData['unique_field']
    doctype = indexData['type']

    upload_docs = []
    for doc in docs:
      for key, value in doc.items():
        if isinstance(value, dict):
          doc[key] = json.dumps(value)
        elif isinstance(value, list):
          if value and isinstance(value[0], dict):
            flattened_value = [json.dumps(item) for item in value]
            doc[key] = flattened_value

      doc_to_insert = {
        '_id' : doc[uniqueField],
        '_type' : doctype,
        '_index' : index,
        '_source' : doc
      }
      upload_docs.append(doc_to_insert)

    response = {}
    try:
      es = Utils.esConn()
      helpers.bulk(es, upload_docs)
    except Exception as e:
      print(traceback.format_exc())
      raise 
    return response

if __name__ == "__main__":
  ret = SolrUtils.get_active_inactive_indexes('livecore')
  print(ret)
  embed()
