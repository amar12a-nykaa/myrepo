import json
import socket
import sys
import traceback
import urllib.parse
import urllib.request
from datetime import datetime

import elasticsearch
import requests
from IPython import embed
from elasticsearch import helpers, Elasticsearch

sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import CATALOG_COLLECTION_ALIAS, MemcacheUtils, Utils


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
      "unique_field" : "_id",
      "type" : "entity",
      "copy_fields" : {
        "entity" : "entity_ngram"
      }
    },
  "entity":
    {
      "collections": ['entity_yin', 'entity_yang'],
      "config" : "entity",
      "unique_field" : "_id",
      "type" : "entity"
    },
  "guide":
    {
      "collections": ['guide_yin', 'guide_yang'],
      "config": "guide",
      "unique_field": "_id",
      "type": "entity"
    }
}

class EsUtils:

  def get_connection():
    return Utils.esConn()

  def get_index_client():
    return elasticsearch.client.IndicesClient(Utils.esConn())

  def get_index_from_alias(alias):
    response = {}
    es = Utils.esConn()
    if es.indices.exists_alias(alias):
      response = es.indices.get_alias(
        index=alias
      )
    else:
      if index_alias_config.get(alias):
        settings = index_alias_config.get(alias)
        if settings and settings.get('collections'):
          index_client =  EsUtils.get_index_client()
          for index in settings['collections']:
            if not index_client.exists(index):
              if index in ['yin', 'yang']:
                schema = json.load(open('/home/ubuntu/nykaa_scripts/feed_pipeline/schema.json'))
                es.indices.create(index = index, body = schema)
                es.indices.put_alias(index= index, name = alias)
              if index in ['autocomplete_yin', 'autocomplete_yang']:
                schema = json.load(open('/home/ubuntu/nykaa_scripts/autocomplete/schema.json'))
                es.indices.create(index = index, body = schema)
                es.indices.put_alias(index= index, name = alias)
              if index in ['entity_yin', 'entity_yang']:
                schema = json.load(open('/home/ubuntu/nykaa_scripts/feed_pipeline/entity_schema.json'))
                es.indices.create(index = index, body = schema)
                es.indices.put_alias(index= index, name = alias)
              if index in ['guide_yin', 'guide_yang']:
                schema = json.load(open('/home/ubuntu/nykaa_scripts/feed_pipeline/guide_schema.json'))
                es.indices.create(index = index, body = schema)
                es.indices.put_alias(index= index, name = alias)
            else:
              es.indices.put_alias(index= index, name = alias)
    response = es.indices.get_alias(index=alias)
    for index, index_aliases in response.items():
      return index

    raise Exception("Couldnt find index for alias: %s" % alias )

  def get_active_inactive_indexes(index_alias):
    for alias, settings in index_alias_config.items():
      if alias == index_alias:
        active_index = EsUtils.get_index_from_alias(alias)
        for index in settings['collections']:
          if index != active_index:
            return {'active_index'  : active_index, 'inactive_index' : index}

    raise Exception("Cannot find alias: %s" % index_alias)

  def switch_index_alias(alias, from_index, to_index):
    response = {}
    try:
      es = Utils.esConn()
      response = es.indices.update_aliases(
        body={
          "actions" : [
            { "add":  { "index": to_index, "alias": alias } },
            { "remove": { "index": from_index, "alias": alias } }  
          ]
        }
      )

      for index, index_aliases in response.items():
        return index
    except Exception as e:
      print(traceback.format_exc())
      raise 
    return response

  def clear_index_data(index):
    response = {}
    try:
      es = Utils.esConn()
      response = es.delete_by_query(
        index=index,
        body={
          "query": {
            "match_all": {}
          }
        }
      )
    except Exception as e:
      print(traceback.format_exc())
      raise 
    return response

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

      if 'copy_fields' in indexData:
        for field_src, field_name in indexData['copy_fields'].items():
          doc[field_name] = doc[field_src]

      uniqueValue = doc[uniqueField]
      if '_id' in doc:
        doc.pop('_id', None)

      doc_to_insert = {
        '_id' : uniqueValue,
        '_type' : doctype,
        '_index' : index,
        '_source' : doc
      }
      upload_docs.append(doc_to_insert)

    response = {}
    try:
      es = Utils.esConn()
      helpers.bulk(es, upload_docs, request_timeout=120)
    except Exception as e:
      print(traceback.format_exc())
      raise 
    return response

  def swap_index(alias):
    print("Swapping Index")
    indexes = EsUtils.get_active_inactive_indexes(alias)
    EsUtils.switch_index_alias(alias, from_index=indexes['active_index'], to_index=indexes['inactive_index'])
  

if __name__ == "__main__":
  ret = EsUtils.get_active_inactive_indexes('livecore')
  print(ret)
  embed()
