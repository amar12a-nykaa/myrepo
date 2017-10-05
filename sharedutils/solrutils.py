import json
import socket
import sys
import traceback
import urllib.parse
import urllib.request
from datetime import datetime

import requests
from IPython import embed

sys.path.append('/home/apis/nykaa/')
from pas.v1.exceptions import SolrError
from pas.v1.utils import CATALOG_COLLECTION_ALIAS, MemcacheUtils, Utils


SOLR_GROUPS = {
  "livecore": # This must be changed to 'catalog' 
    { 
      "collections": ['yin', 'yang'],
      "config" : "livecore",
    },
  "autocomplete": 
    { 
      "collections": ['autocomplete_yin', 'autocomplete_yang'],
      "config" : "autocomplete",
    },
}


class SolrUtils:

  def solrClusterStatus():
    url = Utils.solrHostName() + "/solr/admin/collections?action=CLUSTERSTATUS&wt=json"
    try:
      response = requests.get(url)
      response = json.loads(response.content.decode('utf-8'))
    except Exception as e:
      print(traceback.format_exc())
      raise
    return response 

  def clearSolrCollection(coll_name):
    if coll_name==CATALOG_COLLECTION_ALIAS:
      raise Exception("[ERROR] You are about to clear currently active collection. Please double-check and clear it manually if needed.")

    url = Utils.solrBaseURL(collection=coll_name) + 'update?commit=true&stream.body=<delete><query>*:*</query></delete>&wt=json'
    try:
      response = requests.get(url)
      response = json.loads(response.content.decode('utf-8'))
      response_header = response.get('responseHeader', {})
      if response_header.get('status')!=0:
        raise Exception("[ERROR] There was an error clearing the collection '%s': %s"%(coll_name, response['error']['msg']))
    except Exception as e:
      print(traceback.format_exc())
      raise
    return response

  def createSolrCollectionAlias(collection, alias):
    url = Utils.solrHostName() + "/solr/admin/collections?action=CREATEALIAS&wt=json&name=%s&collections=%s"%(alias, collection)
    print(url)
    try:
      response = requests.get(url)
      response = json.loads(response.content.decode('utf-8'))
      response_header = response.get('responseHeader', {})
      if response_header.get('status')!=0:
        raise Exception("[ERROR] There was an error creating alias '%s' for collection %s: %s"%(alias, collection, response['error']['msg']))
    except Exception as e:
      print(traceback.format_exc())
      raise
    return response

  def indexDocs(docs, collection):
    url = Utils.solrBaseURL(collection) + "update/json/docs?commit=true"
    upload_docs = []
    for doc in docs:
      for key, value in doc.items():
        if isinstance(value, dict):
          doc[key] = json.dumps(value)
        elif isinstance(value, list):
          if value and isinstance(value[0], dict):
            flattened_value = [json.dumps(item) for item in value]
            doc[key] = flattened_value
      upload_docs.append(doc)

    response = requests.post(url, json=upload_docs)
    response_content = json.loads(response.content.decode("utf-8"))
    if response.status_code != 200:
      raise SolrError(response_content['error']['msg'])
    return response_content

  def get_active_inactive_collections(group):
    # fetch the inactive collection
    active_collection = ''
    inactive_collection = ''
    config = SOLR_GROUPS[group]['config']
    # fetch solr cluster status to find out which collection default alias points to
    cluster_status = SolrUtils.solrClusterStatus()
    cluster_status = cluster_status.get('cluster')
    if cluster_status:
      aliases = cluster_status.get('aliases')
      if aliases and aliases.get(group):
        active_collection = aliases.get(group)
        inactive_collection = [x for x in SOLR_GROUPS[group]['collections'] if x != active_collection][0] 
      else:
        # first time
        active_collection = SOLR_GROUPS[group]['collections'][0]
        inactive_collection = SOLR_GROUPS[group]['collections'][1]

    else:
      msg = "[ERROR] Failed to fetch solr cluster status. Aborting.."
      print(msg)
      raise Exception(msg)

    return {"active_collection": active_collection, "inactive_collection": inactive_collection}

  def swap_core(group):
    collections = SolrUtils.get_active_inactive_collections(group)
    inactive_collection = collections['inactive_collection']
    print("Currently active collection: %s " % SolrUtils.get_active_inactive_collections(group)['active_collection'])
    resp = SolrUtils.createSolrCollectionAlias(inactive_collection, group)
    print("New active collection: %s " % SolrUtils.get_active_inactive_collections(group)['active_collection'])


if __name__ == "__main__":
  ret = SolrUtils.get_active_inactive_collections('livecore')
  print(ret)
  embed()
