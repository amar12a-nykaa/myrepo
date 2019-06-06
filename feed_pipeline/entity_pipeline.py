#!/usr/bin/python
import os
import json
import socket
import sys
import time
import timeit
import requests
import argparse
import traceback
import subprocess
import urllib.request
import csv

sys.path.append('/nykaa/scripts/sharedutils/')
from esutils import EsUtils
from entity_indexer import EntityIndexer

from catalog_nykaa_importer import NykaaImporter

sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

FEED_URL = "http://www.nykaa.com/media/feed/master_feed_gludo.csv"
FEED_LOCATION = '/data/nykaa/master_feed_gludo.csv'
hostname = socket.gethostname()



def indexESData():
  indexes = EsUtils.get_active_inactive_indexes("entity")
  active_index = indexes['active_index']
  inactive_index = indexes['inactive_index']
  print("ES Active Index: %s"%active_index)
  print("ES Inactive Index: %s"%inactive_index)

  

  #clear inactive index
  index_client = EsUtils.get_index_client()
  if index_client.exists(inactive_index):
    print("Deleting index: %s" % inactive_index)
    index_client.delete(inactive_index)
  schema = json.load(open(  os.path.join(os.path.dirname(__file__), 'entity_schema.json')))
  index_client.create(inactive_index, schema)
  print("Creating index: %s" % inactive_index)

  index_start = timeit.default_timer()

  EntityIndexer.index(inactive=True, swap=True, index_all=True)

  index_stop = timeit.default_timer()
  index_duration = index_stop - index_start
  print("Time taken to index data to ElasticSearch: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))

  # Verify correctness of indexing by comparing total number of documents in both active and inactive collections
  body = {"query": {"match_all": {}}, "size" : 0}
  num_docs_active = DiscUtils.makeESRequest(body, active_index)['hits']['total']
  num_docs_inactive = DiscUtils.makeESRequest(body, inactive_index)['hits']['total']
  print('ES Number of documents in active index(%s): %s'%(active_index, num_docs_active))
  print('ES Number of documents in inactive index(%s): %s'%(inactive_index, num_docs_inactive))

  resp = EsUtils.switch_index_alias("entity", active_index, inactive_index)
  print("\n\nFinished running catalog pipeline for ElasticSearch. NEW ACTIVE INDEX: %s\n\n"%inactive_index)

if __name__ == "__main__":

  parser = argparse.ArgumentParser()
  argv = vars(parser.parse_args())
  
  indexESData()


  script_stop = timeit.default_timer()
  script_duration = script_stop - script_start
  print("Total time taken for the script to run: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
