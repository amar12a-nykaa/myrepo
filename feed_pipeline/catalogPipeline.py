#!/usr/bin/python
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
from solrutils import SolrUtils
from esutils import EsUtils

from importDataFromNykaa import NykaaImporter
from indexCatalog import CatalogIndexer
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils, CATALOG_COLLECTION_ALIAS

FEED_URL = "http://www.nykaa.com/media/feed/master_feed_gludo.csv"
FEED_LOCATION = '/data/nykaa/master_feed_gludo.csv'
hostname = socket.gethostname()


def indexSolrData(file_path, force_run):
  collections = SolrUtils.get_active_inactive_collections(CATALOG_COLLECTION_ALIAS)
  active_collection = collections['active_collection']
  inactive_collection = collections['inactive_collection']
  print("Active collection: %s"%active_collection)
  print("Inactive collection: %s"%inactive_collection)

  #clear inactive collection
  resp = SolrUtils.clearSolrCollection(inactive_collection)

  index_start = timeit.default_timer()

  print("\n\nIndexing documents from csv file '%s' to collection '%s'."%(file_path, inactive_collection))
  CatalogIndexer.index(search_engine='solr', file_path=file_path, collection=inactive_collection)

  #print("Committing all remaining docs")
  #base_url = Utils.solrBaseURL(collection=inactive_collection)
  #requests.get(base_url + "update?commit=true")

  index_stop = timeit.default_timer()
  index_duration = index_stop - index_start

  # Verify correctness of indexing by comparing total number of documents in both active and inactive collections
  params = {'q': '*:*', 'rows': '0'}
  num_docs_active = Utils.makeSolrRequest(params, collection=active_collection)['numFound']
  num_docs_inactive = Utils.makeSolrRequest(params, collection=inactive_collection)['numFound']
  print('Solr: Number of documents in active collection(%s): %s'%(active_collection, num_docs_active))
  print('Solr: Number of documents in inactive collection(%s): %s'%(inactive_collection, num_docs_inactive))

  # if it decreased more than 5% of current, abort and throw an error
  docs_ratio = num_docs_inactive/num_docs_active
  if docs_ratio < 0.95 and not force_run:
    msg = "[ERROR] Number of documents decreased by more than 5% of current documents. Please verify the data or run with --force option to force run the indexing."
    print(msg)
    raise Exception(msg)

  # Update alias CATALOG_COLLECTION_ALIAS to point to freshly indexed collection(inactive_collection)
  # and do basic verification
  resp = SolrUtils.createSolrCollectionAlias(inactive_collection, CATALOG_COLLECTION_ALIAS)

  print("\n\nFinished running catalog pipeline for Solr. NEW ACTIVE COLLECTION: %s\n\n"%inactive_collection)
  print("Time taken to index data to Solr: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))

def indexESData(file_path, force_run):
  indexes = EsUtils.get_active_inactive_indexes(CATALOG_COLLECTION_ALIAS)
  active_index = indexes['active_index']
  inactive_index = indexes['inactive_index']
  print("Active Index: %s"%active_index)
  print("Inactive Index: %s"%inactive_index)

  #clear inactive index
  resp = EsUtils.clear_index_data(inactive_index)

  index_start = timeit.default_timer()

  print("\n\nIndexing documents from csv file '%s' to index '%s'."%(file_path, inactive_index))
  CatalogIndexer.index(search_engine='elasticsearch', file_path=file_path, collection=inactive_index)

  index_stop = timeit.default_timer()
  index_duration = index_stop - index_start
  print("Time taken to index data to ElasticSearch: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))

  # Verify correctness of indexing by comparing total number of documents in both active and inactive collections
  body = {"query": {"match_all": {}}, "size" : 0}
  num_docs_active = Utils.makeESRequest(body, active_index)['hits']['total']
  num_docs_inactive = Utils.makeESRequest(body, inactive_index)['hits']['total']
  print('ES Number of documents in active index(%s): %s'%(active_index, num_docs_active))
  print('ES Number of documents in inactive index(%s): %s'%(inactive_index, num_docs_inactive))

  # Update alias CATALOG_COLLECTION_ALIAS to point to freshly generated index
  # and do basic verification
  resp = EsUtils.switch_index_alias(CATALOG_COLLECTION_ALIAS, active_index, inactive_index)
  print("\n\nFinished running catalog pipeline for ElasticSearch. NEW ACTIVE INDEX: %s\n\n"%inactive_index)

if __name__ == "__main__":

  parser = argparse.ArgumentParser()
  parser.add_argument("-p", "--filepath", help='path to csv file')
  parser.add_argument("-u", "--url", help='url to csv file')
  parser.add_argument("-i", "--importattrs", action='store_true', help='Flag to import attributes first')
  parser.add_argument("-f", "--force", action='store_true', help='Force run the indexing, without any restrictions')
  parser.add_argument("-g", "--generate-third-party-feeds", action='store_true')
  parser.add_argument("-s", "--search-engine", default=None, help='"solr" or "elasticsearch". If nothing is passed, both are indexed')
  argv = vars(parser.parse_args())
  
  assert argv['search_engine'] in ['solr', 'elasticsearch', None]

  file_path = argv['filepath']
  url = argv['url']
  if not url and not file_path: 
    if hostname.startswith('admin'):
      url = "http://www.nykaa.com/media/feed/master_feed_gludo.csv"
    elif hostname.startswith('preprod') or hostname.startswith('qa') or hostname.startswith('dev'):
      url = "http://preprod.nykaa.com/media/feed/master_feed_gludo.csv"
    if url:
      print("Using default url for %s machine: %s" % (hostname, url))
  import_attrs = argv.get('importattrs', False)
  force_run = argv.get('force', False)

  if not (file_path or url):
    msg = "Either of filepath[-p] or url[-u] of the feed needs to be provided."
    print(msg)
    raise Exception(msg)
  elif (file_path and url):
    msg = "Please provide only one of filepath[-p] or url[-u] for the feed"
    print(msg)
    raise Exception(msg)

  script_start = timeit.default_timer()

# If url given, download the feed first
  if url:
    try:
      print("Downloading feed from: %s. Please wait..."%url)

      urllib.request.urlretrieve(url, FEED_LOCATION)
      file_path = FEED_LOCATION

      print("Feed download finished!")
    except Exception as e:
      print(traceback.format_exc())
      raise

  import_start = timeit.default_timer()

  # Import attributes from Nykaa DBs
  if import_attrs:
    print("Importing attributes from Nykaa DB....")
    NykaaImporter.importData()

  import_stop = timeit.default_timer()
  import_duration = import_stop - import_start
  print("Time taken to import data from Nykaa: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(import_duration)))

  # Index Elastic Search Data
  if argv['search_engine'] in ['elasticsearch' or None]:
    indexESData(file_path, force_run)

  # Index Solr Data
  if argv['search_engine'] in ['solr' or None]:
    indexSolrData(file_path, force_run)

  script_stop = timeit.default_timer()
  script_duration = script_stop - script_start
  print("Total time taken for the script to run: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
