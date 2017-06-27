#!/usr/bin/python
import sys
import argparse
import traceback
import subprocess
import urllib.request
from importDataFromNykaa import NykaaImporter
from indexCatalog import CatalogIndexer
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils, CATALOG_COLLECTION_ALIAS

YIN_COLL ='yin'
YANG_COLL = 'yang'
FEED_URL = "http://www.nykaa.com/media/feed/master_feed_gludo.csv"
FEED_LOCATION = '/data/nykaa/master_feed_gludo.csv'

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filepath", help='path to csv file')
parser.add_argument("-u", "--url", help='url to csv file')
parser.add_argument("-i", "--importattrs", action='store_true', help='Flag to import attributes first')
argv = vars(parser.parse_args())

file_path = argv['filepath']
url = argv['url']
if not (file_path or url):
  raise Exception("Either of filepath[-f] or url[-u] of the feed needs to be provided.")
elif (file_path and url):
  raise Exception("Please provide only one of filepath[-f] or url[-u] for the feed")

# If url given, download the feed first
if url:
  try:
    print("Downloading feed from: %s. Please wait..."%url)

    urllib.request.urlretrieve(url, FEED_LOCATION)
    file_path = FEED_LOCATION

    print("Feed downlaod finished!")
  except Exception as e:
    print(traceback.format_exc())
    raise

# Import attributes from Nykaa DBs
import_attrs = argv.get('importattrs', False)
if import_attrs:
  print("Importing attributes from Nykaa DB....")
  NykaaImporter.importAttrs()
  
# fetch the inactive collection
active_collection = ''
inactive_collection = ''

# fetch solr cluster status to find out which collection default alias points to
cluster_status = Utils.solrClusterStatus()
cluster_status = cluster_status.get('cluster')
if cluster_status:
  aliases = cluster_status.get('aliases')
  if aliases and aliases.get(CATALOG_COLLECTION_ALIAS):
    active_collection = aliases.get(CATALOG_COLLECTION_ALIAS)
    inactive_collection = YIN_COLL if active_collection==YANG_COLL else YANG_COLL 
  else:
    # first time
    active_collection = YIN_COLL
    inactive_collection = YANG_COLL
    
else:
  raise Exception("[ERROR] Failed to fetch solr cluster status. Aborting..")

print("Active collection: %s"%active_collection)
print("Inactive collection: %s"%inactive_collection)

#clear inactive collection
resp = Utils.clearSolrCollection(inactive_collection)

print("\n\nIndexing documents from csv file '%s' to collection '%s'."%(file_path, inactive_collection))
CatalogIndexer.index(file_path, inactive_collection)

# Update alias CATALOG_COLLECTION_ALIAS to point to freshly indexed collection(inactive_collection)
# and do basic verification
resp = Utils.createSolrCollectionAlias(inactive_collection, CATALOG_COLLECTION_ALIAS)


print("\n\nFinished running catalog pipeline")
