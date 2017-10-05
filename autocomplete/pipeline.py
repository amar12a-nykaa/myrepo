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

sys.path.append('/nykaa/scripts/sharedutils/')
from solrutils import SolrUtils

SOLR_GROUP = 'autocomplete'

parser = argparse.ArgumentParser()
argv = vars(parser.parse_args())
script_start = timeit.default_timer()

  
collections = SolrUtils.get_active_inactive_collections(SOLR_GROUP)
active_collection = collections['active_collection']
inactive_collection = collections['inactive_collection']
print("Active collection: %s"%active_collection)
print("Inactive collection: %s"%inactive_collection)

#clear inactive collection
resp = SolrUtils.clearSolrCollection(inactive_collection)

index_start = timeit.default_timer()

CatalogIndexer.index(file_path, inactive_collection)

#print("Committing all remaining docs")
#base_url = Utils.solrBaseURL(collection=inactive_collection)
#requests.get(base_url + "update?commit=true")

index_stop = timeit.default_timer()
index_duration = index_stop - index_start

# Verify correctness of indexing by comparing total number of documents in both active and inactive collections
params = {'q': '*:*', 'rows': '0'}
num_docs_active = Utils.makeSolrRequest(params, collection=active_collection)['numFound']
num_docs_inactive = Utils.makeSolrRequest(params, collection=inactive_collection)['numFound']
print('Number of documents in active collection(%s): %s'%(active_collection, num_docs_active))
print('Number of documents in inactive collection(%s): %s'%(inactive_collection, num_docs_inactive))

# if it decreased more than 5% of current, abort and throw an error
docs_ratio = num_docs_inactive/num_docs_active
if docs_ratio < 0.95 and not force_run:
  msg = "[ERROR] Number of documents decreased by more than 5% of current documents. Please verify the data or run with --force option to force run the indexing."
  print(msg)
  raise Exception(msg)


# Update alias SOLR_GROUP to point to freshly indexed collection(inactive_collection)
# and do basic verification
resp = SolrUtils.createSolrCollectionAlias(inactive_collection, SOLR_GROUP)

script_stop = timeit.default_timer()
script_duration = script_stop - script_start

print("\n\nFinished running catalog pipeline. NEW ACTIVE COLLECTION: %s\n\n"%inactive_collection)
print("Time taken to import data from Nykaa: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(import_duration)))
print("Time taken to index data to Solr: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))
print("Total time taken for the script to run: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
