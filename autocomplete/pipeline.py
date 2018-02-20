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

sys.path.append('/nykaa/api/')
from pas.v1.utils import Utils

from index import index_all
from generate_brand_category_mapping import generate_brand_category_mapping
from normalize_searches import normalize_search_terms

SOLR_GROUP = 'autocomplete'

parser = argparse.ArgumentParser()
parser.add_argument("--force-run", action='store_true')
argv = vars(parser.parse_args())

force_run = argv['force_run']
script_start = timeit.default_timer()

#normalize_search_terms()
generate_brand_category_mapping()

collections = SolrUtils.get_active_inactive_collections(SOLR_GROUP)
active_collection = collections['active_collection']
inactive_collection = collections['inactive_collection']
print("Active collection: %s"%active_collection)
print("Inactive collection: %s"%inactive_collection)

#clear inactive collection
resp = SolrUtils.clearSolrCollection(inactive_collection)

index_start = timeit.default_timer()
index_all(inactive_collection)
index_duration = timeit.default_timer() - index_start

# Verify correctness of indexing by comparing total number of documents in both active and inactive collections
params = {'q': '*:*', 'rows': '0'}
num_docs_active = Utils.makeSolrRequest(params, collection=active_collection)['numFound']
num_docs_inactive = Utils.makeSolrRequest(params, collection=inactive_collection)['numFound']
print('Number of documents in active collection(%s): %s'%(active_collection, num_docs_active))
print('Number of documents in inactive collection(%s): %s'%(inactive_collection, num_docs_inactive))

# if it decreased more than 5% of current, abort and throw an error
if not num_docs_active:
  if num_docs_inactive:
    docs_ratio = 1
  else:
    docs_ratio = 0
else:
  docs_ratio = num_docs_inactive/num_docs_active
if docs_ratio < 0.95 and not force_run:
  msg = "[ERROR] Number of documents decreased by more than 5% of current documents. Please verify the data or run with --force option to force run the indexing."
  print(msg)
  raise Exception(msg)


# Update alias SOLR_GROUP to point to freshly indexed collection(inactive_collection)
# and do basic verification
#resp = SolrUtils.createSolrCollectionAlias(inactive_collection, SOLR_GROUP)

script_stop = timeit.default_timer()
script_duration = script_stop - script_start

print("\n\nFinished running catalog pipeline. NEW ACTIVE COLLECTION: %s\n\n"%inactive_collection)
print("Time taken to index data to Solr: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))
print("Total time taken for the script to run: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
