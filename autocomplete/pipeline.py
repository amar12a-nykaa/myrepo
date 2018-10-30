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
from esutils import EsUtils

sys.path.append('/nykaa/scripts/autocomplete/feedback')
from insertDataToMongo import insertFeedBackDataInMongo

sys.path.append('/nykaa/api/')
from pas.v2.utils import Utils

from index import index_engine
from generate_brand_category_mapping import generate_brand_category_mapping
from normalize_searches_daily import normalize_search_terms

AUTOCOMPLETE = 'autocomplete'

parser = argparse.ArgumentParser()
parser.add_argument("--force-run", action='store_true')
parser.add_argument('--bucket', '-b', type=str, default='nykaa-nonprod-feedback-autocomplete')
argv = vars(parser.parse_args())

force_run = argv['force_run']
script_start = timeit.default_timer()

normalize_search_terms()
generate_brand_category_mapping()
insertFeedBackDataInMongo(argv['bucket'])

indexes = EsUtils.get_active_inactive_indexes(AUTOCOMPLETE)
print(indexes)
active_index = indexes['active_index']
inactive_index = indexes['inactive_index']
print("Old Active index: %s"%active_index)
print("Old Inactive index: %s"%inactive_index)

#clear inactive index
resp = EsUtils.clear_index_data(inactive_index)

index_start = timeit.default_timer()
index_engine(engine='elasticsearch', collection=inactive_index, swap=True, index_all=True)
index_duration = timeit.default_timer() - index_start

## Verify correctness of indexing by comparing total number of documents in both active and inactive indexes
#params = {'q': '*:*', 'rows': '0'}
#num_docs_active = Utils.makeESRequest(params, index=active_index)['numFound']
#num_docs_inactive = Utils.makeESRequest(params, index=inactive_index)['numFound']
#print('Number of documents in active index(%s): %s'%(active_index, num_docs_active))
#print('Number of documents in inactive index(%s): %s'%(inactive_index, num_docs_inactive))
#
## if it decreased more than 5% of current, abort and throw an error
#if not num_docs_active:
#  if num_docs_inactive:
#    docs_ratio = 1
#  else:
#    docs_ratio = 0
#else:
#  docs_ratio = num_docs_inactive/num_docs_active
#if docs_ratio < 0.95 and not force_run:
#  msg = "[ERROR] Number of documents decreased by more than 5% of current documents. Please verify the data or run with --force option to force run the indexing."
#  print(msg)
#  raise Exception(msg)
#
#resp = EsUtils.switch_index_alias(AUTOCOMPLETE, active_index, inactive_index)

script_stop = timeit.default_timer()
script_duration = script_stop - script_start

print("\n\nFinished running catalog pipeline. NEW ACTIVE COLLECTION: %s\n\n"%inactive_index)
print("Time taken to index data to ES: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))
print("Total time taken for the script to run: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
