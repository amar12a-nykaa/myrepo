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
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils

from index import index_engine
from generate_brand_category_mapping import generate_brand_category_mapping
from normalize_searches_daily import normalize_search_terms

AUTOCOMPLETE = 'autocomplete'

parser = argparse.ArgumentParser()
parser.add_argument("--force-run", action='store_true')
argv = vars(parser.parse_args())

force_run = argv['force_run']
script_start = timeit.default_timer()

normalize_search_terms()
generate_brand_category_mapping()
insertFeedBackDataInMongo()

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


script_stop = timeit.default_timer()
script_duration = script_stop - script_start

print("\n\nFinished running catalog pipeline. NEW ACTIVE COLLECTION: %s\n\n"%inactive_index)
print("Time taken to index data to ES: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))
print("Total time taken for the script to run: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
