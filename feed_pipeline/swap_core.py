#!/usr/bin/python
import sys
import time
import timeit
import requests
import argparse
import traceback
import subprocess
import urllib.request
from pipelineUtils import SolrUtils
from importDataFromNykaa import NykaaImporter
from indexCatalog import CatalogIndexer
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils, CATALOG_COLLECTION_ALIAS

YIN_COLL ='yin'
YANG_COLL = 'yang'


# fetch the inactive collection
active_collection = ''
inactive_collection = ''

# fetch solr cluster status to find out which collection default alias points to
cluster_status = SolrUtils.solrClusterStatus()
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
  msg = "[ERROR] Failed to fetch solr cluster status. Aborting.."
  print(msg)
  raise Exception(msg)

print("Active:", active_collection)
print("Inactive:", inactive_collection)
# Update alias CATALOG_COLLECTION_ALIAS to point to freshly indexed collection(inactive_collection)
# and do basic verification
resp = SolrUtils.createSolrCollectionAlias(inactive_collection, CATALOG_COLLECTION_ALIAS)

