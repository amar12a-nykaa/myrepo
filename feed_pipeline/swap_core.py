#!/usr/bin/python
import sys
from pipelineUtils import SolrUtils

sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils, CATALOG_COLLECTION_ALIAS

collections = SolrUtils.get_active_inactive_collections()
inactive_collection = collections['inactive_collection']

resp = SolrUtils.createSolrCollectionAlias(inactive_collection, CATALOG_COLLECTION_ALIAS)
