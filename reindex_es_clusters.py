#!/usr/bin/python
import sys
import argparse
import time
import timeit
from elasticsearch import helpers, Elasticsearch
sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
sys.path.append('/nykaa/scripts/sharedutils/')
from esutils import EsUtils

if __name__ == '__main__':
    start_time = timeit.default_timer()
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--from-region", default="singapore", help="name of region from which you want to reindex")
    parser.add_argument("-t", "--target-region", default="mumbai", help="name of region to which you want to reindex")
    parser.add_argument("-fi", "--from-index", default="livecore", help="name of index from which you want to reindex")
    parser.add_argument("-ti", "--to-index", default="livecore", help="name of index to which you want to reindex")
    argv = vars(parser.parse_args())
    esutil_obj = EsUtils()
    esutil_obj.reindex_two_clusters(argv['from_region'], argv['target_region'], argv['from_index'], argv['to_index'])
    end_time = timeit.default_timer()
    time_taken = end_time - start_time
    print("Total time taken for the script to run: %s" % time.strftime("%M min %S seconds", time.gmtime(time_taken)))
