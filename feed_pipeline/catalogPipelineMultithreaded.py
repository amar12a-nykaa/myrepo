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

from importDataFromNykaa import NykaaImporter
from indexCatalog import CatalogIndexer
from update_bestseller_product_to_es import update_bestseller_data

sys.path.append("/home/apis/discovery_api/")
from disc.v2.utils import Utils as DiscUtils
#print(django.conf.ENVIRONMENT_VARIABLE)
#print(os.environ['DJANGO_SETTINGS_MODULE_DISCOVERY'])
#print(DiscUtils.test)
#sys.path.append('/home/apis/pds_api/')
#from pas.v2.utils import Utils as PasUtils
#print(django.conf.ENVIRONMENT_VARIABLE)
#print(os.environ['DJANGO_SETTINGS_MODULE_DISCOVERY'])


#print(PasUtils.test)
#print(DiscUtils.test)

# FEED_URL = "http://www.nykaa.com/media/feed/master_feed_gludo.csv"
# FEED_URL_PREPROD = "http://preprod.nykaa.com/media/feed/master_feed_gludo.csv"
FEED_URL_PREPROD = "http://preprod-2012758952.ap-southeast-1.elb.amazonaws.com/media/feed/master_feed_gludo.csv"
FEED_URL = "http://adminpanel.nykaa.com/media/feed/master_feed_gludo.csv"
FEED_LOCATION = '/data/nykaa/master_feed_gludo.csv'
hostname = socket.gethostname()

myname = os.path.basename(__file__)


def getCount():
    print("== List of processes running at the moment ==")
    print(subprocess.check_output("ps xao ppid,pid,pgid,sid,comm -o args |  grep python | grep %s| grep -vE 'vim|grep'  " % myname, shell=True).strip())
    print("== List of parent IDs running at the moment == ")
    print(subprocess.check_output("ps xao ppid,pid,pgid,sid,comm -o args |  grep python | grep %s| grep -vE 'vim|grep' |  awk '{ print $4 }' | sort -n  " % myname, shell=True).strip())
    num =  int(subprocess.check_output("ps xao ppid,pid,pgid,sid,comm -o args |  grep python | grep %s| grep -vE 'vim|grep' |  awk '{ print $4 }' | sort -n  | uniq | wc -l " % myname, shell=True).strip())
    print("== Number of processes running ==")
    print(num)
    print("====")
    return num

if getCount() > 1:
    print()
    print()
    print("=" * 20)
    print("This script is already running. Exiting without doing anything")
    # print(str(subprocess.check_output("ps xao ppid,pid,pgid,sid,comm -o args |  grep python | grep %s| grep -vE 'vim|grep'" % myname, shell=True)))
    print("This means that your intented changes might still be in progress!!!")
    raise Exception("Pipeline is already running. Exiting without doing anything")


def indexESData(file_path, force_run):
    indexes = EsUtils.get_active_inactive_indexes("livecore")
    active_index = indexes['active_index']
    inactive_index = indexes['inactive_index']
    print("ES Active Index: %s" % active_index)
    print("ES Inactive Index: %s" % inactive_index)

    # clear inactive index
    index_client = EsUtils.get_index_client()
    if index_client.exists(inactive_index):
        print("Deleting index: %s" % inactive_index)
        index_client.delete(inactive_index)
    schema = json.load(open(os.path.join(os.path.dirname(__file__), 'schema.json')))
    index_client.create(inactive_index, schema)
    sett = {'refresh_interval': '-1'}
    index_client.put_settings(sett, inactive_index)
    print("Creating index: %s" % inactive_index)

    index_start = timeit.default_timer()

    print("\n\nES: Indexing documents from csv file '%s' to index '%s'." % (file_path, inactive_index))
    CatalogIndexer.index(search_engine='elasticsearch', file_path=file_path, collection=inactive_index,
                         limit=argv['limit'], update_productids=True)

    sett = {'refresh_interval': '1s'}
    index_client.put_settings(sett, inactive_index)
    try:
        index_client.refresh(index=inactive_index, request_timeout=120)
    except Exception as e:
        print(traceback.format_exc())
        raise Exception("Index refresh failed.....")
    index_stop = timeit.default_timer()
    index_duration = index_stop - index_start
    print("Time taken to index data to ElasticSearch: %s seconds" % time.strftime("%M min %S seconds",
                                                                                  time.gmtime(index_duration)))

    # Verify correctness of indexing by comparing total number of documents in both active and inactive collections
    body = {"query": {"match_all": {}}, "size": 0}
    num_docs_active = DiscUtils.makeESRequest(body, active_index)['hits']['total']
    num_docs_inactive = DiscUtils.makeESRequest(body, inactive_index)['hits']['total']
    print('ES Number of documents in active index(%s): %s' % (active_index, num_docs_active))
    print('ES Number of documents in inactive index(%s): %s' % (inactive_index, num_docs_inactive))


    if num_docs_active > 0:
        if abs((num_docs_inactive - num_docs_active) / num_docs_active) > 0.05:
            if not force_run:
                raise Exception("Difference in the number of docs on the active and inactive indices is more than 5%")
            else:
                print("Warning!!!!  Difference in the number of docs on the active and inactive indices is more than 5%")
                print("Ignoring the difference because its a force run.")
        else:
            print("Check of 5 percent is passed")

    if argv['no_swap'] == True:
        print("\n\nIndex switch is not allowed. ACTIVE INDEX: %s\n\n" % active_index)
    else:
        resp = EsUtils.switch_index_alias("livecore", active_index, inactive_index)

    print("\n\nFinished running catalog pipeline for ElasticSearch. NEW ACTIVE INDEX: %s\n\n" % inactive_index)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--filepath", help='path to csv file')
    parser.add_argument("-u", "--url", help='url to csv file')
    parser.add_argument("-i", "--importattrs", action='store_true', help='Flag to import attributes first')
    parser.add_argument("-f", "--force", action='store_true', help='Force run the indexing, without any restrictions')
    parser.add_argument("-g", "--generate-third-party-feeds", action='store_true')
    parser.add_argument("-s", "--search-engine", default="elasticsearch")
    parser.add_argument("-l", "--limit", default=0, help='number of docs to index', type=int)
    parser.add_argument("--no-swap", action="store_true", help="Do not swap index")
    parser.add_argument("--throw-dummy-error", action="store_true", help="Throws an dummy error for testing purpose")
    parser.add_argument("--bestsellerupdate", default=True, type=bool)

    argv = vars(parser.parse_args())

    if argv['throw_dummy_error']:
      raise Exception("This is a dummy error")

    assert argv['search_engine'] in ['elasticsearch', None]

    file_path = argv['filepath']
    url = argv['url']
    if not url and not file_path:
        if hostname.startswith('admin'):
            url = FEED_URL
        elif hostname.startswith('preprod') or hostname.startswith('qa') or hostname.startswith('dev'):
            url = FEED_URL_PREPROD
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
            print("Downloading feed from: %s. Please wait..." % url)

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
    print("Time taken to import data from Nykaa: %s seconds" % time.strftime("%M min %S seconds",
                                                                             time.gmtime(import_duration)))

    # Index Elastic Search Data
    if argv['search_engine'] in ['elasticsearch', None]:
        indexESData(file_path, force_run)

    if argv['bestsellerupdate']:
        update_bestseller_data(100)

    script_stop = timeit.default_timer()
    script_duration = script_stop - script_start
    print("Total time taken for the script to run: %s seconds" % time.strftime("%M min %S seconds",
                                                                               time.gmtime(script_duration)))
