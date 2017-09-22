#!/usr/bin/python
import csv
import socket
import sys
import time
import timeit
import requests
import argparse
import traceback
import subprocess
import urllib.request
from pipelineUtils import SolrUtils, YIN_COLL, YANG_COLL
from importDataFromNykaa import NykaaImporter
from indexCatalog import CatalogIndexer
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils, CATALOG_COLLECTION_ALIAS

from pymongo import MongoClient
sys.path.append("/nykaa/scripts/utils")
from loopcounter import LoopCounter


client = MongoClient()
master_feed = client['feed_pipeline']['master_feed']

FEED_URL = "http://www.nykaa.com/media/feed/master_feed_gludo.csv"
FEED_LOCATION = '/data/nykaa/master_feed_gludo.csv'
hostname = socket.gethostname()

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--filepath", help='path to csv file')
parser.add_argument("-u", "--url", help='url to csv file')
parser.add_argument("-i", "--importattrs", action='store_true', help='Flag to import attributes first')
parser.add_argument("-f", "--force", action='store_true', help='Force run the indexing, without any restrictions')
argv = vars(parser.parse_args())

file_path = argv['filepath']
url = argv['url']
if not url and not file_path: 
  if hostname.startswith('admin'):
    url = "http://preprod.nykaa.com/media/feed/master_feed_gludo.csv"
  elif hostname.startswith('preprod'):
    url = "http://www.nykaa.com/media/feed/master_feed_gludo.csv"
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

with open(file_path) as csvfile:
  reader = csv.DictReader(csvfile)
  nrows = int(subprocess.check_output('wc -l ' + file_path, shell=True).decode().split()[0])
  ctr = LoopCounter(name='Importing feed to Mongo', total=nrows)

  for row in reader:
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    #print(row)
    master_feed.update({"product_id": row['product_id']}, {"$set": row}, upsert=True)

index_start = timeit.default_timer()
# indexing herer .... 
index_stop = timeit.default_timer()
index_duration = index_stop - index_start




script_stop = timeit.default_timer()
script_duration = script_stop - script_start

print("Time taken to index data to Solr: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))
print("Total time taken for the script to run: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
