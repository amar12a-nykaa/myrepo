#!/usr/bin/python
import sys
from dateutil import tz
from pipelineUtils import PipelineUtils
from datetime import datetime, timedelta
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils, MemcacheUtils, CATALOG_COLLECTION_ALIAS
import argparse
import re
import sys
import subprocess
import math
import os
import json
import time
import pprint

CHUNK_SIZE = 100

def getCurrentDateTime():
  current_datetime = datetime.utcnow()
  from_zone = tz.gettz('UTC')
  to_zone = tz.gettz('Asia/Kolkata')
  current_datetime = current_datetime.replace(tzinfo=from_zone)
  current_datetime = current_datetime.astimezone(to_zone) 
  return current_datetime

print("=" * 30 + " %s ======= " % getCurrentDateTime())
def getCount():
  return int(subprocess.check_output("ps aux | grep python | grep indexScheduledPrices.py | grep -vE 'vim|grep|/bin/sh' | wc -l ", shell=True).strip())

if getCount() >= 2: 
  print("getCount(): %r" % getCount())
  print("[%s] This script is already running. Exiting without doing anything"% getCurrentDateTime())
  print(str(subprocess.check_output("ps aux | grep python | grep indexScheduledPrices.py | grep -vE 'vim|grep|/bin/sh' ", shell=True)))
  exit()


class ScheduledPriceUpdater:

  def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
      yield l[i:i + n]

  def update():
    #Current time
    current_datetime = getCurrentDateTime()
    last_datetime = current_datetime - timedelta(hours=1)
    try:
      with open("last_update_time.txt", "r+") as f:    
        content = f.read()
        if content:
          last_datetime = content
          print("reading last_datetime from file: %s" % last_datetime)
        f.seek(0)
        f.write(str(current_datetime))
        f.truncate()
    except FileNotFoundError:
      print("FileNotFoundError")
      with open("last_update_time.txt", "w") as f:
        f.write(str(current_datetime))
    except Exception as e:
      print("[ERROR] %s"% str(e))

    last_datetime = '2018-03-21 16:00:32.765618+05:30'
    where_clause = " WHERE ((schedule_start > %s AND schedule_start <= %s) OR (schedule_end > %s AND schedule_end <= %s))"
    product_type_condition = " AND type = %s"
    product_updated_count = 0
 
    mysql_conn = Utils.mysqlConnection('r')
    query = "SELECT sku, psku, type FROM products" + where_clause + product_type_condition
    print("[%s] "% getCurrentDateTime() + query % (last_datetime, current_datetime, last_datetime, current_datetime, 'simple'))
    results = Utils.fetchResults(mysql_conn, query, (last_datetime, current_datetime, last_datetime, current_datetime, 'simple'))    
    print("[%s] Starting simple product updates" % getCurrentDateTime())

    chunk_results = list(ScheduledPriceUpdater.chunks(results, CHUNK_SIZE))
    total_count = 0
    for product in chunk_results:
      products = []
      sku_list = []
      psku_list = []

      for single_product in product:
        sku_list.append(single_product['sku'])
        psku_list.append(single_product['psku'])
        products.append({'sku': single_product['sku'], 'type': single_product['type']})
        if single_product['psku'] and single_product['psku'] != single_product['sku']:
          products.append({'sku': single_product['psku'], 'type': 'configurable'})
        
      new_sku_list = sku_list + list(set(psku_list) - set(sku_list))
      sku_string = "','".join(new_sku_list)
      query = "SELECT product_sku, bundle_sku FROM bundle_products_mappings WHERE product_sku in('" + sku_string + "')"
      results = Utils.fetchResults(mysql_conn, query)

      for res in results:
        products.append({'sku': res['bundle_sku'], 'type': 'bundle'})

      update_docs = PipelineUtils.getProductsToIndex(products)
      if update_docs:
        Utils.updateESCatalog(update_docs)
      total_count += len(update_docs)
      

      # product_updated_count += PipelineUtils.updateCatalog(product['sku'], product['psku'], product['type'])
      # if product_updated_count % 100 == 0:
      # print("[%s] Update progress: %s products updated" % (getCurrentDateTime(), product_updated_count))
    
    
    mysql_conn = Utils.mysqlConnection('r')
    products = []
    query = "SELECT sku FROM bundles" + where_clause
    results = Utils.fetchResults(mysql_conn, query, (last_datetime, current_datetime, last_datetime, current_datetime))
    print("[%s] Starting bundle product updates" % getCurrentDateTime())
    
    for bundle in results:
      products.append({'sku': sku, 'type': 'bundle'})
      if product_updated_count % 100 == 0:
        print("[%s] Update progress: %s products updated" % (getCurrentDateTime(), product_updated_count))
    
    update_docs = PipelineUtils.getProductsToIndex(products)
    if update_docs:
      Utils.updateESCatalog(update_docs)
    
    mysql_conn.close()
    print("\n[%s] Total %s products updated."%(getCurrentDateTime(), product_updated_count))

if __name__ == "__main__":
  ScheduledPriceUpdater.update()
