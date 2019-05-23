#!/usr/bin/python
import time
import sys
from dateutil import tz
#from pipelineUtils import PipelineUtils
from datetime import datetime, timedelta

#sys.path.append('/var/www/pds_api/')
sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

from nykaa.settings import DISCOVERY_SQS_ENDPOINT

import argparse
import elasticsearch
import subprocess
import queue
import threading
import traceback
import json
total = 0
CHUNK_SIZE = 200
NUMBER_OF_THREADS = 4
def synchronized(func):
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func

@synchronized
def incrementGlobalCounter(increment):
    global total
    curr = total + increment
    total = curr
    return total

class Worker(threading.Thread):
    def __init__(self, q):
        self.q = q
        super().__init__()

    def run(self):
        while True:
            try:
                product_chunk = self.q.get(timeout=3)  # 3s timeout
                ScheduledPriceUpdater.updateChunkPrice(product_chunk)
            except queue.Empty:
                return
            self.q.task_done()


def getCurrentDateTime():
    current_datetime = datetime.utcnow()
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Asia/Kolkata')
    current_datetime = current_datetime.replace(tzinfo=from_zone)
    current_datetime = current_datetime.astimezone(to_zone)
    return current_datetime

print("=" * 30 + " %s ======= " % getCurrentDateTime())

def getCount():
    return int(subprocess.check_output(
        "ps aux | grep python | grep cron_pas_listener_es.py | grep -vE 'vim|grep|/bin/sh' | wc -l ",
        shell=True).strip())


if getCount() >= 2:
    print("getCount(): %r" % getCount())
    print("[%s] This script is already running. Exiting without doing anything" % getCurrentDateTime())
    print(str(
        subprocess.check_source_output("ps aux | grep python | grep cron_pas_listener_es.py | grep -vE 'vim|grep|/bin/sh' ",
                                shell=True)))
    exit()


class ScheduledPriceUpdater:

    @classmethod
    def getChunks(cls, data_list, chunk_size):
        """Yield successive n-sized chunks from list l."""
        for i in range(0, len(data_list), chunk_size):
            yield data_list[i:i + chunk_size]

    def updateChunkPrice(product_chunk):
        products = []
        sku_list = []
        psku_list = []

        for single_product in product_chunk:
            sku_list.append(single_product['sku'])
            psku_list.append(single_product['psku'])
            products.append({'sku': single_product['sku'], 'type': single_product['type']})
            if single_product['psku'] and single_product['psku'] != single_product['sku']:
                products.append({'sku': single_product['psku'], 'type': 'configurable'})

        new_sku_list = list(set(sku_list) | set(psku_list))
        sku_string = "','".join(new_sku_list)
        query = "SELECT product_sku, bundle_sku FROM bundle_products_mappings WHERE product_sku in('" + sku_string + "')"
        mysql_conn = PasUtils.mysqlConnection('r')
        results = PasUtils.fetchResults(mysql_conn, query)
        mysql_conn.close()
        for res in results:
            products.append({'sku': res['bundle_sku'], 'type': 'bundle'})

        products = [dict(t) for t in {tuple(d.items()) for d in products}]
        update_docs = []
        try:
            update_docs = PipelineUtils.getProductsToIndex(products, add_limit = True)
            if update_docs:
                DiscUtils.updateESCatalog(update_docs)
        except Exception as e:
            print(traceback.format_exc())

        total_count = incrementGlobalCounter(len(update_docs))
        print("[%s] Update progress: %s products updated" % (getCurrentDateTime(), total_count))


    def update():
        q = queue.Queue(maxsize=0)
        current_datetime = getCurrentDateTime()
        last_datetime = current_datetime - timedelta(hours=1)
        from IPython import embed
        import boto3
        # Create SQS client
        sqs = boto3.client('sqs')
        queue_url = DISCOVERY_SQS_ENDPOINT
        # Receive message from SQS queue
        startts = time.time()
        update_docs = []
        is_queue_empty=False
        while True:
          response = sqs.receive_message(
              QueueUrl=queue_url,
              AttributeNames=[
                  'SentTimestamp'
              ],
              MaxNumberOfMessages=10,
              MessageAttributeNames=[
                  'All'
              ],
              VisibilityTimeout=30,
              WaitTimeSeconds=0
          )
          if 'Messages' in response:
            for message in response['Messages']:
              #message = response['Messages'][0]
              receipt_handle = message['ReceiptHandle']
              update_docs.append(json.loads(message['Body'])[0])
              sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
              ) 
          else:
            is_queue_empty=True
            print("No more messages")

          if len(update_docs)==1000 or (len(update_docs)>=1 and is_queue_empty) :
            try:
              print("Updating %s docs" % len(update_docs))
              DiscUtils.updateESCatalog(update_docs)
            except elasticsearch.helpers.BulkIndexError as e:
              missing_skus = []
              for error in e.errors:
                if(error['update']['error']['type'] == "document_missing_exception"):
                  missing_skus.append(error['update']['data']['doc']['sku'])
              if missing_skus:
                print("Missing SKUs in ES %s" % missing_skus)  
            finally:
              update_docs.clear()
          if is_queue_empty:
            break

        endts=time.time()
        diff = endts - startts
        print("Time taken:  %s seconds" % diff)
        exit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, help='number of records in single index request')
    parser.add_argument("--threads", type=int, help='number of records in single index request')
    argv = vars(parser.parse_args())
    ScheduledPriceUpdater.update()
