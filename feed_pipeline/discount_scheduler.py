#!/usr/bin/python
import sys
from dateutil import tz
from pipelineUtils import PipelineUtils
from datetime import datetime, timedelta

sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
import argparse

import subprocess
import queue
import threading
import traceback
import json
sys.path.append("/home/ubuntu/nykaa_scripts/")
from utils.priceUpdateLogUtils import PriceUpdateLogUtils
import uuid

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
    def __init__(self, q,schedule_start,schedule_end):
        self.q = q
        self.schedule_start = schedule_start
        self.schedule_end = schedule_end
        super().__init__()

    def run(self):
        while True:
            try:
                product_chunk = self.q.get(timeout=3)  # 3s timeout
                ScheduledPriceUpdater.updateChunkPrice(product_chunk,self.schedule_start,self.schedule_end)
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
        "ps aux | grep python | grep discount_scheduler.py | grep -vE 'vim|grep|/bin/sh' | wc -l ",
        shell=True).strip())


if getCount() >= 2:
    print("getCount(): %r" % getCount())
    print("[%s] This script is already running. Exiting without doing anything" % getCurrentDateTime())
    print(str(
        subprocess.check_source_output("ps aux | grep python | grep discount_scheduler.py | grep -vE 'vim|grep|/bin/sh' ",
                                shell=True)))
    exit()


class ScheduledPriceUpdater:

    @classmethod
    def getChunks(cls, data_list, chunk_size):
        """Yield successive n-sized chunks from list l."""
        for i in range(0, len(data_list), chunk_size):
            yield data_list[i:i + chunk_size]

    def updateChunkPrice(product_chunk,schedule_start,schedule_end):
        products = []
        sku_list = []
        psku_list = []
        totalProductsToLog = []
        priceChangeData={}
        batch_Id = uuid.uuid4().int
        for single_product in product_chunk:
            sku_list.append(single_product['sku'])
            psku_list.append(single_product['psku'])
            products.append({'sku': single_product['sku'], 'type': single_product['type']})
            if single_product['psku'] and single_product['psku'] != single_product['sku']:
                products.append({'sku': single_product['psku'], 'type': 'configurable'})
            totalProductsToLog.append(
                (
                    batch_Id,
                    single_product['sku'],
                    json.dumps({"scheduled_discount":single_product['scheduled_discount']})
                )
            )
            priceChangeData[single_product['sku']] = {}
            priceChangeData[single_product['sku']]['old_price'] = single_product['sp']
            priceChangeData[single_product['sku']]['type'] = single_product['type']
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
                for single_doc in update_docs:
                    print("sku : %s" % single_doc['sku'])
                    if single_doc['sku'] in priceChangeData:
                      priceChangeData[single_doc['sku']]['new_price'] = single_doc['price']
        except Exception as e:
            print(traceback.format_exc())

        total_count = incrementGlobalCounter(len(update_docs))
        print("[%s] Update progress: %s products updated" % (getCurrentDateTime(), total_count))
        if totalProductsToLog:
            PriceUpdateLogUtils.logBulkChangeViaProductScheduleUpdate(batch_Id, "cron_schedule_es", schedule_start, schedule_end,totalProductsToLog)
        if priceChangeData:
            PriceUpdateLogUtils.logBulkPriceChange(priceChangeData)
    def update():
        # Current time
        q = queue.Queue(maxsize=0)
        current_datetime = getCurrentDateTime()
        last_datetime = current_datetime - timedelta(hours=1)
        totalBundlesToLog = []
        batch_Id = uuid.uuid4().int
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
            print("[ERROR] %s" % str(e))
        where_clause = " WHERE ((schedule_start > %s AND schedule_start <= %s) OR (schedule_end > %s AND schedule_end <= %s))"
        product_type_condition = " AND type = %s"
        product_updated_count = 0

        query = "SELECT sku, psku, type,scheduled_discount,schedule_start,schedule_end,sp FROM products" + where_clause + product_type_condition
        print("[%s] " % getCurrentDateTime() + query % (last_datetime, current_datetime, last_datetime, current_datetime, 'simple'))
        mysql_conn = PasUtils.mysqlConnection('r')
        results = PasUtils.fetchResults(mysql_conn, query, (last_datetime, current_datetime, last_datetime, current_datetime, 'simple'))
        mysql_conn.close()

        print("[%s] Starting simple product updates" % getCurrentDateTime())
        chunk_size = argv['batch_size']
        num_of_threads = argv['threads']
        
        if not chunk_size:
            chunk_size = CHUNK_SIZE

        if not num_of_threads:
            num_of_threads = NUMBER_OF_THREADS

        chunk_results = list(ScheduledPriceUpdater.getChunks(results, chunk_size))

        for single_chunk in chunk_results:
            q.put_nowait(single_chunk)

        for _ in range(num_of_threads):
            Worker(q,last_datetime,current_datetime).start()
        q.join()

        # Code for bundle products
        products = []
        query = "SELECT sku,scheduled_discount FROM bundles" + where_clause
        mysql_conn = PasUtils.mysqlConnection('r')
        results = PasUtils.fetchResults(mysql_conn, query, (last_datetime, current_datetime, last_datetime, current_datetime))
        mysql_conn.close()
        print("[%s] Starting bundle product updates" % getCurrentDateTime())

        for bundle in results:
            product_updated_count += 1
            products.append({'sku': bundle['sku'], 'type': 'bundle'})
            totalBundlesToLog.append(
                (
                    batch_Id,
                    bundle['sku'],
                    json.dumps({"scheduled_discount": bundle['scheduled_discount']})
                )
            )
            if product_updated_count % 100 == 0:
                print("[%s] Update progress: %s products updated" % (getCurrentDateTime(), product_updated_count))

        try:
            update_docs = PipelineUtils.getProductsToIndex(products, add_limit=True)
            if update_docs:
                DiscUtils.updateESCatalog(update_docs)
                for singleBundle in update_docs:
                    print("bundle sku: %s" % singleBundle['sku'])
        except Exception as e:
            print(traceback.format_exc())

        print("\n[%s] Total %s products updated." % (getCurrentDateTime(), product_updated_count))
        if totalBundlesToLog:
            PriceUpdateLogUtils.logBulkChangeViaProductScheduleUpdate(batch_Id, "cron_schedule_es", last_datetime,
                                                                      current_datetime, totalBundlesToLog)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, help='number of records in single index request')
    parser.add_argument("--threads", type=int, help='number of records in single index request')
    argv = vars(parser.parse_args())
    ScheduledPriceUpdater.update()
