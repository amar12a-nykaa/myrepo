#!/usr/bin/python
import argparse
from collections import OrderedDict

import boto3
import elasticsearch
import json
import queue
import subprocess
import sys
import threading
import time
import traceback
from dateutil import tz
from datetime import datetime
from dateutil import parser


sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils


total = 0
CHUNK_SIZE = 200


def format_date(offer_date):
    offer_date = parser.parse(offer_date)
    offer_date = offer_date.strftime("%Y-%m-%d %H:%M:%S")
    return offer_date


def getCurrentDateTime():
    current_datetime = datetime.utcnow()
    from_zone = tz.gettz("UTC")
    to_zone = tz.gettz("Asia/Kolkata")
    current_datetime = current_datetime.replace(tzinfo=from_zone)
    current_datetime = current_datetime.astimezone(to_zone)
    return current_datetime


def getOfferConsumerCount():
    return int(subprocess.check_output(
        "ps aux | grep python | grep cron_offer_listener_es.py | grep -vE 'vim|grep|/bin/sh' | wc -l ",
        shell=True).strip())

def getPipelineCount():
    return int(subprocess.check_output(
        "ps aux | grep python | grep catalogPipelineMultithreaded.py | grep -vE 'vim|grep|/bin/sh' | wc -l ",
        shell=True).strip())

print("=" * 30 + " %s ======= " % getCurrentDateTime())


ES_BULK_UPLOAD_BATCH_SIZE = 100
NUMBER_OF_THREADS = 5


class OfferSQSConsumer:
    """
    TODO: THis consumer can go berserk if number of threads are increased and number of products per bulk are increased beyond a point. 
    We need a throttling mechanism to cap the maximum rate of Indexing.
    """

    @classmethod
    def __init__(self):
        self.q = queue.Queue(maxsize=0)
        self.sqs = boto3.client("sqs", region_name='ap-south-1')

        self.thread_manager = ThreadManager(self.q, callback=self.upload_one_chunk)
        self.thread_manager.start_threads(NUMBER_OF_THREADS)



    def start(self,):
        # Receive message from SQS queue
        startts = time.time()
        time.sleep(1)
        update_docs = []
        is_sqs_empty = False
        num_consecutive_empty_checks = 0
        num_products_processed = 0

        index = 100
        while index > 0:
            index -= 1
            self.q.put_nowait({})

        sqs_endpoint = DiscUtils.get_offer_delta_sqs_details()

        while not is_sqs_empty:

            response = self.sqs.receive_message(
                QueueUrl=sqs_endpoint,
                AttributeNames=["SentTimestamp"],
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
                VisibilityTimeout=30,
                WaitTimeSeconds=0,
            )
            from IPython import embed

            if "Messages" in response:
                for message in response["Messages"]:
                    receipt_handle = message["ReceiptHandle"]
                    self.q.put_nowait(json.loads(message["Body"]))
                    self.sqs.delete_message(QueueUrl=sqs_endpoint, ReceiptHandle=receipt_handle)
            else:
                is_sqs_empty = True
                print("Main Thread: SQS is empty!")

        self.thread_manager.stop_workers()
        self.thread_manager.join()

        print("Main Thread: All threads finished")
        time_taken = time.time() - startts
        speed = round(num_products_processed / time_taken * 1.0)
        print("Main Thread: Number of products processed: %s @ %s products/sec" % (num_products_processed, speed))

    @staticmethod
    def offers_data_merge(doc, offers_data):
        nykaa_offers = offers_data.get('nykaa', [])
        for offer in nykaa_offers:
            if not offer.get('offer_start_date'):
                offer['offer_start_date'] = ""
            else:
                offer['offer_start_date'] = format_date(offer['offer_start_date'])
            if not offer.get('offer_end_date'):
                offer['offer_end_date'] = ""
            else:
                offer['offer_end_date'] = format_date(offer['offer_end_date'])
        doc['offers'] = nykaa_offers
        doc['offer_count'] = len(doc['offers'])
        doc['offer_ids'] = []
        doc['offer_facet'] = []
        for offer in nykaa_offers:
            doc['key'] = []
            doc['key'].append(offer)
            offer_facet = OrderedDict()
            offer_facet['id'] = offer.get("id")
            offer_facet['name'] = offer.get("name")
            doc['offer_facet'].append(offer_facet)
            doc['offer_ids'].append(offer.get("id"))

        nykaaman_offers = offers_data.get('nykaaman', [])
        for offer in nykaaman_offers:
            if not offer.get('offer_start_date'):
                offer['offer_start_date'] = ""
            else:
                offer['offer_start_date'] = format_date(offer['offer_start_date'])
            if not offer.get('offer_end_date'):
                offer['offer_end_date'] = ""
            else:
                offer['offer_end_date'] = format_date(offer['offer_end_date'])
        doc['nykaaman_offers'] = nykaaman_offers
        doc['nykaaman_offer_count'] = len(doc['nykaaman_offers'])
        doc['nykaaman_offer_ids'] = []
        doc['nykaaman_offer_facet'] = []
        for offer in nykaaman_offers:
            nykaaman_offer_facet = OrderedDict()
            nykaaman_offer_facet['id'] = offer['id']
            nykaaman_offer_facet['name'] = offer['name']
            doc['nykaaman_offer_facet'].append(nykaaman_offer_facet)
            doc['nykaaman_offer_ids'].append(offer['id'])

        nykaa_pro_offers = offers_data.get('nykaa_pro', [])
        for offer in nykaa_pro_offers:
            if not offer.get('offer_start_date'):
                offer['offer_start_date'] = ""
            else:
                offer['offer_start_date'] = format_date(offer['offer_start_date'])
            if not offer.get('offer_end_date'):
                offer['offer_end_date'] = ""
            else:
                offer['offer_end_date'] = format_date(offer['offer_end_date'])
        doc['nykaa_pro_offers'] = nykaa_pro_offers
        doc['nykaa_pro_offer_count'] = len(doc['nykaa_pro_offers'])
        doc['nykaa_pro_offer_ids'] = []
        doc['nykaa_pro_offer_facet'] = []
        for offer in nykaa_pro_offers:
            nykaa_pro_offer_facet = OrderedDict()
            nykaa_pro_offer_facet['id'] = offer['id']
            nykaa_pro_offer_facet['name'] = offer['name']
            doc['nykaa_pro_offer_facet'].append(nykaa_pro_offer_facet)
            doc['nykaa_pro_offer_ids'].append(offer['id'])
        doc['delta_offer_update'] = True

    @classmethod
    def upload_one_chunk(cls, chunk, threadname):
        """
        Callback function called by a thread to bulk upload the products
        """
        thread_id = threading.get_ident()
        print("Thread {} start".format(thread_id))

        process_docs = []
        for sku in chunk:
            offer_data = chunk.get(sku)
            if offer_data and sku:
                process_doc = {"sku": offer_data['sku']}
                OfferSQSConsumer.offers_data_merge(process_doc, offer_data)
                process_docs.append(process_doc)

        try:
            print(threadname + ": Sending %s docs to bulk upload" % len(process_docs))
            response = DiscUtils.updateESCatalog(process_docs, refresh=True, raise_on_error=False)
            # print("response: ", response)
            print(threadname + ": Done with one batch of bulk upload")
        except elasticsearch.helpers.BulkIndexError as e:
            missing_skus = []
            for error in e.errors:
                if error["update"]["error"]["type"] == "document_missing_exception":
                    missing_skus.append(error["update"]["data"]["doc"]["sku"])
                else:
                    print(
                        "Unhandled Error for sku '%s' - %s"
                        % (error["update"]["data"]["doc"]["sku"], error["update"]["error"]["type"])
                    )
            print("Missing Skus count", len(missing_skus))
            print("Missing Skus", missing_skus)
        except:
            raise


class ThreadManager:
    """
        A generic class for running multithreaded applications.
        We can resuse this class for other purpose too.
        This is to be used along with WorkerThread class
    """

    def __init__(self, q, callback):
        self.threads = []
        self.q = q
        self.callback = callback

    def start_threads(self, number_of_threads):
        for i, _ in enumerate(range(number_of_threads)):
            name = "T" + str(i)
            thread = WorkerThread(self.q, callback=self.callback, name=name)
            thread.start()
            self.threads.append(thread)

    def stop_workers(self):
        for _ in range(NUMBER_OF_THREADS):
            print("Main Thread: Adding None to queue")
            self.q.put(None)

    def join(self):
        print("Main Thread: Waiting for threads to finish .. ")
        for thread in self.threads:
            thread.join()


class WorkerThread(threading.Thread):
    """
        A generic class for running multithreaded applications.
        We can resuse this class for other purpose too.
        This is best used along with ThreadManager class
    """

    def __init__(self, q, callback, name):
        self.q = q
        self.callback = callback
        super().__init__(name=name)

    def run(self):
        while True:
            try:
                chunk = self.q.get(timeout=3)  # 3s timeout
                if chunk is None:
                    print(self.name + " Dying now.")
                    self.q.task_done()
                    break
                else:
                    self.callback(chunk=chunk, threadname=self.name)
                    self.q.task_done()
            except queue.Empty:
                print(self.name + " zz..")
                time.sleep(15)
                continue


if __name__ == "__main__":
    if getOfferConsumerCount() >= 2:
        print("getCount(): %r" % getOfferConsumerCount())
        print("[%s] This script is already running. Exiting without doing anything" % getCurrentDateTime())
        exit()
    if getPipelineCount() > 1:
        print("getPipelineCount(): %r" % getPipelineCount())
        print("[%s] Catalog pipeline running. Exiting without doing anything" % getCurrentDateTime())
        exit()
        
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, help="number of records in single index request")
    argv = vars(parser.parse_args())
    if argv["threads"]:
        NUMBER_OF_THREADS = argv["threads"]

    consumer = OfferSQSConsumer()
    consumer.start()
