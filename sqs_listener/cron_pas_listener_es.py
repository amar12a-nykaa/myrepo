#!/usr/bin/python
import argparse
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
from datetime import datetime, timedelta

sys.path.append("/home/ubuntu/nykaa_scripts/")
from feed_pipeline.pipelineUtils import PipelineUtils

sys.path.append("/var/www/pds_api/")
from pas.v2.utils import Utils as PasUtils
from nykaa.settings import DISCOVERY_SQS_ENDPOINT

sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

total = 0
CHUNK_SIZE = 200


def getCurrentDateTime():
    current_datetime = datetime.utcnow()
    from_zone = tz.gettz("UTC")
    to_zone = tz.gettz("Asia/Kolkata")
    current_datetime = current_datetime.replace(tzinfo=from_zone)
    current_datetime = current_datetime.astimezone(to_zone)
    return current_datetime

def insert_in_varnish_purging_sqs(docs):
    SQS_ENDPOINT, SQS_REGION = PipelineUtils.getDiscoveryVarnishPurgeSQSDetails()
    for doc in docs:
        purge_doc = {}
        purge_doc['sku'] = doc.get('sku')
        purge_doc['purge_trigger'] = "pas"
        purge_doc['doc'] = doc
        try:
            sqs = boto3.client("sqs", region_name=SQS_REGION)
            queue_url = SQS_ENDPOINT
            response = sqs.send_message(
                QueueUrl=queue_url,
                DelaySeconds=0,
                MessageAttributes={},
                MessageBody=(json.dumps(purge_doc, default=str))
            )
        except:
            print(traceback.format_exc())
            print("Insertion in SQS failed")


def getCount():
    return int(subprocess.check_output(
        "ps aux | grep python | grep cron_pas_listener_es.py | grep -vE 'vim|grep|/bin/sh' | wc -l ",
        shell=True).strip())


if getCount() >= 2:
    print("getCount(): %r" % getCount())
    print("[%s] This script is already running. Exiting without doing anything" % getCurrentDateTime())
    print(str(
        subprocess.check_output("ps aux | grep python | grep cron_pas_listener_es.py | grep -vE 'vim|grep|/bin/sh' ",
                                shell=True)))
    exit()


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




print("=" * 30 + " %s ======= " % getCurrentDateTime())


ES_BULK_UPLOAD_BATCH_SIZE = 100
NUMBER_OF_THREADS = 1


class SQSConsumer:
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
        update_docs = []
        is_sqs_empty = False
        num_consecutive_empty_checks = 0
        num_products_processed = 0

        while not is_sqs_empty:
            response = self.sqs.receive_message(
                QueueUrl=DISCOVERY_SQS_ENDPOINT,
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
                    update_docs += json.loads(message["Body"])
                    self.sqs.delete_message(QueueUrl=DISCOVERY_SQS_ENDPOINT, ReceiptHandle=receipt_handle)
            else:
                is_sqs_empty = True
                print("Main Thread: SQS is empty!")

            if len(update_docs) >= ES_BULK_UPLOAD_BATCH_SIZE or (len(update_docs) >= 1 and is_sqs_empty):
                print("Main Thread: Putting chunk of size %s in queue " % len(update_docs))
                self.q.put_nowait(update_docs)
                num_products_processed += len(update_docs)
                update_docs = []

        self.thread_manager.stop_workers()
        self.thread_manager.join()

        print("Main Thread: All threads finished")
        time_taken = time.time() - startts
        speed = round(num_products_processed / time_taken * 1.0)
        print("Main Thread: Number of products processed: %s @ %s products/sec" % (num_products_processed, speed))

    @classmethod
    def upload_one_chunk(cls, chunk, threadname):
        """
        Callback function called by a thread to bulk upload the products
        """
        try:
            update_docs = chunk
            #print(chunk)
            print(threadname + ": Sending %s docs to bulk upload" % len(update_docs))
            response = DiscUtils.updateESCatalog(update_docs, refresh=True, raise_on_error=False)
            insert_in_varnish_purging_sqs(update_docs)
            print("response: ", response)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, help="number of records in single index request")
    argv = vars(parser.parse_args())
    if argv["threads"]:
        NUMBER_OF_THREADS = argv["threads"]

    consumer = SQSConsumer()
    consumer.start()
