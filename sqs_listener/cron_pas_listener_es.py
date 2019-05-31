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

sys.path.append("/var/www/pds_api/")
from pas.v2.utils import Utils as PasUtils
from nykaa.settings import DISCOVERY_SQS_ENDPOINT

sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils


total = 0
CHUNK_SIZE = 200


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
            print("Adding None to queue")
            self.q.put(None)

    def join(self):
        print("Waiting for threads to finish .. ")
        for thread in self.threads:
            thread.join()


class WorkerThread(threading.Thread):
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


def getCurrentDateTime():
    current_datetime = datetime.utcnow()
    from_zone = tz.gettz("UTC")
    to_zone = tz.gettz("Asia/Kolkata")
    current_datetime = current_datetime.replace(tzinfo=from_zone)
    current_datetime = current_datetime.astimezone(to_zone)
    return current_datetime


print("=" * 30 + " %s ======= " % getCurrentDateTime())


ES_BULK_UPLOAD_BATCH_SIZE = 100
NUMBER_OF_THREADS = 1


class SQSConsumer:
    @classmethod
    def __init__(self):
        self.q = queue.Queue(maxsize=0)
        self.sqs = boto3.client("sqs")

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
            #time.sleep(1)
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
                print("SQS is empty!")

            # DELETE ME
            # is_sqs_empty = True

            if len(update_docs) >= ES_BULK_UPLOAD_BATCH_SIZE or (len(update_docs) >= 1 and is_sqs_empty):
                print("Main Thread: Putting chunk of size %s in queue " % len(update_docs))
                self.q.put_nowait(update_docs)
                num_products_processed += len(update_docs)
                update_docs = []

        self.thread_manager.stop_workers()
        self.thread_manager.join()

        print("All threads finished")
        time_taken = time.time() - startts
        speed = round(num_products_processed / time_taken * 1.0)
        print("Number of products processed: %s @ %s products/sec" % (num_products_processed, speed))

    @classmethod
    def upload_one_chunk(cls, chunk, threadname):
        try:
            update_docs = chunk
            print(threadname + ": Sending %s docs to bulk upload" % len(update_docs))
            DiscUtils.updateESCatalog(update_docs, refresh=True)
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
