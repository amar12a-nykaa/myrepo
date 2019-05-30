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


class WorkerThread(threading.Thread):
    def __init__(self, q, name):
        self.q = q
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
                    SQSConsumer.upload_one_chunk(chunk, threadname=self.name)
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


ES_BULK_UPLOAD_BATCH_SIZE = 1000
NUMBER_OF_THREADS = 2


class SQSConsumer:
    @classmethod
    def __init__(self):
        self.q = queue.Queue(maxsize=0)
        self.sqs = boto3.client("sqs")
        self.threads = []

        for i, _ in enumerate(range(NUMBER_OF_THREADS)):
            name = "T" + str(i)
            thread = WorkerThread(self.q, name=name)
            thread.start()
            self.threads.append(thread)

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
                print("SQS is empty!")

            if len(update_docs) >= ES_BULK_UPLOAD_BATCH_SIZE or (len(update_docs) >= 1 and is_sqs_empty):
                print("Main Thread: Putting chunk of size %s in queue " % len(update_docs))
                self.q.put_nowait(update_docs)
                num_products_processed += len(update_docs)
                update_docs = []

        # stop workers
        for _ in range(NUMBER_OF_THREADS):
            print("Adding None to queue")
            self.q.put(None)

        print("Waiting for threads to finish .. ")
        for thread in self.threads:
            thread.join()

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
            print("Missing Skus", len(missing_skus))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, help="number of records in single index request")
    parser.add_argument("--threads", type=int, help="number of records in single index request")
    argv = vars(parser.parse_args())
    consumer = SQSConsumer()
    consumer.start()
