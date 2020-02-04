import httplib2
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





    # !/usr/bin/python


    # from utils.discovery_varnish_purge_utils import *
    #
    # sys.path.append("/var/www/pds_api/")
    # from pas.v2.utils import Utils as PasUtils
    # from nykaa.settings import DISCOVERY_SQS_ENDPOINT
    #
    # sys.path.append("/var/www/discovery_api")
    # from disc.v2.utils import Utils as DiscUtils
    #
    # total = 0
    # CHUNK_SIZE = 200



class SQSConsumer:
    """
    TODO: THis consumer can go berserk if number of threads are increased and number of products per bulk are increased beyond a point.
    We need a throttling mechanism to cap the maximum rate of Indexing.
    """

    @classmethod
    def __init__(self):
        # self.q = queue.Queue(maxsize=0)
        self.sqs = boto3.client("sqs", region_name='ap-south-1')
        self.sqs_endpoint, self.sqs_region = PipelineUtils.getDiscoveryVarnishPurgeSQSDetails()
        # self.thread_manager = ThreadManager(self.q, callback=self.upload_one_chunk)
        # self.thread_manager.start_threads(NUMBER_OF_THREADS)

    def start(self, ):
        # Receive message from SQS queue
        startts = time.time()
        update_docs = []
        is_sqs_empty = False
        num_consecutive_empty_checks = 0
        num_products_processed = 0

        while not is_sqs_empty:
            response = self.sqs.receive_message(
                QueueUrl=self.sqs_endpoint,
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
                    body = json.loads(message["Body"])
                    update_docs += body.get("sku", "")
                    print(update_docs)
                    self.sqs.delete_message(QueueUrl=self.sqs_endpoint, ReceiptHandle=receipt_handle)
            else:
                is_sqs_empty = True
                print("Main Thread: SQS is empty!")

            if len(update_docs) <= 10 or (len(update_docs) >= 1 and is_sqs_empty):
                print("Main Thread: Putting chunk of size %s in queue " % len(update_docs))
                sku_ids = "|".join(update_docs)
                self.purge_varnish_for_product(sku_ids)
                # self.q.put_nowait(update_docs)
                # num_products_processed += len(update_docs)
                # update_docs = []



        # self.thread_manager.stop_workers()
        # self.thread_manager.join()
        #
        # print("Main Thread: All threads finished")
        # time_taken = time.time() - startts
        # speed = round(num_products_processed / time_taken * 1.0)
        # print("Main Thread: Number of products processed: %s @ %s products/sec" % (num_products_processed, speed))

    @classmethod
    def purge_varnish_for_product(sku_ids):
        print(sku_ids)
        h = httplib2.Http(".cache")
        varnish_hosts = ["172.26.17.250:80"]
        headers = {"X-depends-on": sku_ids}
        for varnish_host in varnish_hosts:
            try:
                (resp, content) = h.request("http://%s/" % varnish_host, "BAN", body="", headers=headers)
                print(resp)
                # logger.info(content)
            except Exception as e:
                print(e)
                # logger.info(type(e))
    # def upload_one_chunk(cls, chunk, threadname):
    #     """
    #     Callback function called by a thread to bulk upload the products
    #     """
    #     try:
    #         update_docs = chunk
    #         # print(chunk)
    #         print(threadname + ": Sending %s docs to bulk upload" % len(update_docs))
    #         response = DiscUtils.updateESCatalog(update_docs, refresh=True, raise_on_error=False)
    #         # insert_in_varnish_purging_sqs(update_docs, "pas")
    #         print("response: ", response)
    #         print(threadname + ": Done with one batch of bulk upload")
    #     except elasticsearch.helpers.BulkIndexError as e:
    #         missing_skus = []
    #         for error in e.errors:
    #             if error["update"]["error"]["type"] == "document_missing_exception":
    #                 missing_skus.append(error["update"]["data"]["doc"]["sku"])
    #             else:
    #                 print(
    #                         "Unhandled Error for sku '%s' - %s"
    #                         % (error["update"]["data"]["doc"]["sku"], error["update"]["error"]["type"])
    #                 )
    #         print("Missing Skus count", len(missing_skus))
    #         print("Missing Skus", missing_skus)
    #     except:
    #         raise

if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--threads", type=int, help="number of records in single index request")
    # argv = vars(parser.parse_args())
    # if argv["threads"]:
    #     NUMBER_OF_THREADS = argv["threads"]

    consumer = SQSConsumer()
    consumer.start()
