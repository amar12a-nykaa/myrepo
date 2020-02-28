import httplib2
import argparse
import boto3
import json
import queue
import subprocess
import sys
import threading
import time
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os

sys.path.append("/home/ubuntu/nykaa_scripts/")
from feed_pipeline.pipelineUtils import PipelineUtils


filename = "/var/log/sqs_consumer/sqs_consumer.log"
def create_log_file():
    os.makedirs(os.path.dirname(filename), exist_ok=True)

NUMBER_OF_THREADS = 1


def get_consumer_count():
    return int(subprocess.check_output(
        "ps aux | grep python | grep varnish_purge_sqs_consumer.py | grep -vE 'vim|grep|/bin/sh|bash' | wc -l ",
        shell=True).strip())


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
                    self.callback(update_docs=chunk)
                    self.q.task_done()
            except queue.Empty:
                print(self.name + " zz..")
                time.sleep(15)
                continue




# print("=" * 30 + " %s ======= " % getCurrentDateTime())


class SQSConsumer:
    """
    TODO: THis consumer can go berserk if number of threads are increased and number of products per bulk are increased beyond a point.
    We need a throttling mechanism to cap the maximum rate of Indexing.
    """

    @classmethod
    def __init__(self):
        self.q = queue.Queue(maxsize=0)
        self.sqs = boto3.client("sqs", region_name='ap-south-1')
        self.sqs_endpoint, self.sqs_region = PipelineUtils.getDiscoveryVarnishPurgeSQSDetails()
        self.thread_manager = ThreadManager(self.q, callback=self.purge_varnish_for_product)
        self.thread_manager.start_threads(NUMBER_OF_THREADS)
        self.varnish_hosts = PipelineUtils.getVarnishDetails()

    def start(self, ):
        # Receive message from SQS queue
        startts = time.time()
        docs = []
        update_docs = []
        is_sqs_empty = False
        num_consecutive_empty_checks = 0
        num_products_processed = 0

        while not is_sqs_empty:
            response = self.sqs.receive_message(
                QueueUrl=self.sqs_endpoint,
                AttributeNames=["SentTimestamp"],
                MaxNumberOfMessages=1,
                MessageAttributeNames=["All"],
                VisibilityTimeout=30,
                WaitTimeSeconds=0,
            )
            from IPython import embed

            if "Messages" in response:
                for message in response["Messages"]:
                    receipt_handle = message["ReceiptHandle"]
                    body = json.loads(message["Body"])
                    docs.extend(body.get("sku_ids", ""))
                    update_docs.append({"sku_ids": docs, "source": body.get("source", "")})
                    self.sqs.delete_message(QueueUrl=self.sqs_endpoint, ReceiptHandle=receipt_handle)
            else:
                is_sqs_empty = True
                print("Main Thread: SQS is empty!")

            if len(update_docs) >= 1 or (len(update_docs) >= 1 and is_sqs_empty):
                print("Main Thread: Putting chunk of size %s in queue " % len(update_docs))
                #sku_ids = "|".join(update_docs)
                # self.purge_varnish_for_product(sku_ids)
                self.q.put_nowait(update_docs)
                num_products_processed += len(update_docs)
                docs = []
                update_docs = []
                #



        self.thread_manager.stop_workers()
        self.thread_manager.join()

        print("Main Thread: All threads finished")
        time_taken = time.time() - startts
        speed = round(num_products_processed / time_taken * 1.0)
        print("Main Thread: Number of products processed: %s @ %s products/sec" % (num_products_processed, speed))

    @classmethod
    def purge_varnish_for_product(self, update_docs):
        h = httplib2.Http(".cache")
        for doc in update_docs:
            sku_ids_list = doc.get("sku_ids", [])
            source = doc.get("source", "")
            sku_ids = "|".join(sku_ids_list)
            headers = {"X-depends-on": sku_ids}
            for varnish_host in self.varnish_hosts:
                try:
                    (resp, content) = h.request("http://%s/" % varnish_host, "BAN", body="", headers=headers)
                    if resp.status == 200:
                        self.log_info(sku_ids, source, varnish_host, "success 200")
                    else:
                        self.log_info(sku_ids, source, varnish_host, "failure {}".format(resp.status))
                except Exception as e:
                    self.log_info(sku_ids, source, varnish_host, "failure exception", e)

    @classmethod
    def log_info(self, sku_ids, source, host, status, exception=None):
        sku_ids = sku_ids.replace("|", ",")
        logger = logging.getLogger()
        create_log_file()
        file_logger = RotatingFileHandler(filename, maxBytes=5000000, backupCount=5)
        SIMPLE_LOG_FORMAT = {
            "status": "%(status)s",
            "msg": "%(message)s",
            "ts": "%(asctime)s",
            "sku_ids": "%(sku_ids)s",
            "source": "%(source)s",
            "host": "%(host)s"
        }
        extra = {'source': source, 'sku_ids': sku_ids, 'status': status, 'host': host}
        if exception:
            SIMPLE_LOG_FORMAT["exception"] = "%(exception)s"
            extra['exception'] = type(exception).__name__
        file_logger.setFormatter(logging.Formatter(json.dumps(SIMPLE_LOG_FORMAT)))
        file_logger.setLevel(logging.ERROR)
        file_logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            logger.addHandler(file_logger)
        logger.error("Discovery Varnish Purge Sqs consumer", extra=extra)


if __name__ == "__main__":
    if get_consumer_count() > 1:
        print("getPipelineCount(): %r" % get_consumer_count())
        print("[%s] Catalog pipeline running. Exiting without doing anything" % datetime.now())
        exit()
    parser = argparse.ArgumentParser()
    parser.add_argument("--threads", type=int, help="number of records in single index request")
    argv = vars(parser.parse_args())
    if argv["threads"]:
        NUMBER_OF_THREADS = argv["threads"]

    consumer = SQSConsumer()
    consumer.start()
