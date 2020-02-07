import sys
import boto3
import json
import traceback
import os
import logging
from logging.handlers import RotatingFileHandler

sys.path.append("/home/ubuntu/nykaa_scripts/")
from feed_pipeline.pipelineUtils import PipelineUtils

CHUNK_SIZE = 20
filename = "/var/log/discovery_purge_sqs/discovery_purge_sqs.log"
def create_log_file():
    os.makedirs(os.path.dirname(filename), exist_ok=True)

def chunkify(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def log_info(purge_doc,status,exception=None):
    logger = logging.getLogger()
    create_log_file()
    file_logger = RotatingFileHandler(filename, maxBytes=5000000, backupCount=5)
    SIMPLE_LOG_FORMAT = {
        "status": "%(status)s",
        "msg": "%(message)s",
        "ts": "%(asctime)s",
        "sku_ids": "%(sku_ids)s",
        "source": "%(source)s",
    }
    extra = {'source': purge_doc['source'],'sku_ids': purge_doc['sku_ids'], 'status': status}
    if exception:
        SIMPLE_LOG_FORMAT["exception"] = "%(exception)s"
        extra['exception'] = type(exception).__name__
    file_logger.setFormatter(logging.Formatter(json.dumps(SIMPLE_LOG_FORMAT)))
    file_logger.setLevel(logging.ERROR)
    file_logger.setLevel(logging.INFO)
    logger.addHandler(file_logger)
    logger.error("Discovery Varnish Purge Sqs insertion", extra=extra)

def insert_in_varnish_purging_sqs(docs,source):
    sku_list = [doc.get('sku',"") for doc in docs]
    chunks = chunkify(sku_list,CHUNK_SIZE)
    SQS_ENDPOINT, SQS_REGION = PipelineUtils.getDiscoveryVarnishPurgeSQSDetails()
    sqs = boto3.client("sqs", region_name=SQS_REGION)
    queue_url = SQS_ENDPOINT
    for chunk in chunks:
        purge_doc = {}
        purge_doc['sku_ids'] = chunk
        purge_doc['source'] = source
        try:
            response = sqs.send_message(
                QueueUrl=queue_url,
                DelaySeconds=0,
                MessageAttributes={},
                MessageBody=(json.dumps(purge_doc, default=str))
            )
            log_info(purge_doc, "success")
        except Exception as e:
            print(traceback.format_exc())
            print("Insertion in SQS failed for skus %s and source %s" % (purge_doc['sku_ids'], purge_doc['source']))
            log_info(purge_doc, "failure", e)
