import sys
import boto3
import json
import traceback

sys.path.append("/home/ubuntu/nykaa_scripts/")
from feed_pipeline.pipelineUtils import PipelineUtils

def insert_in_varnish_purging_sqs(docs,source):
    SQS_ENDPOINT, SQS_REGION = PipelineUtils.getDiscoveryVarnishPurgeSQSDetails()
    for doc in docs:
        purge_doc = {}
        purge_doc['sku'] = doc.get('sku')
        purge_doc['source'] = source
        purge_doc['data'] = doc
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
            print("Insertion in SQS failed for sku %s" % purge_doc['sku'])