import sys
import boto3
import json
import traceback

sys.path.append("/home/ubuntu/nykaa_scripts/")
from feed_pipeline.pipelineUtils import PipelineUtils

CHUNK_SIZE = 20

def chunkify(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

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
        except:
            print(traceback.format_exc())
            print("Insertion in SQS failed for skus %s and source %s" % (purge_doc['sku_ids'], purge_doc['source']))
            with open('/var/log/discovery_purge_sqs_error.log','a') as the_file:
                the_file.write(traceback.format_exc())
                the_file.write("Insertion in SQS failed for skus %s and source %s" % (purge_doc['sku_ids'], purge_doc['source']))