import json
import os
import time
from sqs_launcher import SqsLauncher
from IPython import embed

#launcher = SqsLauncher('myqueue.fifo')
#response = launcher.launch_message({'param1': 'hello', 'param2': 'world'})

import boto3

# Create SQS client
sqs = boto3.client('sqs')

queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/911609873560/gludo_api.fifo'
queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/579953726837/myqueue.fifo'
queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/911609873560/gludo_api.fifo'
queue_url = "https://sqs.ap-southeast-1.amazonaws.com/911609873560/preprod-discovery-api-sqs.fifo"

chunks = []
chunk = []
products = [] 
with open(  os.path.join(os.path.dirname(__file__), 'sqs_10k.csv')) as f:
    lines = f.readlines()
products = [json.loads(line.strip()) for line in lines]
    
def chunkify(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

chunks = chunkify(products, 100)

for chunk in chunks:
# Send message to SQS queue
    response = sqs.send_message(
	QueueUrl=queue_url,
	DelaySeconds=0,
	MessageAttributes={
	    'Title': {
		'DataType': 'String',
		'StringValue': 'The Whistler'
	    },
	    'Author': {
		'DataType': 'String',
		'StringValue': 'John Grisham'
	    },
	    'WeeksOn': {
		'DataType': 'Number',
		'StringValue': '6'
	    }
	},
	MessageBody=(
		json.dumps(chunk)
	),
	MessageGroupId= "1",
	MessageDeduplicationId = str(time.time())
    )


