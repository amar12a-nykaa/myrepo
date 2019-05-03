import time
from sqs_launcher import SqsLauncher

#launcher = SqsLauncher('myqueue.fifo')
#response = launcher.launch_message({'param1': 'hello', 'param2': 'world'})

import boto3

# Create SQS client
sqs = boto3.client('sqs')

queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/911609873560/gludo_api.fifo'
queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/579953726837/myqueue.fifo'
queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/911609873560/gludo_api.fifo'

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
        'Information about current NY Times fiction bestseller for '
        'week of 12/11/2016.'
    ),
    MessageGroupId= "1",
    MessageDeduplicationId = str(time.time())
)

