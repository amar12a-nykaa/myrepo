from sqs_listener import SqsListener
from IPython import embed

#class MyListener(SqsListener):
#    def handle_message(self, body, attributes, messages_attributes):
#        run_my_function(body['param1'], body['param2'])

#listener = MyListener('my-message-queue', error_queue='my-error-queue', region_name='us-east-1')
#listener.listen()
import boto3

# Create SQS client
sqs = boto3.client('sqs')

queue_url = 'https://sqs.ap-southeast-1.amazonaws.com/579953726837/myqueue.fifo	'

# Receive message from SQS queue
response = sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=[
        'SentTimestamp'
    ],
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=10,
    WaitTimeSeconds=0
)

print(response)
message = response['Messages'][0]
receipt_handle = message['ReceiptHandle']

# Delete received message from queue
sqs.delete_message(
    QueueUrl=queue_url,
    ReceiptHandle=receipt_handle
)
print('Received and deleted message: %s' % message)
#embed()
