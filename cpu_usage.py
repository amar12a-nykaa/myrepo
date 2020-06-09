import boto3
import sys
from datetime import datetime, timedelta

client = boto3.client('cloudwatch',region_name='ap-south-1')
response = client.get_metric_statistics(
    Namespace='AWS/EC2',
    MetricName='CPUUtilization',
    Dimensions=[
        {
            'Name': 'InstanceId',
            'Value': 'i-0dba274cff9fbbe05'
        },
    ],
    StartTime=datetime.now() - timedelta(seconds=300),
    EndTime=datetime.now(),
    Period=86400,
    Statistics=[
        'Average',
    ],
    Unit='Percent'
)
for cpu in response['Datapoints']:
  if 'Average' in cpu:
    print(cpu['Average'])
