import requests
import json
import collections
import pytz
import arrow
import datetime
import sys
import boto3
import os

url = 'http://nyk-aggregator-api.nykaa.com/api/getRedisDump'

def get_redis_data_dump():
    global url
    response = requests.get(url,timeout=5)
    if (response.ok):
        responseObject = json.loads(str(response.content, 'utf-8'))
        return responseObject
    else:
        response.raise_for_status()
        return None


def upload_redis_dump_to_s3(response):
  mapping = response.get('response')

  file_path = '/nykaa/adminftp/redis_dump.json'
  with open(file_path, 'w') as f:
      json.dump(mapping, f)
  f.close()
  date = arrow.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  s3_file_location = 'dt=%s/redis_dump.json' % date.strftime("%Y%m%d")
  pipeline = boto3.session.Session(profile_name='datapipeline')
  s3 = pipeline.client('s3')
  bucket_name = 'nykaa-prod-autocomplete-feedback'
  s3.upload_file(file_path, bucket_name, s3_file_location)

  os.remove(file_path)

if __name__ == '__main__':
    response = get_redis_data_dump()
    upload_redis_dump_to_s3(response)

