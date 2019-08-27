import requests
import json
import collections
import pytz
import datetime
import sys

sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils

url = 'http://nyk-aggregator-api.nykaa.com/api/getRedisDump'

def get_redis_data_dump():
    global url
    response = requests.get(url,timeout=5)
    if (response.ok):
        responseObject = json.loads(response.content, 'utf-8')
        return responseObject
    else:
        response.raise_for_status()
        return None


def upload_redis_dump_to_s3(response):
  tz = pytz.timezone('Asia/Kolkata')
  local_date = datetime.datetime.now(tz=tz).strftime('%d-%m-%Y')
  mapping = response.get('response')
  file_name = 'redis_dump_{}.json'.format(local_date)

  with open(file_name, 'w') as f:
      json.dump(mapping, f)

  f.close()
  PasUtils.upload_file_to_s3(file_name)
  os.remove(file_name)

if __name__ == '__main__':
    response = get_redis_data_dump()
    upload_redis_dump_to_s3(response)

