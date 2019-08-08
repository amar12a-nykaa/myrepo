import sys 

import socket 
from IPython  import embed
from pymongo import MongoClient

hostname = socket.gethostname()
if hostname.startswith('admin') or hostname.startswith('api'):
  MONGO_CLIENT = "priceapi-mongo1.nykaa-internal.com,priceapi-mongo2.nykaa-internal.com,priceapi-mongo3.nykaa-internal.com"
else:
  MONGO_CLIENT = "localhost"

class MongoUtils:

  @classmethod
  def getClient(cls):
    return MongoClient(MONGO_CLIENT)

if __name__ == '__main__':
  c = MongoUtils.getClient()
  print(c.server_info())

