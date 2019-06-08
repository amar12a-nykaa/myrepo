import sys 

import socket 
from IPython  import embed
from pymongo import MongoClient

hostname = socket.gethostname()
if hostname.startswith('admin') or hostname.startswith('api'):
  MONGO_CLIENT = "172.30.3.5,172.30.2.45,172.30.2.154"
else:
  MONGO_CLIENT = "localhost"

class MongoUtils:

  @classmethod
  def getClient(cls):
    return MongoClient(MONGO_CLIENT)

if __name__ == '__main__':
  c = MongoUtils.getClient()
  print(c.server_info())

