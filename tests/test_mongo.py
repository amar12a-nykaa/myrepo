import sys

def test_mongo_connection():
  sys.path.append("/nykaa/scripts/sharedutils")
  from mongoutils import MongoUtils
  client = MongoUtils.getClient()
  print(client.server_info())


