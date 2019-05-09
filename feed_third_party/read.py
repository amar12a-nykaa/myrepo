
from pprint import pprint
import json
import string
from datetime import datetime
import socket
import sys
import time
import timeit
import requests
import argparse
import urllib.request
from bson import json_util

sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils


sys.path.append('/home/apis/pds_api/')

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils
client = MongoUtils.getClient()
data = client['local']['feed3']


def create_feed_to_json():
  f = open('/data/nykaa/feed.json','w')
  json.dump(list(data.find({},{"_id":0,"created_at":0})), f,default=json_util.default)
  f.close()

create_feed_to_json = create_feed_to_json()
