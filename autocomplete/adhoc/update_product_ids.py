#!/usr/bin/python
import argparse
import json
import pprint
import socket
import sys
import traceback
from collections import OrderedDict
from IPython import embed
from datetime import datetime
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import dateparser
import requests


sys.path.append("/nykaa/api")
from pas.v2.csvutils import read_csv_from_file
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter
from mongoutils import MongoUtils



client = MongoUtils.getClient()
master_feed= client['feed_pipeline']['master_feed']

conn =  Utils.mysqlConnection()
conn_nykaa =  Utils.nykaaMysqlConnection()
#embed()

ctr = LoopCounter(name='Updating product_id and parent_id', total=master_feed.count())
for doc in master_feed.find({}, {"product_id": 1, "parent_id": 1, "sku": 1}).limit(0):
	try:
		ctr += 1
		if ctr.should_print():
			print(ctr.summary)

		set_clause_arr = []
		#if doc.get('parent_id'):
		#	set_clause_arr.append("parent_id='%s'" % doc.get('parent_id'))
		if doc.get('product_id'):
			set_clause_arr.append("product_id='%s'" % doc.get('product_id'))

		if set_clause_arr:
			set_clause = " set " + ", ".join(set_clause_arr)
			query = "update products {set_clause} where sku ='{sku}' ".format(set_clause=set_clause, sku=doc['sku'])
			#print(query)
			Utils.mysql_write(query, connection=conn)
	except:
		print(traceback.format_exc())

