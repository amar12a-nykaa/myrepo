from pymongo import MongoClient
from pprint import pprint
import json
import string
import time
import datetime
import socket
import sys
import time
import timeit
import requests
import argparse
import traceback
import subprocess
import urllib.request
from datetime import date, timedelta


sys.path.append("/nykaa/scripts/sharedutils")

from loopcounter import LoopCounter

sys.path.append('/home/apis/nykaa/')

from pas.v1.utils import Utils, MemcacheUtils

client = MongoClient()
data = client['local']['feed3']

# Open database connection



def updatewislist_feed4():
	coredata = []
	preorder_categories=[]
	preorder_text1=[]
	wishlist=[]
	preorder=[]
	nykaa_mysql_conn = Utils.nykaaMysqlConnection()
	cursor = nykaa_mysql_conn.cursor()
	sqlquery ="SELECT path,value FROM core_config_data WHERE path IN ('catalog/frontend/nyx_catid','preorder/active/all_ids','preorder/active/button_text','catalog/frontend/nyx_catid_enable','preorder/active/enabled')"

	cursor.execute(sqlquery)
	columns = cursor.description
	for corevalues in cursor.fetchall():
		#pprint(corevalues)
		if(corevalues[0]=='catalog/frontend/nyx_catid'):
			wishlist_categories=corevalues[1].split(",")

		if(corevalues[0]=='preorder/active/all_ids'):
			preorder_categories=corevalues[1].split(",")

		if(corevalues[0]=='preorder/active/button_text'):
			preorder_text1.append(corevalues[1])

		if(corevalues[0] == "catalog/frontend/nyx_catid_enable" and corevalues[1]== "0"):
			wishlist.append("0")

		if (corevalues[0] == "preorder/active/enabled" and corevalues[1] == "1"):
			preorder.append("1")


		
	#Update show wishlist and button text fields
	start = datetime.datetime.now()
	end = (datetime.datetime.now()-datetime.timedelta(hours=24))

	mongoresult = data.aggregate([
        {
            "$match":
                {
                    "created_at":
                        {
                            "$lte": start,
                            "$gte": end
                        }
                }
        }
    ]) 
	final_value={}
	for doc in mongoresult:
		button_text=""
		show_wishlist_button = 0;
		show_button_text = 0;
		button_text = doc['button_text']


		if(wishlist[0] == '0'):
			if(any((True for x in doc['category_id'] if x in wishlist_categories)) == True):
				show_wishlist_button = 1
			final_value['show_wishlist_button']=show_wishlist_button	
		if(preorder[0] == '1'):
			if(any((True for x in doc['category_id'] if x in preorder_categories)) == True):
				show_button_text=1
				button_text=preorder_text1[0]
			final_value['button_text']=button_text
		#pprint(final_value)
		#final_value['updated_at']=datetime.now()
		data.update({"product_id":doc["product_id"]},{"$set":final_value})

updatewislist = updatewislist_feed4()