
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

from pas.v2.utils import Utils, MemcacheUtils

client = MongoClient("mongofeed.nyk00-int.network")
data = client['local']['feed3']



def updateFields_feed3():
	productids=[]
	pids=[]
	offerids=[]
	parent_ids=[]
	ppids={}
	nykaa_mysql_conn = Utils.nykaaMysqlConnection()
	cursor = nykaa_mysql_conn.cursor()
	start = datetime.datetime.now()
	end = (datetime.datetime.now()-datetime.timedelta(hours=24))

	result = data.aggregate([
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


	mongodata={}
	for doc in result:

		productids.append(str(doc['product_id']))
		
		if(doc['offer_id'] != None):
	#		pprint(doc['offer_id'])
			# fields_oofer = str(doc['offer_id']).split(",")
			# for fields_offer in fields_oofer:
			offerids.append(str(doc['offer_id']))
		pids.append(doc['product_id'])
		mongodata[doc['product_id']]=doc
		if(doc['redirect_to_parent'] ==1):
			ppids[doc['product_id']]=doc['parent_id']
			parent_ids.append(ppids)

	# pprint(offerids)
	# exit()
	sqlquery ="SELECT COUNT(DISTINCT entity_id) AS qna_count, product_id  FROM aw_pquestion2_question WHERE STATUS = 2 and product_id IN (" + ','.join(productids) +") GROUP BY product_id"

	cursor.execute(sqlquery)
	columns = cursor.description
	result1 = []
	for value in cursor.fetchall():
		tmp = {}
		for (index,values) in enumerate(value):
			if(values != ""):
				tmp[columns[index][0]] = values
		result1.append(tmp)

	mongoresult = list(data.find({'product_id':{'$in': pids}}))
	
	#offer="','".join(list(set(offerids)))
	
	sqloffer="SELECT entity_id as id ,name, offerfiltername, description FROM nykaa_offers WHERE enabled=1 and app = 1 and entity_id IN ('" + "','".join(offerids).strip("',") +"') ORDER BY offer_priority DESC , updated_at DESC"
	# print(sqloffer)
	# exit()
	cursor.execute(sqloffer)
	columnsoffer = cursor.description
	result2 = {}
	tmpEnabledOfferId = []
	for valueoffer in cursor.fetchall():
		tmpoffer = {}
		tmpofferfinal = {}
		for (index,values) in enumerate(valueoffer):
			tmpoffer[columnsoffer[index][0]] = values
			result2[tmpoffer['id']] = tmpoffer
			tmpEnabledOfferId.append(tmpoffer['id'])
	enabledOfferId = list(set(tmpEnabledOfferId))
		


	query_bundleurl = "SELECT GROUP_CONCAT(option_id ) AS option_ids, parent_product_id, GROUP_CONCAT(selection_id) AS selection_ids, GROUP_CONCAT(selection_qty ) AS qtys FROM catalog_product_bundle_selection  WHERE parent_product_id  IN (" + ','.join(productids) +") GROUP BY parent_product_id ";
	cursor.execute(query_bundleurl)
	columnsbundleurl = cursor.description
	tmpbundleurl1=[]
	relationcoloumns=["option_ids","selection_ids","qtys","parent_product_id"]
	for valuebundleurl in cursor.fetchall():
		tmp = {}
		for (index,values) in enumerate(valuebundleurl):
			tmp[columnsbundleurl[index][0]]=values
		tmpbundleurl1.append(tmp)


	#pprint(tmpbundleurl1)
	# exit()
	mongores=[]
	final_value={}
	final_valueoffer={}
	final_valueqna={}



	for value in mongoresult:
		valuemongo=[]
		valuemongoqna=[]

		purl={}
		ppurl=[]
		#for redirect to parent url
		if(value['redirect_to_parent']==1):
			if type(value['parent_id']) != type(None):
				if "," not in value['parent_id']:
					if value['parent_id'] == mongodata[int(value['product_id'])]:
						purl = mongodata[int(value['parent_id'])]['product_url']
						ppurl.append(purl)
		else:		
			purl =value['product_url']
			ppurl.append(purl)
		#pprint(ppurl)
		#exit()
		#for offers
		fields_array = str(value['offer_id']).split(",")
		for fields_value in fields_array:
			if(fields_value != 'None' and fields_value != ''):
				fields_value = int(fields_value)
				if fields_value in enabledOfferId:
					valueoffer = result2[fields_value]
					valuemongo.append(valueoffer)
		#for qna
		for valueqna in result1:
			valuecount= {}
			if(valueqna['product_id'] == value['product_id']):
				valuecount[valueqna['product_id']] = str(valueqna['qna_count'])
				valuemongoqna.append(str(valueqna['qna_count']))
				#pprint(valueqna['qna_count'])
		#for bundle url
		bundleOptionsvalue=[]
		for url1 in tmpbundleurl1:
			bundleOptions=""
			
			# pprint(url1['parent_product_id'])
			#pprint(value)
			if(url1['parent_product_id'] == int(value['product_id'])):
				#pprint(url1['parent_product_id'])
				option_ids = str(url1['option_ids']).split(",")
				pids=str(url1['selection_ids']).split(",")
				qtys=str(url1['qtys']).split(",")
				for key,value1 in enumerate(pids):
					bundleOptions += '&bundle_option[' + option_ids[key] + ']=' + value1 +'&bundle_option_qty[' + option_ids[key] + ']=' + qtys[key];
				bundleurl = "http://www.nykaa.com/ajaxaddcart/index/add/uenc/,/product/"+str(url1['parent_product_id'])+"/isAjax/1/?"+bundleOptions
				bundleOptionsvalue.append(bundleurl)
				del bundleOptions
			else:
				bundleOptionsvalue.append(value['add_to_cart_url'])

		

		final_value["offer_id"] = valuemongo
		final_value['qna_count'] = valuemongoqna
		final_value['add_to_cart_url']=bundleOptionsvalue
		final_value['product_url']=ppurl
		#final_value['updated_at']=datetime.now()
		# pprint(value["product_id"])
		pprint(final_value)
		# exit()
		data.update({"product_id":value["product_id"]},{"$set":final_value})

updatefields=updateFields_feed3()
