
from pprint import pprint
import json
import string
import datetime
import socket
import sys
import time
import timeit
import requests
import argparse
import traceback
import subprocess
import os.path
import csv
import math
from urllib.request import urlopen

def incremental_feed():
	print("Start Time ==="+datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
	sys.path.append("/nykaa/scripts/sharedutils")
	sys.path.append('/var/www/html/nykaa_apis/')
	from pas.v2.utils import Utils, MemcacheUtils
	client 	 = MongoClient("localhost")
	data 	 = client['local']['feeddata']
	crondata = client['local']['crondata']
	cron_val_data = list(crondata.find({}))
	
	current_date_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
	psku 	  = []
	totaldata = []
	inventory_sku_dump  = {}

	if len(cron_val_data)==0:
		minus_hour_date_time = datetime.datetime.now() - datetime.timedelta(minutes = 60)
		minus_hour_date_time = minus_hour_date_time.strftime('%Y-%m-%dT%H:%M:%S')
		minus_hour_date_time = minus_hour_date_time.replace(" ", "")
	else:
		minus_hour_date_time = cron_val_data[0]['last_update_date']

	current_date_time = current_date_time.replace(" ", "")
	
	nykaa_mysql_conn = Utils.nykaaMysqlConnection()
	cursor = nykaa_mysql_conn.cursor()
	sql_query = "select plain_value from core_variable_value where variable_id in (select variable_id from core_variable where code='incremental_price_ip')"	
	cursor.execute(sql_query)
	values = cursor.fetchone()
	api_url = list(values)
	
	url = api_url[0]+"?where=last_price_updated >'"+minus_hour_date_time+"' and last_price_updated <'"+current_date_time+"'and type='simple'&select=count(*) as price_count"
	# url = "http://preprod-api.nyk00-int.network/apis/v2/getPASbyQuery?where=last_price_updated > '2018-05-03T04:00:0' and last_price_updated < '2018-05-03T07:21:30' and type='simple'&select=count(*) as price_count,psku,sku,mrp,sp,is_in_stock"
	r = requests.post(url)
	data_res  = json.loads(r.text)
	price_range_value = math.ceil(data_res['products'][0]['price_count']/1000)
	price_j = 0
	price_i = 1001
	
	for number in range(1,int(price_range_value)+1):
		price_k = 1000
		price_url 		= api_url[0]+"?where=last_price_updated >'"+minus_hour_date_time+"' and last_price_updated <'"+current_date_time+"'and type='simple'&&select=psku,sku,mrp,sp,is_in_stock&offset="+str(price_j)+"&limit="+str(price_k)
		# print(price_url)
		price_api       = requests.post(price_url)
		price_api_data  = json.loads(price_api.text)
		price_j = price_i+1000
		price_i = price_j
		if len(price_api_data['products'])>0:
			bulk 	= data.initialize_unordered_bulk_op()
			counter = 0;
			for x in price_api_data['products']:
				psku.append(x['sku'])
				bulk.find({ 'mpn': x['sku'] }).update({ '$set': { 'price': x['mrp'],'sale_price':x['sp'],'updated_at':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}})
				counter += 1
				if(counter % 1000 == 0):
					bulk.execute()
					bulk = data.initialize_unordered_bulk_op()
			if(counter % 1000 != 0):
				bulk.execute()
	
	inventory_url = api_url[0]+"?where=update_time >'"+minus_hour_date_time+"' and update_time <'"+current_date_time+"'&select=count(*) as count_value"
	
	# inventory_url ="http://preprod-api.nyk00-int.network/apis/v2/getPASbyQuery?where=update_time>'2018-05-03T12:19:27' and update_time < '2018-05-03T20:19:27' &select=count(*) as count_value"
	# print(inventory_url)
	
	invrt  = requests.post(inventory_url)
	inventory_data  = json.loads(invrt.text)
	
	# print(inventory_data['products'][0]['count_value'])
	inventory_range = math.ceil(inventory_data['products'][0]['count_value']/1000)
	
	# print(inventory_range)
	j = 0
	i = 1001

	for number in range(1,int(inventory_range)+1):
		k = 1000
		inventory_api = api_url[0]+"?where=update_time >'"+minus_hour_date_time+"' and update_time <'"+current_date_time+"'&select=sku,is_in_stock,disabled&offset="+str(j)+"&limit="+str(k)
		# inventory_api = "http://preprod-api.nyk00-int.network/apis/v2/getPASbyQuery?where=update_time>'2018-05-03T12:19:27' and update_time < '2018-05-03T20:19:27'&select=sku,is_in_stock,disabled&offset="+str(j)+"&limit="+str(k)

		# print(inventory_api)
		invrt_api  	  = requests.post(inventory_api)
		inventory_api_data  = json.loads(invrt_api.text)
		for inv_data in inventory_api_data['products']:
			inventory_sku_dump[inv_data['sku']] = inv_data
			if len(inventory_api_data['products'])<10:
				break
		j = i+1000
		i = j

	# print(inventory_sku_dump)
	mongoCollection = list(data.find({}))
	# print(len(mongoCollection))
	inventory_bulk 	= data.initialize_unordered_bulk_op()
	inventory_counter = 0;
	
	for mongo_coll in mongoCollection:
		if mongo_coll['mpn'] in inventory_sku_dump.keys():
			# print(inventory_sku_dump[mongo_coll['mpn']])
			if inventory_sku_dump[mongo_coll['mpn']]['is_in_stock']:
				check_stock_status = 'in stock'
			else:
				check_stock_status = 'out of stock'
			if mongo_coll['availability'] !=check_stock_status:
				inventory_bulk.find({ 'mpn': mongo_coll['mpn'] }).update({'$set':{'availability':check_stock_status,'updated_at':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}})
				inventory_counter += 1
				if(inventory_counter % 1000 == 0):
					inventory_bulk.execute()
					inventory_bulk = data.initialize_unordered_bulk_op()

	if(inventory_counter % 1000 != 0):
		inventory_bulk.execute()
	insert_cron = {'status':'success','last_update_date':datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}
	# print(insert_cron)
	
	if len(cron_val_data)==0:
		crondata.insert_one(insert_cron)
	else:
		crondata.update_one({'_id':cron_val_data[0]['_id']},{'$set':{'last_update_date':datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}})
	print("End Time ==="+datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
incremental_feed_run = incremental_feed()
