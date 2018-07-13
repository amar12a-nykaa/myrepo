from pymongo import MongoClient
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
import traceback
import subprocess
import urllib.request
import os.path
import csv


sys.path.append("/nykaa/scripts/sharedutils")

sys.path.append('/home/apis/nykaa/')

from pas.v2.utils import Utils, MemcacheUtils

client = MongoClient("localhost")
data = client['local']['feeddata']


def create_feed_index():
	totaldata = list(data.find({},{"_id":0}))
	num = len(totaldata)
	path='/var/www/html/nykaalive/media/feed/'
	
	exclude_file = path +"fb_exclude.csv"
	excludeFlag=0
	mylist = []
	if os.path.exists(exclude_file):
		excludeFlag=1
		f = open(exclude_file,'r')
		file_data = f.read() #string data
		f.close()

	else:
		print("file not found "+ exclude_file)
	
	adultFlag=0
	exclude_lingeriefile = path + "pids_lingerie.csv"
	if os.path.exists(exclude_lingeriefile):
		adultFlag=1
		fadult = open(exclude_lingeriefile,'r')
		file_dataadult = fadult.read() #string data
		# adult = []
		# adult.append(file_dataadult.splitlines())
		

	else:
		print("file not found "+ exclude_lingeriefile)

	topSkusFile=path + "top_1000_skus.csv"
	if os.path.exists(topSkusFile):
		topSkus = open(topSkusFile,'r')
		file_datatopSkus = topSkus.read() #string data
		
	else:
		print("file not found "+ topSkusFile)
	#print(file_data)
	finalcsv=[]
	for totaldatas in totaldata:
		ids=str(int(totaldatas['id']))
		# break
		if excludeFlag == 1:
			if ids in file_data:
				continue
		if adultFlag == 1:
			if ids in file_dataadult or totaldatas['age_group']==1 or totaldatas['age_group']=='true':
				totaldatas['age_group'] = 'adult'
			#print(type(totaldatas['age_group']))
			if  totaldatas['age_group']=="0" or totaldatas['age_group']=='false':
				totaldatas['age_group'] = ''

		if num < 24:
			continue
		if type(totaldatas['link']) != type(None):
			totaldatas['link'] = totaldatas['link']+"?ptype=product&id="+ids
		totaldatas['android_package'] = "com.fsn.nykaa"
		totaldatas['android_app_name'] = "Nykaa"

		totaldatas['ios_app_name']="Nykaa - Beauty Shopping App"
		totaldatas['ios_app_store_id']="1022363908"

		if ids in file_datatopSkus:
			totaldatas['custom_label_0']=1
		else:
			totaldatas['custom_label_0']=0

		if(totaldatas['availability']!="out of stock" or totaldatas['availability']!="in stock"):
			totaldatas['availability']="out of stock"


		if totaldatas['price']=="0":
			continue
		else:
			totaldatas['price']=str(totaldatas['price']) + ' INR'
			if type(totaldatas['sale_price']) != type(None):
				totaldatas['sale_price']=str(totaldatas['sale_price']) + ' INR'
		totaldatas['condition'] = 'new'
		finalcsv.append(totaldatas)
	
	myFile = open(path +'fb.csv', 'w')
	head =[]
	head = (['id ','mpn ','brand ','link ','image_link ','price ','sale_price ','age_group ','gtin ','title ','custom_label_0 ','custom_label_1 ','custom_label_2 ','custom_label_3 ','custom_label_4 ','promotion_id ','availability ','description ','product_type ','google_product_category ','android_url ','android_package ','android_app_name ','ios_url ','ios_app_store_id ','ios_app_name ','color ','condition'])
	writer = csv.writer(myFile)
	writer.writerow(head)
	
	for i in range(len(finalcsv)):
		
		ids=int(finalcsv[i]['id'])
		finaldata1 =[]
		finaldata =([ids,finalcsv[i]['mpn'],finalcsv[i]['brand'],finalcsv[i]['link'],finalcsv[i]['image_link'],finalcsv[i]['price'],finalcsv[i]['sale_price'],finalcsv[i]['age_group'],finalcsv[i]['gtin'],finalcsv[i]['title'],finalcsv[i]['custom_label_0'],finalcsv[i]['custom_label_1'],finalcsv[i]['custom_label_2'],finalcsv[i]['custom_label_3'],finalcsv[i]['custom_label_4'],finalcsv[i]['promotion_id'],finalcsv[i]['availability'],'',finalcsv[i]['product_type'],finalcsv[i]['google_product_category'],finalcsv[i]['android_url'],finalcsv[i]['android_package'],finalcsv[i]['android_app_name'],finalcsv[i]['ios_url'],finalcsv[i]['ios_app_store_id'],finalcsv[i]['ios_app_name'],finalcsv[i]['color'],finalcsv[i]['condition']])
		writer.writerow(finaldata)
		

product_id_index = create_feed_index()
