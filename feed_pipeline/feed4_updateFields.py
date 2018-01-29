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



def updateFields_feed4():
	productids=[]
	pids=[]
	offerids=[]
	parent_ids=[]
	ppids={}
	nykaa_mysql_conn = Utils.nykaaMysqlConnection()
	cursor = nykaa_mysql_conn.cursor()
	mongodata={}
	sqlquery ="SELECT entity_id FROM catalog_product_entity_varchar WHERE attribute_id = 605 AND value IS NOT NULL AND value NOT LIKE '0' AND value != ''"

	cursor.execute(sqlquery)
	columns = cursor.description
	freeProds = []
	for value in cursor.fetchall():
		tmp = {}
		for (index,values) in enumerate(value):
			if(values != ""):
				tmp[columns[index][0]] = values
		freeProds.append(tmp)
	#pprint(freeProds)

	query_cats_l4 = "SELECT main_table.entity_id FROM catalog_category_entity main_table JOIN catalog_category_entity_int join_table ON main_table.entity_id = join_table.entity_id AND join_table.attribute_id = 32 AND join_table.value = 1 WHERE main_table.level = 4";

	cursor.execute(query_cats_l4)

	l4_cats = []
	query_cats_l4columns = cursor.description

	for value in cursor.fetchall():
		if(value != ""):
			l4_cats.append(value[0])


	query_l5 = "SELECT title,identifier FROM cms_page WHERE is_active = 1";
	cursor.execute(query_l5)
	query_l5columns = cursor.description
	cms_titles=[]
	cms_urls=[]
	for value in cursor.fetchall():
		for (index,values) in enumerate(value):
			if(values != ""):
				if(query_l5columns[index][0]=='title'):
					cms_titles.append(values)
				if(query_l5columns[index][0]=='identifier'):
					cms_urls.append(values)
		#l4_cats.append(tmp)
	#pprint(cms_titles)
	#pprint(cms_urls)

	brandidssql="SELECT plain_value FROM core_variable_value cvv LEFT JOIN core_variable cv ON cv.variable_id =cvv.variable_id WHERE cv.code='brand-category-ids'"

	cursor.execute(brandidssql)
	brandids= cursor.fetchone()
	brand_parent_ids=brandids[0].split(",")
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
	for doc in result:
		final_value={}
		new_cat_ids_array = []
		new_cats_array = []
		brand_ids_array = []
		brands_array = []
		# pprint(doc)
		# exit()
		parent_category_ids = doc['parent_category_id'].split(",");
		category_ids = doc['category_id']
		categorys =doc['category']
		featured_in_urls = str(doc['featured_in_urls']).split(",")
		product_id = doc['product_id']
		l4_category = ''
		category_ids=list(filter(None,category_ids))

		try:
			l4_match=list(set(list(map(int, category_ids))).intersection(l4_cats))
			#pprint(len(l4_match))
			for key, value in enumerate(category_ids):
				if(len(l4_match)):
					if(l4_match[0]==int(value)):
						l4_category = categorys[key];
						#final_value['l4_category'] = l4_category
		except ValueError:
			l4_category=""
		#pprint(categorys)
			
		brand_ids_array=[]
		brands_array=[]
		new_cat_ids_array=[]
		new_cats_array=[]

		if(len(parent_category_ids) == len(category_ids)):
			for parent_key, parent_cat in enumerate(parent_category_ids):
				#pprint(len(parent_category_ids))
				
				# pprint(brand_parent_ids)
				if(parent_cat in  brand_parent_ids):
					brand_ids_array.append(category_ids[parent_key])
					brands_array.append(categorys[parent_key])
				new_cat_ids_array.append(category_ids[parent_key])
				try :
					new_cats_array.append(categorys[parent_key])	
				except IndexError:
					new_cats_array.append('')
			final_value['brand_ids'] = brand_ids_array
			final_value['brands'] = brands_array
			final_value['category_id'] = new_cat_ids_array
			final_value['category'] = new_cats_array
		#pprint(final_value)

		created_at = doc['created_at'];
		#pprint(created_at)
		result_tag = doc['tag'].replace(" ", "")
		result_tag=result_tag.lower()
		last15daate = (datetime.datetime.now()-datetime.timedelta(15));
		#pprint(last15daate)
		tag = '';
		if (result_tag == 'nykaa-choice' or result_tag == "nykaa'schoice"):
			tag = 'Nykaa Choice';
		elif (result_tag == 'exclusive'):
			tag = 'Exclusive';
		elif ((created_at >= last15daate or result_tag == 'new-at-nykaa' or result_tag == 'new')):
			tag = 'New';
		elif (product_id in freeProds or result_tag == 'special-offer' or result_tag == 'specialoffer'):
		    tag = 'Special offer';
		elif (result_tag == 'new-at-nykaa' or result_tag == 'new'):
		    tag = 'New';
		elif (result_tag == 'special-offer' or result_tag == 'specialoffer'):
		    tag = 'Special offer';
		elif (result_tag == 'offer'):
		    tag = 'Offer';
		final_value['tag'] = tag

		featured_urls = []
		featured_titles = []
		for featured_in_value in featured_in_urls:
			try:
				featured_keys = cms_urls.index(featured_in_value)
			except ValueError:
				featured_keys = 'None'
			if(featured_keys != 'None'):
				featured_urls.append("http://www.nykaa.com/"+featured_in_value)
				featured_titles.append(cms_titles[featured_keys])
		
		final_value['featured_in_urls'] = featured_urls
		final_value['featured_in_titles'] = featured_titles
		#final_value['updated_at']=datetime.now()
		#pprint(final_value)
		data.update({"product_id":doc["product_id"]},{"$set":final_value})

updatefields=updateFields_feed4()


