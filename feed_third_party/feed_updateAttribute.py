
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

sys.path.append('/home/apis/pds_api/')

from pas.v2.utils import Utils, MemcacheUtils

client = MongoClient("mongofeed.nyk00-int.network")
data = client['local']['feed3']



def updateAtrribute_feed3():
	productids=[]
	pids=[]
	idsvalue = {}
	result1 = []
	nykaa_mysql_conn = Utils.nykaaMysqlConnection()
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

	# pprint(result)
	# exit()
	for doc in result:
		productids.append(str(doc['product_id']))
		pids.append(doc['product_id'])
	#pprint(productids)
	sqlquery1 = "SELECT e.entity_id as product_id, REPLACE(REPLACE(REPLACE(TRIM(cpetd.value), '{{media url=\"','http://www.nykaa.com'), '\"}}', ''), '\n', '<br>') AS description ,  REPLACE(REPLACE(REPLACE(TRIM(cpetpu.value), '{{media url=\"','http://www.nykaa.com'), '\"}}', ''), '\n', '<br>') AS product_use ,  REPLACE(REPLACE(REPLACE(TRIM(cpetpi.value), '{{media url=\"','http://www.nykaa.com'), '\"}}', ''), '\n', '<br>') AS product_ingredients ,  cpeie.value AS eretailer ,  cpeitry.value AS try_it_on ,  cpeitrytype.value AS try_it_on_type ,   (CASE WHEN cpeiredirect.value=1 THEN 1 ELSE 0 END)  AS redirect_to_parent ,  cpeifree.value AS is_a_free_sample ,  cpevvid.value AS video ,  cpevstlc.value AS shop_the_look_category_tags ,  cpevsq.value AS shipping_quote ,  cpevpe.value AS product_expiry ,  cpevbdp.value AS bucket_discount_percent ,  TRIM(cpevcos.value) AS copy_of_sku ,  cpeisd.value AS style_divas FROM catalog_product_entity e LEFT JOIN catalog_product_entity_text cpetd ON cpetd.entity_id = e.entity_id AND cpetd.attribute_id = 57 AND cpetd.store_id = 0 LEFT JOIN catalog_product_entity_text cpetpu ON cpetpu.entity_id = e.entity_id AND cpetpu.attribute_id = 569 AND cpetpu.store_id = 0 LEFT JOIN catalog_product_entity_text cpetpi ON cpetpi.entity_id = e.entity_id AND cpetpi.attribute_id = 571 AND cpetpi.store_id = 0 LEFT JOIN catalog_product_entity_int cpeie ON cpeie.entity_id = e.entity_id AND cpeie.attribute_id = 735 AND cpeie.store_id = 0 LEFT JOIN catalog_product_entity_int cpeitry ON cpeitry.entity_id = e.entity_id AND cpeitry.attribute_id = 759 AND cpeitry.store_id = 0 LEFT JOIN catalog_product_entity_int cpeitrytype ON cpeitrytype.entity_id = e.entity_id AND cpeitrytype.attribute_id = 760 AND cpeitrytype.store_id = 0 LEFT JOIN catalog_product_entity_int cpeiredirect ON cpeiredirect.entity_id = e.entity_id AND cpeiredirect.attribute_id = 765 AND cpeiredirect.store_id = 0 LEFT JOIN catalog_product_entity_int cpeifree ON cpeifree.entity_id = e.entity_id AND cpeifree.attribute_id = 783 AND cpeifree.store_id = 0 LEFT JOIN catalog_product_entity_varchar cpevvid ON cpevvid.entity_id = e.entity_id AND cpevvid.attribute_id = 603 AND cpevvid.store_id = 0 LEFT JOIN catalog_product_entity_varchar cpevstlc ON cpevstlc.entity_id = e.entity_id AND cpevstlc.attribute_id = 716 AND cpevstlc.store_id = 0 LEFT JOIN catalog_product_entity_varchar cpevsq ON cpevsq.entity_id = e.entity_id AND cpevsq.attribute_id = 738 AND cpevsq.store_id = 0 LEFT JOIN catalog_product_entity_varchar cpevpe ON cpevpe.entity_id = e.entity_id AND cpevpe.attribute_id = 792 AND cpevpe.store_id = 0 LEFT JOIN catalog_product_entity_varchar cpevbdp ON cpevbdp.entity_id = e.entity_id AND cpevbdp.attribute_id = 791 AND cpevbdp.store_id = 0 LEFT JOIN catalog_product_entity_varchar cpevcos ON cpevcos.entity_id = e.entity_id AND cpevcos.attribute_id = 788 AND cpevcos.store_id = 0 LEFT JOIN catalog_product_entity_int cpeisd ON cpeisd.entity_id = e.entity_id AND cpeisd.attribute_id = 729 AND cpeisd.store_id = 0"
	sqlquery3 = " where e.entity_id IN (" + ','.join(productids) +")";

	sqlquery= sqlquery1 + sqlquery3
	#print(sqlquery)

	cursor = nykaa_mysql_conn.cursor()
	cursor.execute(sqlquery)
	columns = cursor.description
	for value in cursor.fetchall():
		tmp = {}
		coloumnvalue = {}
		coloumnpids = {}
		for (index,values) in enumerate(value):
			tmp[columns[index][0]] = values

			if (columns[index][0].find("_v1") > -1 and type(values) != type(None)):
				coloumnvalue[columns[index][0]]=values
			if columns[index][0]=="product_id":
				coloumnvalue['product_id']=values
				productid=values
			coloumnvalue[columns[index][0]]=values
		coloumnpids=coloumnvalue
		result1.append(coloumnpids)


	mongoresult = list(data.find({'product_id':{'$in': pids}}))
	mongores={}
	mongodata={}
	for value in mongoresult:
		mongodata[value['product_id']]=value
		
	i=0
	for fields in result1:
		
		final_value={}
		mongopid=fields["product_id"]
		if mongopid == mongodata[mongopid]["product_id"] :
			for fields_key in fields.keys():
				if(fields_key=='product_id' and fields_key == 'created_at'):	
					continue	
				else:
					final_value[fields_key]=fields[fields_key]
						
			#pprint("-----------------")
			#pprint(final_value)
			data.update({"product_id":fields["product_id"]},{"$set":final_value})
		i += 1


updateattribute=updateAtrribute_feed3()
