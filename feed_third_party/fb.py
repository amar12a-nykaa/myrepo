
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

sys.path.append("/nykaa/scripts/sharedutils")

sys.path.append('/home/apis/pds_api/')

from pas.v2.utils import Utils, MemcacheUtils

client = MongoClient("mongofeed.nyk00-int.network")
data = client['local']['feeddata']


def create_feed_index():
	j=0
	i=1001
#  j=0
#  j=0
	nykaa_mysql_conn = Utils.nykaaMysqlConnection()
	cursor = nykaa_mysql_conn.cursor()
	for number in range(1,200):
		k=1000

		sqlquery1="select sd4.product_id as id,sd3.sku as mpn, sd4.brands as brand, sd3.product_url as link, CONCAT(sd4.main_image,'?tr=h-600,w-600,q-100') as image_link, sd4.price as price, sd4.special_price as sale_price, (select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 710) as age_group, (select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 796) as gtin, sd3.name as title, (select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 778 limit 1) as custom_label_0, (select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 779 limit 1) as custom_label_1, (select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 780 limit 1) as custom_label_2, (select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 781 limit 1) as custom_label_3,(select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 782 limit 1) as custom_label_4, (select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 711 limit 1) as promotion_id, (case when sd3.is_in_stock = '1' then 'in stock' else 'out of stock' end) as availability, (select COALESCE((select cpet.value from catalog_product_entity_text as cpet where cpet.entity_id = sd3.product_id and cpet.attribute_id = 776 limit 1),(select cpet.value from catalog_product_entity_text as cpet where cpet.entity_id = sd3.product_id and cpet.attribute_id = 57 limit 1)) ) as description,(select cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 775 limit 1) as product_type, (select  cpev.value from catalog_product_entity_varchar as cpev where sd3.product_id = cpev.entity_id and cpev.attribute_id = 785 limit 1) as google_product_category, concat('com.fsn.nykaa-android://nykaa/product?product_id=', sd3.product_id, '&is_native=1') as android_url, NULL as android_package, NULL as android_app_name, concat('Nykaa://product?product_id=', sd3.product_id) as ios_url, NULL as ios_app_store_id, NULL as ios_app_name, (select eaov.value from eav_attribute_option_value as eaov where eaov.option_id in (select value from catalog_product_entity_varchar as cpevc where cpevc.attribute_id = 658 and cpevc.entity_id = sd3.product_id) limit 1) as color from solr_dump_3 as sd3 join solr_dump_4 as sd4 ON sd3.product_id = sd4.product_id where sd3.product_id in (select cpei.entity_id from catalog_product_entity_int as cpei where cpei.attribute_id = 718 and cpei.value <> 1) and sd3.product_id in (select  cpei.entity_id from catalog_product_entity_int as cpei where attribute_id = 713 and value <> 1) and sd3.product_id in (select cpei.entity_id from catalog_product_entity_int as cpei where attribute_id = 80 and value = 1) and sd4.price > 0 limit "+ str(j) +','+str(k)
		print(str(j))
		print(str(k))
		sqlquery= sqlquery1
	
		cursor.execute(sqlquery)
		columns = cursor.description
		result = []
		result1 = []
			
		productidappend=[]
		j=i+1000
		a=0    
		for value in cursor.fetchall():
			a = a+1
			tmp = {}
			for (index,values) in enumerate(value):
				tmp[columns[index][0]] = values
				if(type(values) != type(None)):
					if(columns[index][0]=="price" or columns[index][0]=="sale_price"):
						tmp[columns[index][0]] = float(values)
			result.append(tmp)
		#pprint(result)
		if(len(result)>0):
			resultquery1=data.insert_many(result)
		else:
			break
		i=j
	
product_id_index = create_feed_index()
