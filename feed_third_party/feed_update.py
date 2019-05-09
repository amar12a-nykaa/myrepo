
from pprint import pprint
import json
import string
import socket
import sys
import time
import datetime
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

idsvalue = {}

result1 = []

def update_feed3():
	productids=[]
	pids=[]
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

	for doc in result:
		productids.append(str(doc['product_id']))
		pids.append(doc['product_id'])

	sqlquery1="SELECT e.entity_id AS product_id,e.created_at AS created_at,(SELECT GROUP_CONCAT(category_id) FROM catalog_category_product WHERE product_id = e.entity_id AND category_id != 1) AS category_id,(SELECT GROUP_CONCAT(TRIM(VALUE)) FROM catalog_category_entity_varchar WHERE entity_id IN (SELECT category_id FROM catalog_category_product WHERE product_id = e.entity_id AND category_id != 1) AND attribute_id = 31) AS category,NULL AS category_token,( SELECT GROUP_CONCAT(CONCAT('http://local.nykaa.com/media/catalog/product', main.value) SEPARATOR '|') FROM catalog_product_entity_media_gallery AS main LEFT JOIN catalog_product_entity_media_gallery_value AS value ON main.value_id = value.value_id AND value.store_id = 0 WHERE (main.attribute_id = '73') AND (main.entity_id = e.entity_id) AND (value.disabled = 0) ) AS image_url,cpedp.value AS price,cpedsp.value AS special_price,0 AS discount,(SELECT VALUE FROM eav_attribute_option_value AS eaov WHERE eaov.option_id = cpevtag.value AND eaov.store_id = 0) AS tag,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.c1), ',', -1) color_v1 FROM (SELECT  @c1 := @c1 +1 AS c1 FROM catalog_product_entity_varchar cpev, (SELECT @c1:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.c1-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=658) AND eaov.store_id = 0) AS color_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT  SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.co), ',', -1) concern_v1 FROM (SELECT  @co := @co +1 AS co FROM catalog_product_entity_varchar cpev, (SELECT @co:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev  ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.co-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=656) AND eaov.store_id = 0) AS concern_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.s1), ',', -1) spf_v1 FROM (SELECT  @s1 := @s1 +1 AS s1 FROM catalog_product_entity_varchar cpev, (SELECT @s1:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.s1-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=662) AND eaov.store_id = 0) AS spf_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.sk), ',', -1) skin_type_v1 FROM (SELECT  @sk := @sk +1 AS sk FROM catalog_product_entity_varchar cpev, (SELECT @sk:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.sk-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=657) AND eaov.store_id = 0) AS skin_type_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.pre), ',', -1) prefrence_v1 FROM (SELECT  @pre := @pre +1 AS pre FROM catalog_product_entity_varchar cpev, (SELECT @pre:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.pre-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=661) AND eaov.store_id = 0) AS preference_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT  SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.gen), ',', -1) gender_v1 FROM (SELECT  @gen := @gen +1 AS gen FROM catalog_product_entity_varchar cpev, (SELECT @gen:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.gen-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=655) AND eaov.store_id = 0) AS gender_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.form), ',', -1) formulation_v1 FROM (SELECT  @form := @form +1 AS form FROM catalog_product_entity_varchar cpev, (SELECT @form:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.form-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=659) AND eaov.store_id = 0) AS formulation_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.fin), ',', -1) finish_v1 FROM (SELECT  @fin := @fin +1 AS fin FROM catalog_product_entity_varchar cpev, (SELECT @fin:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.fin-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=664) AND eaov.store_id = 0) AS finish_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.occ), ',', -1) occasion_v1 FROM (SELECT  @occ := @occ +1 AS occ FROM catalog_product_entity_varchar cpev, (SELECT @occ:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.occ-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=585) AND eaov.store_id = 0) AS occasion_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.cov), ',', -1) coverage_v1 FROM (SELECT  @cov := @cov +1 AS cov FROM catalog_product_entity_varchar cpev, (SELECT @cov:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.cov-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=663) AND eaov.store_id = 0) AS coverage_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.frag), ',', -1) fragrance_v1 FROM (SELECT  @frag := @frag +1 AS frag FROM catalog_product_entity_varchar cpev, (SELECT @frag:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.frag-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=660) AND eaov.store_id = 0) AS fragrance_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.hair), ',', -1) hair_type_v1 FROM (SELECT  @hair := @hair +1 AS hair FROM catalog_product_entity_varchar cpev, (SELECT @hair:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev  ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.hair-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=677) AND eaov.store_id = 0) AS hair_type_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.skin), ',', -1) skin_tone_v1 FROM (SELECT  @skin := @skin +1 AS skin FROM catalog_product_entity_varchar cpev, (SELECT @skin:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev  ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.skin-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=665) AND eaov.store_id = 0) AS skin_tone_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.ben), ',', -1) benefits_v1 FROM (SELECT  @ben := @ben +1 AS ben FROM catalog_product_entity_varchar cpev, (SELECT @ben:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.ben-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=666) AND eaov.store_id = 0) AS benefits_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT  SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.bri), ',', -1) bristle_type_v1 FROM (SELECT  @bri := @bri +1 AS bri FROM catalog_product_entity_varchar cpev, (SELECT @bri:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.bri-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=667) AND eaov.store_id = 0) AS bristle_type_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.luxu), ',', -1) luxury_fragrance_type_v1 FROM (SELECT  @luxu := @luxu +1 AS luxu FROM catalog_product_entity_varchar cpev, (SELECT @luxu:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.luxu-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=693) AND eaov.store_id = 0) AS luxury_fragrance_type_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.age), ',', -1) age_range_v1 FROM (SELECT  @age := @age +1 AS age FROM catalog_product_entity_varchar cpev, (SELECT @age:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.age-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=546) AND eaov.store_id = 0) AS age_range_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.fragr), ',', -1) fragrance_type_v1 FROM (SELECT  @fragr := @fragr +1 AS fragr FROM catalog_product_entity_varchar cpev, (SELECT @fragr:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.fragr-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=622) AND eaov.store_id = 0) AS fragrance_type_v1,(SELECT GROUP_CONCAT(eaov.value) FROM eav_attribute_option_value AS eaov WHERE eaov.option_id IN (SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(cpev.value, ',', numbers.colle), ',', -1) collection_v1 FROM (SELECT  @colle := @colle +1 AS colle FROM catalog_product_entity_varchar cpev, (SELECT @colle:=0) len LIMIT 100) numbers INNER JOIN catalog_product_entity_varchar cpev ON CHAR_LENGTH(cpev.value)-CHAR_LENGTH(REPLACE(cpev.value, ',', '')) >= numbers.colle-1 WHERE cpev.entity_id =e.entity_id AND cpev.attribute_id=621) AND eaov.store_id = 0) AS collection_v1, "

	sqlquery2="cpeisubs.value is_subscribable,CONCAT('http://adn-static1.nykaa.com/media/catalog/product/tr:h-800,w-800,cm-pad_resize', cpevimg.value) AS main_image,0 AS show_wishlist_button, 'ADD TO BAG' AS button_text,(SELECT GROUP_CONCAT(parent_id SEPARATOR ',') FROM catalog_category_entity WHERE entity_id IN (SELECT category_id FROM catalog_category_product WHERE product_id = e.entity_id AND category_id != 1)) AS parent_category_id,NULL AS brands_v1,NULL AS brands,( SELECT GROUP_CONCAT(CONCAT('http://adn-static1.nykaa.com/media/catalog/product/tr:h-800,w-800,cm-pad_resize', main.value) SEPARATOR '|') FROM catalog_product_entity_media_gallery AS main LEFT JOIN catalog_product_entity_media_gallery_value AS value ON main.value_id = value.value_id AND value.store_id = 0 WHERE (main.attribute_id = '73') AND (main.entity_id = e.entity_id) AND (value.disabled = 0) ) AS image_url_utility,NULL AS l4_category,cpetfp.value AS featured_in_urls,NULL AS featured_in_title,(SELECT eas.attribute_set_name FROM eav_attribute_set AS eas WHERE eas.entity_type_id = 4 AND eas.attribute_set_id = e.attribute_set_id) AS attribute_set_name,(SELECT GROUP_CONCAT(VALUE SEPARATOR '|') FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 1 AND ccpi.category_id != 2 AND cce.level = 2) AND attribute_id = 31) AS categorylevel1,(SELECT VALUE FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 1 AND ccpi.category_id != 2 AND cce.level = 2) AND attribute_id = 31 LIMIT 1) AS catlevel1name,(SELECT GROUP_CONCAT(VALUE SEPARATOR '|') FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 1 AND ccpi.category_id != 2 AND cce.level = 3) AND attribute_id = 31) AS categorylevel2,(SELECT VALUE FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 1 AND ccpi.category_id != 2 AND cce.level = 3) AND attribute_id = 31 LIMIT 1) AS catlevel2name,(SELECT GROUP_CONCAT(VALUE SEPARATOR '|') FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 1 AND ccpi.category_id != 2 AND cce.level = 4) AND attribute_id = 31) AS categorylevel3,(SELECT VALUE FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 1 AND ccpi.category_id != 2 AND cce.level = 4) AND attribute_id = 31 LIMIT 1) AS catlevel3name,(SELECT GROUP_CONCAT(VALUE SEPARATOR '|') FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 1 AND ccpi.category_id != 2 AND cce.level = 5) AND attribute_id = 31) AS categorylevel4,(SELECT VALUE FROM catalog_category_entity_varchar WHERE entity_id IN ( SELECT category_id FROM catalog_category_product_index AS ccpi LEFT JOIN catalog_category_entity AS cce ON ccpi.category_id = cce.entity_id WHERE ccpi.product_id = e . entity_id AND ccpi.store_id = 1 AND ccpi.category_id != 2 AND cce.level = 5) AND attribute_id = 31 LIMIT 1) AS catlevel4name,(SELECT VALUE FROM eav_attribute_option_value AS eaov WHERE eaov.option_id = cpeinvg.value AND eaov.store_id = 0) AS veg_nonveg FROM catalog_product_entity AS e INNER JOIN catalog_product_entity_int AS at_status_default ON (at_status_default.entity_id = e.entity_id) AND (at_status_default.attribute_id = '80') AND at_status_default.store_id = 0 INNER JOIN catalog_product_entity_int AS at_visibility_default ON (at_visibility_default.entity_id = e.entity_id) AND (at_visibility_default.attribute_id = '85') AND at_visibility_default.store_id = 0 LEFT JOIN catalog_product_entity_int  AS cpeid ON cpeid.entity_id = e.entity_id AND cpeid.attribute_id = 718 AND cpeid.store_id = 0 LEFT JOIN catalog_product_entity_int  AS cpeis ON cpeis.entity_id = e.entity_id AND cpeis.attribute_id = 714 AND cpeis.store_id = 0 LEFT JOIN catalog_product_entity_decimal AS cpedp ON cpedp.entity_id = e.entity_id AND cpedp.attribute_id = 60 AND cpedp.store_id = 0 LEFT JOIN catalog_product_entity_decimal AS cpedsp ON cpedsp.entity_id = e.entity_id AND cpedsp.attribute_id = 61 AND cpedsp.store_id = 0 LEFT JOIN catalog_product_entity_varchar  AS cpevpz ON cpevpz.entity_id = e.entity_id AND cpevpz.attribute_id = 588 AND cpevpz.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeisubs ON cpeisubs.entity_id = e.entity_id AND cpeisubs.attribute_id = 790 AND cpeisubs.store_id = 0 LEFT JOIN catalog_product_entity_varchar  AS cpevimg ON cpevimg.entity_id = e.entity_id AND cpevimg.attribute_id = 70 AND cpevimg.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevtag ON cpevtag.entity_id = e.entity_id AND cpevtag.attribute_id = 706 AND cpevtag.store_id = 0 LEFT JOIN catalog_product_entity_text  AS cpetfp ON cpetfp.entity_id = e.entity_id AND cpetfp.attribute_id = 798 AND cpetfp.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeinvg ON cpeinvg.entity_id = e.entity_id AND cpeinvg.attribute_id = 696 AND cpeinvg.store_id = 0 WHERE at_status_default.value = '1' "
	sqlquery3 = " and e.entity_id IN (" + ','.join(productids) +")";

	sqlquery= sqlquery1 + sqlquery2 +sqlquery3
	cursor = nykaa_mysql_conn.cursor()
	cursor.execute(sqlquery)
	columns = cursor.description
	idsvalue = {}
	result1 = []

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
	# pprint(result1)
	# exit()
	mongoresult = list(data.find({'product_id':{'$in': pids}}))
	mongores={}
	mongodata={}
	for value in mongoresult:
		mongodata[value['product_id']]=value
	# pprint(mongodata)
	# exit()	
	i=0
	for fields in result1:
		
		final_value={}
		mongoproductid=fields['product_id']
		# pprint(fields["product_id"])
		# pprint(mongodata[str(fields['product_id'])]["product_id"])
		if mongoproductid == mongodata[fields['product_id']]["product_id"]:
			#pprint(mongoproductid)
			for fields_key in fields.keys():
				if(fields_key=='product_id'):	
					continue	
				if(fields_key in mongodata[mongoproductid].keys() and fields_key != 'product_id'):
					fields_array = str(fields[fields_key]).split(",")

					mongodata_array = mongodata[mongoproductid][fields_key]
					j=0
					keysmongo = "'" + fields_key + "':"
					valuemongo=[]
					value={}
					#pprint(mongodata_array)
					if(mongodata_array != [] and fields_key != 'created_at'):
						#pprint(fields_key)
						for fields_value in fields_array:
							value=mongodata_array[j]
							value['value'] = fields_value
							valuemongo.append(value)
							j+=1
						final_value[fields_key]=valuemongo
				else:
					fields_array = str(fields[fields_key]).split("|")			
					
					extracoladdlist=[]
					for fields_value in fields_array:
						if(fields_key != 'created_at'):
							if(fields_value != 'None'):
								if(fields_key=="price" or fields_key=="special_price"):
									fields_value =float(fields_value)
								final_value[fields_key]=fields_value
							else:
								final_value[fields_key]=fields_value
			#pprint("-----------------")
			#final_value['updated_at']=datetime.now()
			#pprint(final_value)
			data.update({"product_id":fields["product_id"]},{"$set":final_value})
		i += 1

	

update_feed = update_feed3()
