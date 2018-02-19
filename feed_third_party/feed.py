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

sys.path.append("/nykaa/scripts/sharedutils")

sys.path.append('/home/apis/nykaa/')

from pas.v1.utils import Utils, MemcacheUtils

client = MongoClient("mongofeed.nyk00-int.network")
data = client['local']['feed3']


def create_feed_index():
  j=0
  i=1001
#  j=0
#  j=0
  nykaa_mysql_conn = Utils.nykaaMysqlConnection()
  cursor = nykaa_mysql_conn.cursor()

  for number in range(1,20):
    k=1000
#		j=i+10000
    sqlquery1='SELECT e.entity_id AS product_id, TRIM(e.sku) AS sku,e.type_id AS type_id , TRIM(cpevn.value) AS name,LCASE(CONCAT(cpevn.value," ",REPLACE(cpevn.value," ",""))) AS name_token, (CASE WHEN cpeif.value>0 THEN "Yes" ELSE "No" END) AS fbn, (CASE WHEN at_visibility_default.value=1 THEN "not_visible" ELSE "visible" END) AS visibility, cpev_offer.value AS offer_id, NULL AS offer_name, (CASE WHEN e.type_id = "configurable" AND (SELECT attribute_id FROM catalog_product_super_attribute WHERE product_id = `e`.`entity_id` LIMIT 1) = 587  THEN (SELECT CONCAT(COUNT(*)," Shades" ) FROM catalog_product_super_link AS cpsl LEFT JOIN catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id AND cpei.store_id = 0 AND cpei.attribute_id = 80 WHERE cpei.value = 1 AND cpsl.parent_id = `e`.`entity_id`) WHEN e.type_id = "configurable" AND (SELECT attribute_id FROM catalog_product_super_attribute WHERE product_id = `e`.`entity_id` LIMIT 1) = 604  THEN (SELECT CONCAT(COUNT(*)," Sizes") FROM catalog_product_super_link AS cpsl LEFT JOIN  catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id AND cpei.store_id = 0 AND cpei.attribute_id = 80 WHERE cpei.value = 1 AND cpsl.parent_id = `e`.`entity_id`) WHEN e.type_id = "configurable" AND (SELECT attribute_id FROM catalog_product_super_attribute WHERE product_id = `e`.`entity_id` LIMIT 1) = 741 THEN (SELECT CONCAT(COUNT(*)," Cities") FROM catalog_product_super_link AS cpsl LEFT JOIN  catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id AND cpei.store_id = 0 AND cpei.attribute_id = 80 WHERE cpei.value = 1 AND cpsl.parent_id = `e`.`entity_id`) ELSE cpevpz.value END) AS pack_size, (CASE WHEN cpevp.value>0 THEN cpevp.value ELSE 0 END) AS popularity, CONVERT(( CASE WHEN e.type_id = "bundle" THEN ( SUBSTRING_INDEX(res.rating_count, "/", 1) ) ELSE ( SELECT COUNT(t2.review_id) FROM review t2 LEFT JOIN rating_option_vote t1 ON t1.review_id = t2.review_id WHERE (t2.entity_pk_value = e.entity_id) AND (t2.status_id IN (1, 2)) AND t1.value IS NOT NULL GROUP BY t2.entity_pk_value ) END ),signed)AS rating, REPLACE(cpevbv.value,"," ,"|") AS brands_v1, (SELECT VALUE FROM eav_attribute_option_value AS eaov WHERE eaov.option_id = cpeib.value AND eaov.store_id = 0) AS brand, CONCAT("http://www.nykaa.com/",cur.request_path) AS product_url, (CASE WHEN e.type_id = "simple" THEN CONCAT("http://www.nykaa.com/ajaxaddcart/index/add/product/",e.entity_id,"/isAjax/1") ELSE NULL END) AS add_to_cart_url, cpedsf.value AS special_from_date,cpedst.value AS special_to_date, ( CASE WHEN e.type_id = "bundle" THEN res.display_rating ELSE res.rating_summary END ) AS rating_percentage, res.reviews_count AS review_count , convert(( CASE WHEN e.type_id = "bundle" THEN ROUND((res.display_rating*5)/100, 1) ELSE ROUND((res.rating_summary*5)/100, 1) END ),CHAR) AS rating_num, csi.stock_status AS is_in_stock, REPLACE(cpevcv.value,"," ,"|") AS color_v1, REPLACE(cpevcnv.value,"," ,"|") AS concern_v1, REPLACE(cpevsv.value,"," ,"|") AS spf_v1,REPLACE(cpevstv.value,"," ,"|") AS skin_type_v1, REPLACE(cpevpv.value,"," ,"|") AS preference_v1,REPLACE(cpevgv.value,"," ,"|") AS gender_v1, REPLACE(cpevfv.value,"," ,"|") AS formulation_v1, REPLACE(cpevfnv.value,"," ,"|") AS finish_v1, REPLACE(cpevov.value,"," ,"|") AS occasion_v1, REPLACE(cpevcov.value,"," ,"|") AS coverage_v1, REPLACE(cpevfrv.value,"," ,"|") AS fragrance_v1, REPLACE(cpevhtv.value,"," ,"|") AS hair_type_v1, REPLACE(cpevsktv.value,"," ,"|") AS skin_tone_v1, REPLACE(cpevbnv.value,"," ,"|") AS benefits_v1, REPLACE(cpevbtv.value,"," ,"|") AS bristle_type_v1, cpeiarv.value AS age_range_v1, cpeiftv.value AS fragrance_type_v1, cpeifcv.value AS collection_v1, REPLACE(cpevlftv.value,"," ,"|") AS luxury_fragrance_type_v1 , ( CASE WHEN e.type_id = "configurable" THEN ( SELECT GROUP_CONCAT(cpsl.product_id SEPARATOR ",") FROM  catalog_product_super_link AS cpsl  LEFT JOIN catalog_product_entity_int AS cpei ON cpsl.product_id = cpei.entity_id WHERE cpei.attribute_id = 80 AND cpei.value = 1 AND cpsl.parent_id = e.entity_id ) WHEN e.type_id = "simple" THEN ( SELECT cpsl.parent_id FROM catalog_product_super_link AS cpsl LEFT JOIN catalog_product_entity_int cpei ON cpei.entity_id = cpsl.parent_id WHERE cpei.attribute_id = 80 AND cpei.value = 1 AND cpsl.product_id = e.entity_id GROUP BY cpsl.product_id ) WHEN e.type_id = "bundle" THEN ( SELECT GROUP_CONCAT(cpbs.product_id SEPARATOR ",") FROM  catalog_product_bundle_selection AS cpbs WHERE cpbs.parent_product_id = e.entity_id ) END ) AS parent_id,  ( CASE WHEN e.type_id = "configurable" THEN ( SELECT GROUP_CONCAT(cpep.sku SEPARATOR "|") FROM catalog_product_entity AS cpep  LEFT JOIN catalog_product_super_link cpsl ON cpep.entity_id = cpsl.product_id LEFT JOIN catalog_product_entity_int cpei ON cpei.entity_id = cpsl.product_id  WHERE cpsl.parent_id = e.entity_id AND cpei.attribute_id = 80 AND cpei.value = 1 ) WHEN e.type_id = "simple" THEN ( SELECT cpep.sku FROM catalog_product_entity AS cpep JOIN catalog_product_super_link cpsl ON cpep.entity_id = cpsl.parent_id LEFT JOIN catalog_product_entity_int cpei ON cpei.entity_id = cpsl.parent_id WHERE cpei.attribute_id = 80 AND cpei.value = 1 AND cpsl.product_id = e.entity_id GROUP BY cpsl.product_id ) WHEN e.type_id = "bundle" THEN ( SELECT GROUP_CONCAT(cpep.sku SEPARATOR "|") FROM catalog_product_entity AS cpep LEFT JOIN catalog_product_bundle_selection cpbs ON cpep.entity_id = cpbs.product_id WHERE cpbs.parent_product_id = e.entity_id ) END ) AS parent_sku,convert(( CASE  WHEN e.type_id = "configurable" AND (SELECT attribute_id FROM  catalog_product_super_attribute  WHERE product_id = e . entity_id LIMIT 1) = 587 THEN (SELECT GROUP_CONCAT(eaov_color.option_id SEPARATOR "|")   FROM  catalog_product_entity_int cpev_color  LEFT JOIN eav_attribute_option_value  eaov_color ON cpev_color.value =  eaov_color.option_id  LEFT JOIN catalog_product_super_link  cpsl ON cpev_color.entity_id =  cpsl.product_id  LEFT JOIN catalog_product_entity_int  AS  cpei ON cpsl.product_id =  cpei.entity_id  AND cpei.store_id = 0   WHERE cpev_color.attribute_id = 587  AND cpei.attribute_id = 80  AND cpei.value = 1  AND eaov_color.store_id = 0  AND cpsl.parent_id =  e.entity_id)  WHEN e.type_id = "simple" THEN (SELECT option_id  FROM  eav_attribute_option_value AS eaov  WHERE eaov.option_id IN (SELECT cpev_color.value   FROM  catalog_product_entity_int  cpev_color   WHERE cpev_color.attribute_id = 587  AND cpev_color.entity_id =  e.entity_id) AND eaov.store_id = 0) END ),CHAR) AS shade_id,  ( CASE  WHEN e.type_id = "configurable" AND (SELECT attribute_id  FROM  catalog_product_super_attribute  WHERE product_id = e . entity_id  LIMIT 1) = 587  THEN (SELECT GROUP_CONCAT(SUBSTRING_INDEX(eaov_color.value,"#","-1") SEPARATOR "|")   FROM  catalog_product_entity_int cpev_color  LEFT JOIN eav_attribute_option_value  eaov_color ON cpev_color.value =  eaov_color.option_id  LEFT JOIN catalog_product_super_link  cpsl ON cpev_color.entity_id =  cpsl.product_id  LEFT JOIN catalog_product_entity_int  AS  cpei ON cpsl.product_id =  cpei.entity_id  AND cpei.store_id = 0   WHERE cpev_color.attribute_id = 587  AND cpei.attribute_id = 80  AND cpei.value = 1  AND eaov_color.store_id = 0  AND cpsl.parent_id =  e.entity_id)  WHEN e.type_id = "simple" THEN (SELECT SUBSTRING_INDEX(VALUE,"#","-1")  FROM  eav_attribute_option_value AS eaov  WHERE eaov.option_id IN (SELECT cpev_color.value   FROM  catalog_product_entity_int  cpev_color   WHERE cpev_color.attribute_id = 587  AND cpev_color.entity_id =  e.entity_id) AND eaov.store_id = 0) END ) AS shade_name, ( CASE  WHEN e.type_id = "configurable" AND (SELECT attribute_id  FROM  catalog_product_super_attribute  WHERE product_id = e . entity_id  LIMIT 1) = 587 THEN (SELECT GROUP_CONCAT( CONCAT("http://www.nykaa.com/media/icons/", SUBSTRING_INDEX(LOWER(REPLACE(TRIM(eaov_color.value)," ","%20")),"#",1), ".jpg") SEPARATOR "|" )   FROM  catalog_product_entity_int cpev_color  LEFT JOIN eav_attribute_option_value  eaov_color ON cpev_color.value =  eaov_color.option_id  LEFT JOIN catalog_product_super_link  cpsl ON cpev_color.entity_id =  cpsl.product_id  LEFT JOIN catalog_product_entity_int  AS  cpei ON cpsl.product_id =  cpei.entity_id  AND cpei.store_id = 0   WHERE cpev_color.attribute_id = 587  AND cpei.attribute_id = 80  AND cpei.value = 1  AND eaov_color.store_id = 0  AND cpsl.parent_id =  e.entity_id)  WHEN e.type_id = "simple"  THEN (SELECT CONCAT("http://www.nykaa.com/media/icons/", SUBSTRING_INDEX(LOWER(REPLACE(TRIM(VALUE)," ","%20")),"#",1),".jpg")  FROM  eav_attribute_option_value AS eaov  WHERE eaov.option_id IN (SELECT cpev_color.value   FROM  catalog_product_entity_int  cpev_color   WHERE cpev_color.attribute_id = 587  AND cpev_color.entity_id =  e.entity_id) AND eaov.store_id = 0) END ) AS variant_icon,  ( CASE  WHEN e.type_id = "configurable" AND (SELECT attribute_id  FROM  catalog_product_super_attribute  WHERE product_id = e . entity_id  LIMIT 1) = 604 THEN (SELECT GROUP_CONCAT(eaov_size.option_id SEPARATOR "|")   FROM  catalog_product_entity_int cpev_color  LEFT JOIN eav_attribute_option_value  eaov_size ON cpev_color.value =  eaov_size.option_id  LEFT JOIN catalog_product_super_link  cpsl ON cpev_color.entity_id =  cpsl.product_id  LEFT JOIN catalog_product_entity_int  AS  cpei ON cpsl.product_id =  cpei.entity_id  AND cpei.store_id = 0   WHERE cpev_color.attribute_id = 604  AND cpei.attribute_id = 80  AND cpei.value = 1  AND eaov_size.store_id = 0  AND cpsl.parent_id =  e.entity_id)  WHEN e.type_id = "simple" THEN (SELECT eaov_size.option_id  FROM  catalog_product_entity_int cpev_size  JOIN eav_attribute_option_value eaov_size  ON cpev_size.value = eaov_size.option_id  WHERE cpev_size.attribute_id = 604  AND cpev_size.entity_id = e.entity_id  AND eaov_size.store_id = 0) END ) AS size_id,  ( CASE  WHEN e.type_id = "configurable" AND (SELECT attribute_id  FROM  catalog_product_super_attribute  WHERE product_id = e . entity_id  LIMIT 1) = 604  THEN (SELECT GROUP_CONCAT(eaov_size.value SEPARATOR "|")   FROM  catalog_product_entity_int cpev_color  LEFT JOIN eav_attribute_option_value  eaov_size ON cpev_color.value =  eaov_size.option_id  LEFT JOIN catalog_product_super_link  cpsl ON cpev_color.entity_id =  cpsl.product_id  LEFT JOIN catalog_product_entity_int  AS  cpei ON cpsl.product_id =  cpei.entity_id  AND cpei.store_id = 0   WHERE cpev_color.attribute_id = 604  AND cpei.attribute_id = 80  AND cpei.value = 1  AND eaov_size.store_id = 0  AND cpsl.parent_id =  e.entity_id)  WHEN e.type_id = "simple" THEN (SELECT eaov_size.value  FROM  catalog_product_entity_int cpev_size  JOIN eav_attribute_option_value eaov_size  ON cpev_size.value = eaov_size.option_id  WHERE cpev_size.attribute_id = 604  AND cpev_size.entity_id = e.entity_id  AND eaov_size.store_id = 0) END ) AS size,  NULL AS description,  NULL AS product_use,  NULL AS product_ingredients,  cpeid.value AS d_sku,  NULL AS eretailer,  NULL AS redirect_to_parent,  NULL AS is_a_free_sample,  NULL AS video,  cpeis.value AS shop_the_look_product,  NULL AS shop_the_look_category_tags,  NULL AS shipping_quote,  NULL AS product_expiry,  NULL AS try_it_on,  NULL AS try_it_on_type,  NULL AS bucket_discount_percent,  NULL AS copy_of_sku,  NULL AS qna_count, NULL AS seller_name, NULL AS offer_description, NULL AS style_divas, "" AS highlights, at_status_default.value AS status, e.attribute_set_id AS attribute_set_id, convert (cssi.qty ,signed) AS qty, cssi.backorders AS backorders , (CASE WHEN csi.stock_status > 0 THEN "true" ELSE "false" END) AS availability, NULL AS offer_count, (SELECT eaov.option_id FROM eav_attribute_option_value AS eaov WHERE eaov.option_id = cpeinvg.value AND eaov.store_id = 0) AS veg_nonveg_v1, NULL AS parent_url FROM `catalog_product_entity` AS `e` INNER JOIN `catalog_product_entity_int` AS `at_status_default` ON (`at_status_default`.`entity_id` = `e`.`entity_id`) AND (`at_status_default`.`attribute_id` = "80") AND `at_status_default`.`store_id` = 0 INNER JOIN `catalog_product_entity_int` AS `at_visibility_default` ON (`at_visibility_default`.`entity_id` = `e`.`entity_id`) AND (`at_visibility_default`.`attribute_id` = "85") AND `at_visibility_default`.`store_id` = 0 LEFT JOIN catalog_product_entity_int AS cpeid ON cpeid.entity_id = e.entity_id AND cpeid.attribute_id = 718 AND cpeid.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeis ON cpeis.entity_id = e.entity_id AND cpeis.attribute_id = 714 AND cpeis.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevn ON cpevn.entity_id = e.entity_id AND cpevn.attribute_id = 56 AND cpevn.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeif ON cpeif.entity_id = e.entity_id AND cpeif.attribute_id = 649 AND cpeif.store_id = 0 LEFT JOIN  catalog_product_entity_varchar AS cpevp ON cpevp.entity_id = e.entity_id AND cpevp.attribute_id = 707 AND cpevp.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevpz ON cpevpz.entity_id = e.entity_id AND cpevpz.attribute_id = 588 AND cpevpz.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevbv ON cpevbv.entity_id = e.entity_id AND cpevbv.attribute_id = 668 AND cpevbv.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeib ON cpeib.entity_id = e.entity_id AND cpeib.attribute_id = 66 AND cpeib.store_id = 0 LEFT JOIN core_url_rewrite AS cur ON cur.product_id = e.entity_id AND cur.store_id = 0 AND category_id IS NULL AND is_system = 1 LEFT JOIN catalog_product_entity_datetime AS cpedsf ON cpedsf.entity_id = e.entity_id AND cpedsf.attribute_id = 62 AND cpedsf.store_id = 0 LEFT JOIN catalog_product_entity_datetime AS cpedst ON cpedst.entity_id = e.entity_id AND cpedst.attribute_id = 63 AND cpedst.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevcv ON cpevcv.entity_id = e.entity_id AND cpevcv.attribute_id = 658 AND cpevcv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevcnv ON cpevcnv.entity_id = e.entity_id AND cpevcnv.attribute_id = 656 AND cpevcnv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevsv ON cpevsv.entity_id = e.entity_id AND cpevsv.attribute_id = 662 AND cpevsv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevstv ON cpevstv.entity_id = e.entity_id AND cpevstv.attribute_id = 657 AND cpevstv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevpv ON cpevpv.entity_id = e.entity_id AND cpevpv.attribute_id = 661 AND cpevpv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevgv ON cpevgv.entity_id = e.entity_id AND cpevgv.attribute_id = 655 AND cpevgv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevfv ON cpevfv.entity_id = e.entity_id AND cpevfv.attribute_id = 659 AND cpevfv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevfnv ON cpevfnv.entity_id = e.entity_id AND cpevfnv.attribute_id = 664 AND cpevfnv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevov ON cpevov.entity_id = e.entity_id AND cpevov.attribute_id = 585 AND cpevov.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevcov ON cpevcov.entity_id = e.entity_id AND cpevcov.attribute_id = 663 AND cpevcov.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevfrv ON cpevfrv.entity_id = e.entity_id AND cpevfrv.attribute_id = 660 AND cpevfrv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevhtv ON cpevhtv.entity_id = e.entity_id AND cpevhtv.attribute_id = 677 AND cpevhtv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevsktv ON cpevsktv.entity_id = e.entity_id AND cpevsktv.attribute_id = 665 AND cpevsktv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevbnv ON cpevbnv.entity_id = e.entity_id AND cpevbnv.attribute_id = 666 AND cpevbnv.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpevbtv ON cpevbtv.entity_id = e.entity_id AND cpevbtv.attribute_id = 667 AND cpevbtv.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeiarv ON cpeiarv.entity_id = e.entity_id AND cpeiarv.attribute_id = 546 AND cpeiarv.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeiftv ON cpeiftv.entity_id = e.entity_id AND cpeiftv.attribute_id = 622 AND cpeiftv.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeifcv ON cpeifcv.entity_id = e.entity_id AND cpeifcv.attribute_id = 621 AND cpeifcv.store_id = 0 '

    sqlquery2=' LEFT JOIN catalog_product_entity_varchar AS cpevlftv ON cpevlftv.entity_id = e.entity_id AND cpevlftv.attribute_id = 693 AND cpevlftv.store_id = 0 LEFT JOIN catalog_product_entity_int AS cpeinvg ON cpeinvg.entity_id = e.entity_id AND cpeinvg.attribute_id = 696 AND cpeinvg.store_id = 0 LEFT JOIN cataloginventory_stock_status AS csi ON csi.product_id = e.entity_id LEFT JOIN cataloginventory_stock_item AS cssi ON cssi.product_id = e.entity_id LEFT JOIN review_entity_summary AS res ON res.entity_pk_value = e.entity_id AND res.store_id = 0 LEFT JOIN catalog_product_entity_varchar AS cpev_offer ON cpev_offer.entity_id = e.entity_id AND cpev_offer.attribute_id = 678 AND cpev_offer.store_id = 0  WHERE at_status_default.value = "1" and e.updated_at >= "2015-09-10 00:00:00" GROUP BY e.entity_id limit '+ str(j) +','+str(k)
    sqlquery= sqlquery1 + sqlquery2
    print(str(j))
    print(str(k))
    print(sqlquery)
    exit();
		
    cursor.execute(sqlquery)
    columns = cursor.description
    result = []
    result1 = []
		
    productidappend=[]
    j=i+1000
  #  i=j
 #   print(len(cursor.fetchall()))
#    continue
    relationcoloumns=["shade_id","parent_sku"]
    a=0    
    for value in cursor.fetchall():
      a = a+1
#      print(a)
      tmp = {}
      for (index,values) in enumerate(value):
        tmp[columns[index][0]] = values
        if columns[index][0]=="product_id":
          productidappend.append(values)
        if (columns[index][0].find("_v1") > -1) or (columns[index][0] in relationcoloumns):
          if type(values) == type(None):
            tmp[columns[index][0]]=[]
          else:	
            values=str(values)
            text_id={}
            text_id=values.split("|")
            shadeidappend=[]
            for text_ids in text_id:
              shade_id={}
              shade_id['id']=text_ids
              shadeidappend.append(shade_id)
              del shade_id
            tmp[columns[index][0]]=shadeidappend
            del shadeidappend
        tmp['created_at']=datetime.now()
			# tmp['updated_at']=datetime.now()		
      result.append(tmp)
			#pprint(result['product_id'])
    if(len(result)>0):
      pids=list(set(productidappend))
      data.remove( { "product_id": {"$in" : pids } } )
      resultquery1=data.insert_many(result)
    i=j
#    print(i)
product_id_index = create_feed_index()
