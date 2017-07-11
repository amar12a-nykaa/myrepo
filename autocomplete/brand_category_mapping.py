import sys
sys.path.append("/nykaa/scripts/sharedutils/")

from commonimports import *

db = client['search']
print(db.popularity.find_one())

def nykaaMysqlConnection():
	host = 'nykaa-analytics.nyk00-int.network'
	user = 'analytics'
	password = 'P1u8Sxh7kNr'

	for i in [0,1,2]:
		try:
			return mysql.connector.connect(host=host, user=user, password=password)
		except:
			print("MySQL connection failed! Retyring %d.." % i)
			if i == 2:
				print(traceback.format_exc())
				print("MySQL connection to Nykaa DB failed 3 times. Giving up..")
				raise

conn = nykaaMysqlConnection()
query = """
SELECT DISTINCT cpe.entity_id AS product_id, cpe.type_id AS 'type', cpe.sku, l4.key AS parent_product_id, l4.key_sku AS parent_product_sku, l4.l2 AS L1, l4.l3 AS L2, l4.l4 AS L3, eaov.value AS brand_name FROM nykaalive1.catalog_product_entity cpe 
LEFT JOIN analytics.sku_l4 l4 ON l4.entity_id = cpe.entity_id
LEFT JOIN nykaalive1.catalog_product_entity_varchar cpev ON cpev.entity_id = cpe.entity_id AND cpev.attribute_id = 668
JOIN nykaalive1.eav_attribute_option_value eaov ON eaov.option_id = cpev.value 
WHERE l4.l2 NOT LIKE '%pop%'
AND l4.l2 NOT LIKE '%glam%'
AND l4.l2 NOT LIKE '%acce%'
AND l4.l2 NOT LIKE '%lux%'
and eaov.value = 'Maybelline New York'   
"""
res = Utils.fetchResults(conn, query)
print(len(res))

