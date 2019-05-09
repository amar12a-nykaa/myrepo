import sys
import datetime
import argparse
import time

sys.path.append('/home/apis/pds_api/')

from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils


def get_category_details(batch_size):
	print('batch size : ', batch_size)
	start_date = (datetime.datetime.now() + datetime.timedelta(-30)).strftime('%Y-%m-%d')
	nykaa_redshift_connection = Utils.redshiftConnection()

	fetch_query = """select parent_sku sku, listagg(child_sku, ',') within group (order by count desc) child_skus from (
	select child.sku child_sku, parent.sku parent_sku, count(*) from dim_sku parent 
	join dim_sku child
	on parent.product_id = child.parent_product_id
	join fact_order_detail_new inventory
	on inventory.product_id = child.product_id
	where parent.sku_type = 'configurable' 
	and inventory.orderdetail_dt_created > """+start_date+"""
	group by child.sku, parent.sku) y
	group by parent_sku;"""

	results = Utils.fetchResults(nykaa_redshift_connection, fetch_query)

	batch_array = []
	batch = []

	for index, row in enumerate(results):
		sku = row['sku']
		child_skus = row['child_skus']
		batch.append((sku, child_skus))
		if index % batch_size == 0:
			batch_array.append(batch)
			batch = []

	if len(batch) > 0:
		batch_array.append(batch)

	nykaa_redshift_connection.close()

	gludo_connection = Utils.mysqlConnection('w')
	purge_query = 'DELETE FROM bestseller_product_mapping;'
	gludo_connection.cursor().execute(purge_query)
	print('bestseller_product_mapping purged')

	for batch in batch_array:
		insert_query = 'INSERT INTO bestseller_product_mapping (sku, bestseller_child_skus) VALUES '
		for row in batch:
			insert_query += str(row).upper() + ','
		insert_query = insert_query[:-1]
		gludo_connection.cursor().execute(insert_query)

	gludo_connection.commit()
	gludo_connection.cursor().close()
	gludo_connection.close()
	print('config products : ', index)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-b", "--batchsize", default=200, help='insert batch size', type=int)
	argv = vars(parser.parse_args())
	batch_size = argv.get('batchsize')
	get_category_details(batch_size)
