import sys
import datetime
import argparse


sys.path.append('/home/apis/nykaa/')

from pas.v2.utils import Utils, hostname


def get_category_details(batch_size):
	print('batch size : ', batch_size)
	start_date = (datetime.datetime.now() + datetime.timedelta(-30)).strftime('%Y-%m-%d')
	nykaa_redshift_connection = Utils.redshiftConnection()
	insert_query = """select parent_product_id product_id, listagg(product_id, ',') within group (order by count desc) child_products
	from (
	select child.product_id, child.parent_product_id, count(*)
	from dim_sku parent
	join dim_sku child
	on parent.product_id = child.parent_product_id
	join fact_order_detail_new inventory
	on inventory.product_id = child.product_id
	where parent.sku_type = 'configurable'
	and inventory.orderdetail_dt_created > """ + start_date + """
	group by child.product_id, child.parent_product_id) y
	group by parent_product_id;"""
	results = Utils.fetchResults(nykaa_redshift_connection, insert_query)
	nykaa_redshift_connection.close()

	batch_array = []
	batch = []

	for index, row in enumerate(results):
		product_id = row['product_id']
		config_products = row['child_products']
		batch.append((product_id, config_products))
		if index % batch_size == 0:
			batch_array.append(batch)
			batch = []

	if len(batch) > 0:
		batch_array.append(batch)

	gludo_connection = Utils.mysqlConnection('w')
	purge_query = 'DELETE FROM bestseller_config_products;'
	gludo_connection.cursor().execute(purge_query)

	for batch in batch_array:
		insert_query = 'INSERT INTO bestseller_config_products (product_id, child_products) VALUES '
		for row in batch:
			insert_query += str(row) + ','
		insert_query = insert_query[:-1]

		gludo_connection.cursor().execute(insert_query)

	gludo_connection.cursor().close()
	print('config products : ', len(index))


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-b", "--batchsize", default=200, help='insert batch size', type=int)
	argv = vars(parser.parse_args())
	batch_size = argv.get('batchsize')
	get_category_details(batch_size)
