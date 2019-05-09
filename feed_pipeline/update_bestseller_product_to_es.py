import sys
import argparse
from contextlib import closing

sys.path.append('/home/apis/pds_api/')

from pas.v2.utils import Utils, hostname, CATALOG_COLLECTION_ALIAS
from elasticsearch import helpers, Elasticsearch

sys.path.append('/nykaa/scripts/sharedutils/')
from esutils import EsUtils

es_connection = Utils.esConn()


def update_es_bestseller_products(docs):
	try:
		es = Utils.esConn()
		actions = []
		for doc in docs:
			actions.append({
				'_op_type': 'update',
				'_index': 'livecore',
				'_type': 'product',
				'_id': doc['sku'],
				'doc': {'bestseller_sku_s': doc['bestseller_skus']}
			})
		kwargs = {
			'stats_only': False,
			'raise_on_error': False
		}
		stats = helpers.bulk(es, actions, **kwargs)
		return stats
	except Exception as e:
		print(e)
		return


def update_bestseller_data(batch_size):

	try:
		print('update_bestseller_data Started ')
		print('batch_size : ', batch_size)
		indexes = EsUtils.get_active_inactive_indexes(CATALOG_COLLECTION_ALIAS)
		active_index = indexes['active_index']
		print("ES Active Index: %s" % active_index)

		fetch_query = """SELECT sku, bestseller_child_skus FROM bestseller_product_mapping;"""
		connection = Utils.mysqlConnection('r')
		with closing(connection.cursor()) as cursor:
			cursor.execute(fetch_query)
			products_array = []
			while True:
				results = cursor.fetchmany(batch_size)
				if not results:
					break
				products = []
				for result in results:
					products.append({
						"sku": result[0],
						"bestseller_skus": result[1]
					})
				products_array.append(products)

			products_updated = 0
			products_skipped = 0
			for products in products_array:
				stats = update_es_bestseller_products(products)
				products_skipped += len(products)
				if stats:
					products_updated += stats[0]
					products_skipped -= stats[0]

			print('Totat products updated : ', products_updated)
			print('Totat products skipped : ', products_skipped)

		print('update_bestseller_data Finished ')
	except:
		print(sys.exc_info()[0])


if __name__=="__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument("-b", "--batchsize", default=200, help='insert batch size', type=int)
	argv = vars(parser.parse_args())
	batch_size = argv.get('batchsize')
	update_bestseller_data(batch_size)