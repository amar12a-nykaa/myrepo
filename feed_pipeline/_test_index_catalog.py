import argparse
import sys
import json

sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
from pas.v2.csvutils import read_csv_from_file


def test_index(url, sample_count):
	rows = read_csv_from_file(url)
	print("Processing CSV .. ")
	columns = rows[0].keys()

	count = 0

	for row in rows:
		if count >= sample_count:
			break

		row_sku = row['sku'].upper()

		query = {"query": {
					"match": {"_id": row_sku}
			}
		}
		response = DiscUtils.makeESRequest(query, index='livecore')
		if len(response['hits']['hits']) == 0:
			continue

		es_data = response['hits']['hits'][0]['_source']
		normal_attributes = ['shipping_quote', 'days_to_return', 'message_on_return', 'return_available', 'country_name']

		filter_attributes = ['brand','gender','color', 'finish','formulation','hair_type',
							 'benefits','skin_tone','skin_type','coverage','preference','spf',
							 'beauty_partner','filter_size','speciality_search','filter_product','usage_period']

		normal_multiselect_attributes = ['catalog_tag']

		for attribute in normal_multiselect_attributes:
			row_values = row.get(attribute)
			es_values = row.get(attribute)

			if es_values != row_values:
				print('SKU', row['sku'])
				print("multiselect attribute values doen't match ", attribute, row_values, es_values)
				continue

		for filter in filter_attributes:
			row_ids = row.get(filter + '_v1')
			row_ids = row_ids.split('|')

			row_values = row.get(filter)
			row_values = row_values.split('|')

			es_ids = es_data.get(filter + '_ids')
			es_values = es_data.get(filter + '_values')
			es_facets = es_data.get(filter + '_facet')

			if (row_ids == [''] or row_ids == ['0']) and es_ids is None:
				continue

			if row_ids != es_ids:
				print('SKU', row['sku'])
				print ("filterable ids doen't match ", filter, row_ids, es_ids)
				continue

			if row_values == [''] and es_values is None:
				continue

			if row_values != es_values:
				print('SKU', row['sku'])
				print ("filterable values doen't match ", filter, row_values, es_values)
				continue

			for i in range(len(row_ids)):
				row_id = row_ids[i]
				row_value = row_values[i]

				found = False
				for es_facet in es_facets:
					es_facet = json.loads(es_facet)
					if es_facet['id'] == row_id and es_facet['name'] == row_value:
						found = True
						break

				if not found:
					row_facet = {"id": row_id, "name": row_value}
					print('SKU', row['sku'])
					print("filterable facets doen't match ", filter, row_facet, es_facets)
					break

		for attribute in normal_attributes:
			row_value = row.get(attribute)
			es_value = es_data.get(attribute)
			if (row_value == '' or row_value == 'NULL') and es_value is None:
				continue
			if es_data.get(attribute) != row.get(attribute):
				print("attribute data don't match for sku ", row_sku, attribute, '-', row.get(attribute), '-', es_data.get(attribute))
				break

		# print('SKU', row['sku'])
		count += 1


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-u", "--url", required=True, help='path to csv file')
	parser.add_argument("-s", "--sample", required=True)
	argv = vars(parser.parse_args())
	url = argv['url']
	sample = argv['sample']
	print('url', url)
	print('sample count ', sample)
	test_index(url=url, sample_count=int(sample))
