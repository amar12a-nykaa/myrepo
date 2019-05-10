import os
from collections import defaultdict
import sys
import time
import json
import psutil
import argparse
import operator
sys.path.append("/home/shweta/nykaa/fresh_nykaa_apis/")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils
import pydevd
# Debug mode is activated when a product_id is passed as an argument to the script
# Debug info for that product would be printed in console
# No DB changes are made in DEBUG mode
# DEBUG_MODE = len(sys.argv) != 1
DEBUG_MODE = len(sys.argv) != 1
# Boolean that will be used to signal whether to print fresh logs in console after each batch
DEBUG_PRODUCT_DATA_UPDATED = False
DEBUG_PRODUCT_L1_CATEGORIES = []
DEBUG_PRODUCT_L3_CATEGORIES = []

pydevd.settrace('localhost', port=7000, stdoutToServer=True, stderrToServer=True)
DEBUG_PRODUCT_BRAND = ''
if DEBUG_MODE:
    DEBUG_PRODUCT = int(sys.argv[1])
    print('***************')
    print('Running in debug mode for product ' + str(DEBUG_PRODUCT))
    print('***************')
# The factor by which 'counts' of bought_with are boosted for products of the same brand
# DO NOT set it as lesser than CROSS_BRAND_BOOTSTING_FACTOR unless you intentionally want to downgrade same brand recommendations
BRAND_BOOSTING_FACTOR = 2
# Conversely the factor by which 'count's of cross-brand products are boosted
CROSS_BRAND_BOOSTING_FACTOR = 0

# Results from query are pulled using cursor's 'fetchmany' fn
# This variable probably doesn't affect perf in any significant way if kept in a sane range
BATCH_SIZE = 1000

ES_BATCH_SIZE = 10000
VARIANTS_LIMIT = 100

nykaadb = DiscUtils.nykaaMysqlConnection()
cursor = nykaadb.cursor()

# Construct a list/map of L1 categories from the start so that the orders query is more human-readable
# There are only ~15 L1 categories
l1_categories_map = {}
l1_categories_query = "SELECT entity_id, value " \
                      "FROM catalog_category_entity_varchar " \
                      "WHERE entity_id IN " \
                      "(" \
                            "SELECT cce.entity_id " \
                            "FROM catalog_category_entity cce " \
                            "LEFT JOIN catalog_category_entity_int ccei ON ccei.entity_id = cce.entity_id " \
                            "WHERE cce.level = 2 AND ccei.attribute_id = 498 AND ccei.value = 1 " \
                      ") " \
                      "AND attribute_id = 31;"

cursor.execute(l1_categories_query)
for row in cursor.fetchall():
    l1_categories_map[row[0]] = row[1]
# the string that will be directly inserted/substituted into the orders query
l1_categories_list_str = ", ".join([str(x) for x in l1_categories_map])


l3_categories_map = {}
l3_categories_query = """select category_id, name from nk_categories where depth = 2"""
cursor.execute(l3_categories_query)
for row in cursor.fetchall():
    l3_categories_map[row[0]] = row[1]
l3_categories_list_str = ", ".join([str(x) for x in l3_categories_map])

# This query, although it might look like it, DOES NOT return rows which are UNIQUE in ORDER_ID + PRODUCT_ID
# We are pulling L1 category information through a join, and some products have more than 1 L1 category.
# For each L1 category, a separate row is returned for same order_id & product_id
# TODO: Will using GROUP_CONCAT(ccpi.category_id) result in more efficient query? If so, WHAT to group by?

processed_current_orders = False
processed_archived_orders = False
def prepare_cursor_for_current_orders_table():
    processed_current_orders = True
    if DEBUG_MODE:
        order_ids = []
        product_orders_query = "SELECT order_id from sales_flat_order_item where product_id = %s" % DEBUG_PRODUCT 
        cursor.execute(product_orders_query)
        for row in cursor.fetchall():
            order_ids.append(row[0])
        if not order_ids:
            print("No current orders for this product")
            return
        order_ids_str = ", ".join([str(x) for x in order_ids])
        # using mrp instead of price
        orders_query = "SELECT sfoi.order_id, sfoi.product_id, sfoi.product_type," \
                       "group_concat(distinct ccpi.category_id separator ',') as l1_category, " \
                       "group_concat(distinct ccpi3.category_id separator ',') as l3_category, " \
                       "eaov.value " \
                       "FROM sales_flat_order_item sfoi FORCE INDEX (IDX_SALES_FLAT_ORDER_ITEM_ORDER_ID) " \
                       "LEFT JOIN catalog_product_entity_varchar cpev ON cpev.entity_id = sfoi.product_id AND cpev.attribute_id = 668 " \
                       "LEFT JOIN eav_attribute_option_value eaov ON eaov.option_id = cpev.value AND eaov.store_id = 0 " \
                       "LEFT JOIN catalog_category_product_index ccpi ON ccpi.product_id = sfoi.product_id " \
                       "LEFT JOIN catalog_category_product_index ccpi3 ON ccpi3.product_id = sfoi.product_id " \
                       "WHERE (sfoi.order_id <> 0 AND sfoi.mrp > 1 AND sfoi.order_id in (" + order_ids_str + ") AND " \
                        "ccpi.category_id IN (" + l1_categories_list_str + ") AND ccpi3.category_id IN (" + l3_categories_list_str + "))" \
                       "GROUP BY 1,2 ORDER BY sfoi.order_id;"
    else:
        orders_query = "SELECT sfoi.order_id, sfoi.product_id, sfoi.product_type, " \
                       "group_concat(distinct ccpi.category_id separator ',') as l1_category, " \
                       "group_concat(distinct ccpi3.category_id separator ',') as l3_category, " \
                       "eaov.value " \
                       "FROM sales_flat_order_item sfoi FORCE INDEX (IDX_SALES_FLAT_ORDER_ITEM_ORDER_ID) " \
                       "LEFT JOIN catalog_product_entity_varchar cpev ON cpev.entity_id = sfoi.product_id AND cpev.attribute_id = 668 " \
                       "LEFT JOIN eav_attribute_option_value eaov ON eaov.option_id = cpev.value AND eaov.store_id = 0 " \
                       "LEFT JOIN catalog_category_product_index ccpi ON ccpi.product_id = sfoi.product_id " \
                       "LEFT JOIN catalog_category_product_index ccpi3 ON ccpi3.product_id = sfoi.product_id " \
                       "WHERE (sfoi.order_id <> 0 AND sfoi.mrp > 1 AND ccpi.category_id IN (" + l1_categories_list_str + ")  AND " \
                        " ccpi3.category_id IN (" + l3_categories_list_str + ")) " \
                       "GROUP BY 1,2 ORDER BY sfoi.order_id;"

    cursor.execute(orders_query)

def prepare_cursor_for_archived_orders_table():
    global processed_archived_orders
    processed_archived_orders = True
    if DEBUG_MODE:
        order_ids = []
        product_orders_query = "SELECT order_id from sales_flat_order_item_archive where product_id = %s" % DEBUG_PRODUCT 
        cursor.execute(product_orders_query)
        for row in cursor.fetchall():
            order_ids.append(row[0])
        if not order_ids:
            print("No archived orders for this product")
            return
        order_ids_str = ", ".join([str(x) for x in order_ids])
        # using mrp instead of price
        orders_query = "SELECT sfoi.order_id, sfoi.product_id, sfoi.product_type, " \
                       "group_concat(distinct ccpi.category_id separator ',') as l1_category, " \
                       "group_concat(distinct ccpi3.category_id separator ',') as l3_category, " \
                       "eaov.value " \
                       "FROM sales_flat_order_item_archive sfoi FORCE INDEX (IDX_SALES_FLAT_ORDER_ITEM_ORDER_ID) " \
                       "LEFT JOIN catalog_product_entity_varchar cpev ON cpev.entity_id = sfoi.product_id AND cpev.attribute_id = 668 " \
                       "LEFT JOIN eav_attribute_option_value eaov ON eaov.option_id = cpev.value AND eaov.store_id = 0 " \
                       "LEFT JOIN catalog_category_product_index ccpi ON ccpi.product_id = sfoi.product_id " \
                       "LEFT JOIN catalog_category_product_index ccpi3 ON ccpi3.product_id = sfoi.product_id " \
                       "WHERE (sfoi.order_id <> 0 AND sfoi.mrp > 1 AND sfoi.order_id in (" + order_ids_str + ") AND " \
                       "ccpi.category_id IN (" + l1_categories_list_str + ") AND ccpi3.category_id IN (" + l3_categories_list_str + ")) " \
                       "GROUP BY 1,2 ORDER BY sfoi.order_id;"
    else:
        orders_query = "SELECT sfoi.order_id, sfoi.product_id, sfoi.product_type, " \
                       "group_concat(distinct ccpi.category_id separator ',') as l1_category, " \
                       "group_concat(distinct ccpi3.category_id separator ',') as l3_category, " \
                       "eaov.value " \
                       "FROM sales_flat_order_item_archive sfoi " \
                       "LEFT JOIN catalog_product_entity_varchar cpev ON cpev.entity_id = sfoi.product_id AND cpev.attribute_id = 668 " \
                       "LEFT JOIN eav_attribute_option_value eaov ON eaov.option_id = cpev.value AND eaov.store_id = 0 " \
                       "LEFT JOIN catalog_category_product_index ccpi ON ccpi.product_id = sfoi.product_id " \
                       "WHERE (sfoi.order_id <> 0 AND sfoi.mrp > 1 AND ccpi.category_id IN (" + l1_categories_list_str + ") " \
                       " AND ccpi3.category_id IN (" + l3_categories_list_str + "))" \
                       "GROUP BY 1,2 ORDER BY sfoi.order_id;"

    cursor.execute(orders_query)

# Outer dictionary. Keys are product_id.
# Each key is mapped to an inner dictionary that has the products bought together the count
product_bought_with_dict = {}


# Function that updates the product_bought_with_dict for one 'full order'
# Params: a dict of product_id's that are part of the same order, mapped to their brand and category info
def update_dict_with_full_order(products):
    global DEBUG_PRODUCT_DATA_UPDATED
    global DEBUG_PRODUCT_BRAND
    global DEBUG_PRODUCT_L1_CATEGORIES
    global DEBUG_PRODUCT_L3_CATEGORIES

    # checks if the pair of products have any common L1 category
    def any_common_l1_category(p1,p2):
        common_l1_categories = [c for c in p1['l1_categories'] if c in p2['l1_categories']]
        return len(common_l1_categories) > 0

    # checks if the pair of products have any common L3 category
    def any_common_l3_category(p1,p2):
        common_l3_categories = [c for c in p1['l3_categories'] if c in p2['l3_categories']]
        return len(common_l3_categories) > 0

    # In Debug Mode, if given product is not present in order, there's no need to process the order
    if DEBUG_MODE and DEBUG_PRODUCT not in products:
        return

    for p in products:
        # In Debug Mode, we're only interested in collecting info for the given product
        if DEBUG_MODE and p != DEBUG_PRODUCT: continue
        

        if DEBUG_MODE and p == DEBUG_PRODUCT:
            DEBUG_PRODUCT_L1_CATEGORIES = products[p]['l1_categories']
            DEBUG_PRODUCT_L3_CATEGORIES = products[p]['l3_categories']
            DEBUG_PRODUCT_BRAND = products[p]['brand']

        DEBUG_PRODUCT_DATA_UPDATED = True
        if p not in product_bought_with_dict:
            product_bought_with_dict[p] = {}
        for q in products:
            # obviously, do not add the product itself in it's bought_with dict
            if p == q: continue
            #if products[p]['brand'] != products[q]['brand']: continue

            if q not in product_bought_with_dict[p]:
                product_bought_with_dict[p][q] = {'score': 0}
                if DEBUG_MODE:
                    # extra fields are for debugging purposes
                    product_bought_with_dict[p][q] = {'score': 0, 'total_count': 0, 'category_l1_matching': False, 'category_l3_matching': False,
                                                      'brand_matching': False, 'l1_categories': products[q]['l1_categories'],
                                                      'l3_categories': products[q]['l3_categories'], 'brand': products[q]['brand']}

            if DEBUG_MODE:
                product_bought_with_dict[p][q]['total_count'] += 1

            # stop here if the two products don't have at least 1 common L1 category
            if ((not any_common_l1_category(products[p], products[q])) or any_common_l3_category(products[p], products[q])
                    or products[p]['brand'] != products[q]['brand']):
                if not DEBUG_MODE:
                    product_bought_with_dict[p].pop(q)
                continue

            if DEBUG_MODE:
                product_bought_with_dict[p][q]['category_l1_matching'] = True
                product_bought_with_dict[p][q]['category_l3_matching'] = False
                product_bought_with_dict[p][q]['brand_matching'] = True

            # Update the score. Same brand products get a boosted count
            product_bought_with_dict[p][q]['score'] += 1


current_order_id = None
# this dict maps the id's of products of current_order to their brand and category info
current_order_products = {}

# Fetches batch of order queries
# Assumes rows fetched are sorted by order_id
# Updates the current_order state as rows are read
# When order_id changes, the compiled product-list of the previous order can be processed to update the main dictionary
prepare_cursor_for_current_orders_table()
def batch_pool_orders():
    global current_order_id
    global current_order_products

    batch_empty = True
    for row in cursor.fetchmany(BATCH_SIZE):
        batch_empty = False

        order_id = row[0]
        product_id = row[1]
        product_type = row[2]
        l1_category = row[3]
        l3_category = row[4]
        brand = row[5]

        # Consider only simple products
        # TODO need to exclude product_type configurable
        if product_type != 'simple': continue

        if l1_category:
            l1_category = l1_category.split(",")
        else:
            l1_category = []
        if l3_category:
            l3_category = l3_category.split(",")
        else:
            l3_category = []

        if current_order_id is None:
            current_order_id = order_id
            current_order_products = { product_id: { 'l1_categories': l1_category, 'l3_categories' : l3_category, 'brand': brand } }
            continue

        # If order_id not changed, continue adding products to the 'current' dict
        if order_id == current_order_id:
            current_order_products[product_id] = { 'l1_categories': l1_category, 'l3_categories' : l3_category, 'brand': brand }

        # New order has started. Time to update the 'bought_with' dict and then set the 'current' to a fresh value
        if order_id != current_order_id:
            if DEBUG_MODE:
                print("Order Details")
                print("Order ID = " + str(current_order_id))
                print(current_order_products)

            update_dict_with_full_order(current_order_products)

            current_order_id = order_id
            current_order_products = { product_id: { 'l1_categories': l1_category, 'l3_categories' : l3_category, 'brand': brand } }
            continue

    # update the last order
    if batch_empty:
        update_dict_with_full_order(current_order_products)
        if not processed_archived_orders:
            prepare_cursor_for_archived_orders_table()
            return True

    return not batch_empty


batch_number = 0
time_start = time.time()
# Fetch orders in batches until all rows are read
while (batch_pool_orders()):
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)

    print('Batch ' + str(batch_number) + ',', str(int(mem)) + 'MB,', str(int(time.time() - time_start)) + 's')
    batch_number += 1

    if DEBUG_MODE:
        print(json.dumps(product_bought_with_dict, indent=4))
    if DEBUG_MODE and DEBUG_PRODUCT_DATA_UPDATED:
        for product_id, bought_with in product_bought_with_dict.items():
            # sort the array in descending of total_count
            sorted_bought_with_tuples = sorted(bought_with.items(), key=lambda tup: tup[1]['total_count'], reverse=True)[:min(5, len(bought_with))]
            print('Top 5 (by total count) frequently bought details for product "' + str(product_id), '(' + DEBUG_PRODUCT_BRAND + ')', DEBUG_PRODUCT_L1_CATEGORIES, '" so far')
            print('')
            print('Count, Category Match, Brand Match, Score, Product Details')
            for f_id, freq_info in sorted_bought_with_tuples:
                print(str(freq_info['total_count']) + ', ' + str(freq_info['category_l1_matching']) + ', ' +
                      str(freq_info['category_l3_matching']) + ', ' + str(freq_info['brand_matching']) + ', ' +
                      str(freq_info['score']),
                      'Product:', f_id, '(' + freq_info['brand'] + ')', freq_info['l1_categories'], freq_info['l3_categories'])
            print('')
        DEBUG_PRODUCT_DATA_UPDATED = False
print('Dict Size', len(product_bought_with_dict))


cursor.close()
nykaadb.close()


# Change data to a structure to a more convenient form of product_id mapped to a sorted array of frequently bought items with it
# Also we discard all the less frequently bought products from the array
products_bought_with_arrays_dict = {}
for product_id, bought_with in product_bought_with_dict.items():
    # sort the array in descending of score
    sorted_bought_with_tuples = sorted(bought_with.items(), key=lambda tup: tup[1]['score'], reverse=True)[:min(15, len(bought_with))]

    bought_with_array = []
    for f_id, freq_info in sorted_bought_with_tuples:
        bought_with_array.append({ 'product_id': f_id, 'score': freq_info['score'] })

    products_bought_with_arrays_dict[product_id] = bought_with_array


# Saves all results to PAS DB
pasdb = DiscUtils.mysqlConnection('w')
cursor = pasdb.cursor()

def write_results_to_db():

    # Frequently bought info stored as a JSON array for each product_id
    create_table_query = "CREATE TABLE IF NOT EXISTS frequently_bought_v3 " \
                         "(" \
                         "product_id INT UNSIGNED NOT NULL, " \
                         "bought_with_json JSON, " \
                         "PRIMARY KEY (product_id)" \
                         ");"

    cursor.execute(create_table_query)

    for product_id, bought_with_array in products_bought_with_arrays_dict.items():
        insert_update_query = "INSERT INTO frequently_bought_v3 (product_id, bought_with_json) VALUES(%s, %s) " \
                              "ON DUPLICATE KEY UPDATE product_id=%s, bought_with_json=%s;"

        cursor.execute(insert_update_query,
                       (product_id, json.dumps(bought_with_array), product_id, json.dumps(bought_with_array)))
        pasdb.commit()

if not DEBUG_MODE:
    write_results_to_db()


product_with_no_variants = []
product_with_no_variants_in_es = []

def _add_configurable_product(product_id, source):
   if not source.get('variants'):
       product_with_no_variants.append(source['product_id'])
       return None
   variants = json.loads(source['variants'])
   child_product_ids = [str(_product['id']) for variant_type, _variants in variants.items() for _product in _variants]
   if not child_product_ids:
       product_with_no_variants.append(source['product_id'])
       return None
   query = {
       "size": VARIANTS_LIMIT,
       "query": {
           "terms": {
               "product_id.keyword": child_product_ids
           }
       },
       "_source": ["product_id"],
       "sort": [
           {"discount": {"order": "desc"}}
       ]
   }
   response = DiscUtils.makeESRequest(query, index='livecore')
   sorted_variants = [int(hit['_source']['product_id']) for hit in response['hits']['hits']]
   if not sorted_variants:
       product_with_no_variants_in_es.append(source['product_id'])
       return None
   frequently_bought_query = "SELECT product_id, bought_with_json FROM frequently_bought WHERE product_id IN (" + ", ".join([str(c) for c in sorted_variants]) + ");"
   cursor.execute(frequently_bought_query)
   results = cursor.fetchall()
   _frequent_bought_dict = dict(results)
   for p in sorted_variants:
       bought_together = _frequent_bought_dict.get(p)
       if bought_together and json.loads(bought_together):
           return (source['product_id'], bought_together)
   return None

def single_configurable_product(product_id):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type.keyword": "configurable"}},
                    {"term": {"product_id.keyword": product_id}}
                ]
            }
        },
        "_source": ["product_id", "variants"]
    }
    response = es_conn.search(index='livecore', body=query)
    if not response['hits']['hits']:
        print("No product with the given product_id")
        return
    row = _add_configurable_product(product_id, response['hits']['hits'][0]['_source'])
    if row:
        print(row)

    print("Successfully added frequently bought products for " + str(len(added_product_ids)) + ": ")
    print(added_product_ids)
    print("Failed to add frequently bought products for " + str(len(failed_product_ids)) + ": ")
    print(failed_product_ids)

def add_all_configurable_products():
    print("Adding for configurable products")
    es_conn = DiscUtils.esConn()
    scroll_id = None
    added_product_ids = []
    failed_product_ids = []
    while True:
        if not scroll_id:
            query = {
                "size": ES_BATCH_SIZE,
                "query": {
                    "term": {
                        "type.keyword": "configurable"
                    }
                },
                "_source": ["product_id", "variants"]
            }
            response = es_conn.search(index='livecore', body=query, scroll='25m')
        else:
            response = es_conn.scroll(scroll_id=scroll_id, scroll='25m')

        if not response['hits']['hits']:
            break
        print("Processing " + str(len(response['hits']['hits'])) + " configurable products")
        batch_product_ids = [hit['_source']['product_id'] for hit in response['hits']['hits']]
        if DEBUG_MODE:
            print("Processing product ids")
            print(batch_product_ids)

        scroll_id = response['_scroll_id']
        rows = []
        for hit in response['hits']['hits']:
            row = _add_configurable_product(hit['_source']['product_id'], hit['_source'])
            if row:
                rows.append(row)
        values_str = ", ".join(["(%s, %s)" for i in range(len(rows))])
        values = tuple([_i for row in rows for _i in row])
        insert_update_query = "INSERT INTO frequently_bought (product_id, bought_with_json) VALUES %s " \
                              "ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), bought_with_json=VALUES(bought_with_json);" % values_str

        cursor.execute(insert_update_query, values)
        pasdb.commit()
        added_product_ids += [row[0] for row in rows]
        failed_product_ids += list(set(batch_product_ids) - set(added_product_ids))

    if scroll_id:
        es_conn.clear_scroll(scroll_id=scroll_id)
    print("Successfully added frequently bought products for " + str(len(added_product_ids)) + ": ")
    print(added_product_ids)
    print("Failed to add frequently bought products for " + str(len(failed_product_ids)) + ": ")
    print(failed_product_ids)

if not DEBUG_MODE:
    add_all_configurable_products()

cursor.close()
pasdb.close()

