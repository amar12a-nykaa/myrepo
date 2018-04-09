import mysql.connector
import os
import psutil
import time
import operator
import sys
import json

# The factor by which 'counts' of bought_with are boosted for products of the same brand
# DO NOT set it as lesser than 1 unless you intentionally want to downgrade same brand recommendations
BRAND_BOOSTING_FACTOR = 2
CROSS_BRAND_BOOSTING_FACTOR = 0

DB_HOST = 'cataloging-13-nov2017.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
DB_NAME = 'nykaalive1'
DB_USER = 'nykaalive'
DB_PASSWORD = 'oh1ued3phi0uh8ooPh6'

# Results from query are pulled using cursor's 'fetchmany' fn
# This variable probably doesn't affect perf in any significant way if kept in a sane range
BATCH_SIZE = 1000

nykaadb = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
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


orders_query = "SELECT sfoi.order_id, sfoi.product_id, ccpi.category_id, eaov.value " \
               "FROM sales_flat_order_item sfoi FORCE INDEX (IDX_SALES_FLAT_ORDER_ITEM_ORDER_ID) " \
               "LEFT JOIN catalog_product_entity_varchar cpev ON cpev.entity_id = sfoi.product_id AND cpev.attribute_id = 668 " \
               "LEFT JOIN eav_attribute_option_value eaov ON eaov.option_id = cpev.value AND eaov.store_id = 0 " \
               "LEFT JOIN catalog_category_product_index ccpi ON ccpi.product_id = sfoi.product_id " \
               "WHERE (sfoi.order_id <> 0 AND sfoi.price > 1 AND ccpi.category_id IN (" + l1_categories_list_str + ") ) " \
               "ORDER BY sfoi.order_id;"

cursor.execute(orders_query)


# Outer dictionary. Keys are product_id.
# Each key is mapped to an inner dictionary that has the products bought together the count
product_bought_with_dict = {}

# Function that updates the product_bought_with_dict for one 'full order'
# Params: a dict of product_id's that are part of the same order, mapped to their brand and category info
def update_dict_with_full_order(products):
    for p in products:
        if p not in product_bought_with_dict:
            product_bought_with_dict[p] = {}
        for q in products:
            # obviously, do not add the product itself in it's bought_with dict
            if p == q: continue
            # skip if the two products don't belong to the same L1 category
            if products[p]['category'] != products[q]['category']: continue

            if q not in product_bought_with_dict[p]:
                product_bought_with_dict[p][q] = 0

            # Update the count. Same brand products get a boosted count
            product_bought_with_dict[p][q] += CROSS_BRAND_BOOSTING_FACTOR if products[p]['brand'] != products[q]['brand'] else BRAND_BOOSTING_FACTOR


current_order_id = None
# this dict maps the id's of products of current_order to their brand and category info
current_order_products = {}

# Fetches batch of order queries
# Assumes rows fetched are sorted by order_id
# Updates the current_order state as rows are read
# When order_id changes, the compiled product-list of the previous order can be processed to update the main dictionary
def batch_pool_orders():
    global current_order_id
    global current_order_products

    batch_empty = True
    for row in cursor.fetchmany(BATCH_SIZE):
        batch_empty = False

        order_id = row[0]
        product_id = row[1]
        category_id = row[2]
        brand = row[3]

        if current_order_id is None:
            current_order_id = order_id
            current_order_products = { product_id: { 'category': category_id, 'brand': brand } }
            continue

        # If order_id not changed, continue adding products to the 'current' dict
        if order_id == current_order_id:
            current_order_products[product_id] = { 'category': category_id, 'brand': brand }
            continue

        # New order has started. Time to update the 'bought_with' dict and then set the 'current' to a fresh value
        if order_id != current_order_id:
            update_dict_with_full_order(current_order_products)

            current_order_id = order_id
            current_order_products = { product_id: { 'category': category_id, 'brand': brand } }
            continue

    # update the last order
    if batch_empty:
        update_dict_with_full_order(current_order_products)

    return not batch_empty


batch_number = 0
time_start = time.time()
# Fetch orders in batches until all rows are read
while (batch_pool_orders()):
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)

    print('Batch ' + str(batch_number) + ',', str(int(mem)) + 'MB,', str(int(time.time() - time_start)) + 's')
    batch_number += 1

print('Dict Size', len(product_bought_with_dict))


cursor.close()
nykaadb.close()


# Change data to a structure to a more convenient form of product_id mapped to a sorted array of frequently bought items with it
# Also we discard all the less frequently bought products from the array
products_bought_with_arrays_dict = {}
for product_id, bought_with in product_bought_with_dict.items():
    sorted_bought_with_tuples = sorted(bought_with.items(), key=operator.itemgetter(1), reverse=True)[:min(15, len(bought_with))]

    bought_with_array = []
    for f_id, freq in sorted_bought_with_tuples:
        bought_with_array.append({ 'product_id': f_id, 'score': freq })

    products_bought_with_arrays_dict[product_id] = bought_with_array


sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

# Save all results to PAS DB
pasdb = Utils.mysqlConnection()
cursor = pasdb.cursor()

create_table_query = "CREATE TABLE IF NOT EXISTS frequently_bought " \
                     "(" \
                        "product_id INT UNSIGNED NOT NULL, " \
                        "bought_with_json JSON, " \
                        "PRIMARY KEY (product_id)" \
                     ");"

cursor.execute(create_table_query)

for product_id, bought_with_array in products_bought_with_arrays_dict.items():
    insert_update_query = "INSERT INTO frequently_bought (product_id, bought_with_json) VALUES(%s, %s) " \
                          "ON DUPLICATE KEY UPDATE product_id=%s, bought_with_json=%s;"

    cursor.execute(insert_update_query, (product_id, json.dumps(bought_with_array), product_id, json.dumps(bought_with_array)))
    pasdb.commit()


cursor.close()
pasdb.close()


