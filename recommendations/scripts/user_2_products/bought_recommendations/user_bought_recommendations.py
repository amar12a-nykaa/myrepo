import os
from collections import defaultdict
import sys
import time
import json
import psutil
import argparse
import operator
import pandas as pd
sys.path.append("/home/ubuntu/nykaa_scripts/sharedutils")
from loopcounter import LoopCounter
sys.path.append("/home/apis/nykaa")
from pas.v2.utils import Utils, RecommendationsUtils
from joblib import Parallel, delayed
from datetime import datetime, timedelta

DEBUG = False

def process_orders(start_datetime=None):

    if start_datetime:
        print("Start Date:" + start_datetime)
        query = "SELECT DISTINCT(customer_id) FROM sales_flat_order WHERE customer_id IS NOT NULL AND created_at >= '%s'" % start_datetime
    else:
        print("No start date")
        query = "SELECT DISTINCT(customer_id) FROM sales_flat_order WHERE customer_id IS NOT NULL"

    rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), query, 10000)     
    customer_ids = [str(row[0]) for row in rows]
    if len(customer_ids) == 0:
        print("No customer ids to process")
        return

    print("Processing for %d customers" % len(customer_ids))

    query = "SELECT customer_id, entity_id from sales_flat_order WHERE customer_id in (%s) ORDER BY created_at DESC ;" % (",".join(customer_ids))
    rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), query, 10000)     
    customer_2_orders = defaultdict(lambda: [])
    for row in rows:
        if len(customer_2_orders[row[0]]) <= 10:
            customer_2_orders[row[0]].append(row[1])

    order_ids = []
    for key in customer_2_orders: 
        order_ids += customer_2_orders[key]
    print("Computing %d orders" % len(order_ids))

    order_products_query = "SELECT order_id, product_id FROM sales_flat_order_item WHERE order_id <> 0 AND mrp > 1 AND parent_item_id IS NULL AND order_id IN (%s);" % ",".join([str(order_id) for order_id in order_ids])
    rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), order_products_query, 10000)     
    order_2_products = defaultdict(lambda: [])
    for row in rows:
        order_2_products[row[0]].append(row[1])
    
    customer_2_product_chunks = defaultdict(lambda: [])
    for customer_id, order_ids in customer_2_orders.items():
        for order_id in order_ids:
            if order_2_products[order_id]:
                customer_2_product_chunks[customer_id].append(order_2_products[order_id])
            else:
                print("No products in order: %d" % order_id)

    recommendation_rows = []
    for customer_id, product_chunks in customer_2_product_chunks.items():
        #print(product_chunks)
        recommendation_rows.append((customer_id, 'user', 'bought', 'coccurence_direct', json.dumps(RecommendationsUtils.customersAlsoBoughtWithMultipleProductsBuckets(product_chunks, 'coccurence_direct'))))

    print("Total Recommendation rows: %d" % len(recommendation_rows))

    values_str = ", ".join(["(%s, %s, %s, %s, %s)" for i in range(len(recommendation_rows))])
    values = tuple([_i for row in recommendation_rows for _i in row])
    insert_recommendations_query = """ INSERT INTO recommendations_v2(entity_id, entity_type, recommendation_type, algo, recommended_products_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json)
    """ % values_str
    Utils.mysql_write(insert_recommendations_query, values, Utils.mysqlConnection())

def insert_recommendations_2_db(recommendation_rows):
    values_str = ", ".join(["(%s, %s, %s, %s, %s)" for i in range(len(recommendation_rows))])
    values = tuple([str(_i) for row in recommendation_rows for _i in row])
    insert_recommendations_query = """ INSERT INTO recommendations_v2(entity_id, entity_type, recommendation_type, algo, recommended_products_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json)
    """ % values_str
    Utils.mysql_write(insert_recommendations_query, values, Utils.mysqlConnection())

def compute_recommendation_rows(customer_ids, entity_type, recommendation_type, algo, recommendations_generation_time, customer_2_product_chunks, recommendation_rows, product_2_recommendations):
    for customer_id in customer_ids:
        recommendation_rows.append((customer_id, entity_type, recommendation_type, algo, json.dumps(RecommendationsUtils.customersAlsoBoughtWithMultipleProductsBuckets(customer_2_product_chunks[customer_id], algo, number_of_suggestions=200, product_2_recommendations=product_2_recommendations))))

def process_orders_df(start_datetime=None):
    print(str(datetime.now()))
    if start_datetime:
        query = "SELECT sfo.customer_id, sfoi.order_id, sfoi.product_id FROM sales_flat_order_item sfoi INNER JOIN sales_flat_order sfo ON sfo.entity_id=sfoi.order_id WHERE sfoi.order_id <> 0 AND sfoi.mrp > 1 AND sfoi.parent_item_id IS NULL AND sfo.customer_id IS NOT NULL AND sfo.customer_id IN (SELECT DISTINCT(customer_id) FROM sales_flat_order WHERE created_at > '%s') ORDER BY sfo.created_at DESC;" % start_datetime
    else:
        query = "SELECT sfo.customer_id, sfoi.order_id, sfoi.product_id FROM sales_flat_order_item sfoi INNER JOIN sales_flat_order sfo ON sfo.entity_id=sfoi.order_id WHERE sfoi.order_id <> 0 AND sfoi.mrp > 1 AND sfoi.parent_item_id IS NULL AND sfo.customer_id IS NOT NULL ORDER BY sfo.created_at DESC;"
    rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), query, 10000)
    if not rows:
        print("No orders to process")
        return
    df = pd.DataFrame(rows)
    df.columns = ['customer_id', 'order_id', 'product_id']
    print("Processing for %d customers" % len(df['customer_id'].unique()))
    print("Total orders processing: %d" % len(df['order_id'].unique()))

    customer_2_orders = defaultdict(lambda: [])
    for row in df.filter(['customer_id', 'order_id']).to_dict(orient='records'):
        if len(customer_2_orders[row['customer_id']]) <= 10:
            customer_2_orders[int(row['customer_id'])].append(int(row['order_id']))

    order_2_products = defaultdict(lambda: [])
    for row in df.filter(['order_id', 'product_id']).to_dict(orient='records'):
        order_2_products[int(row['order_id'])].append(int(row['product_id']))

    customer_2_product_chunks = defaultdict(lambda: [])
    for customer_id, order_ids in customer_2_orders.items():
        for order_id in order_ids:
            if order_2_products[order_id]:
                customer_2_product_chunks[customer_id].append(order_2_products[order_id])
            else:
                print("No products in order: %d" % order_id)

    recommendations_generation_time = datetime.now()
    recommendation_rows = []
    #for algo in ['coccurence_direct', 'coccurence_simple', 'coccurence_log', 'coccurence_sqrt']:
    for algo in ['coccurence_direct']:
        query = "SELECT entity_id, recommended_products_json FROM recommendations_v2 WHERE entity_type='product' AND recommendation_type='bought' AND algo='%s'" % algo
        rows = Utils.fetchResultsInBatch(Utils.mysqlConnection(), query, 10000)
        
        product_2_recommendations = {}
        for row in rows:
            product_2_recommendations[row[0]] = json.loads(row[1])

        print("Computing recommendation rows")
        customer_ids = list(customer_2_product_chunks.keys())
        customer_ids_chunks = [customer_ids[i:i+1000] for i in range(0, len(customer_ids), 1000)]
        Parallel(n_jobs=20, verbose=1, pre_dispatch='1.5*n_jobs', backend="threading")(delayed(compute_recommendation_rows)(customer_ids_chunk, 'user', 'bought', algo, str(recommendations_generation_time), customer_2_product_chunks, recommendation_rows, product_2_recommendations) for customer_ids_chunk in customer_ids_chunks)

    print("Total Recommendation rows: %d" % len(recommendation_rows))
    recommendation_rows_chunks = [recommendation_rows[i:i+1000] for i in range(0, len(recommendation_rows), 1000)]
    for recommendation_rows_chunk in recommendation_rows_chunks:
        insert_recommendations_2_db(recommendation_rows_chunk)
    #Parallel(n_jobs=10, verbose=1, pre_dispatch='1.5*n_jobs', backend="threading")(delayed(insert_recommendations_2_db)(recommendation_rows_chunk) for recommendation_rows_chunk in recommendation_rows_chunks)
    print(str(datetime.now()))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='argument parser')
    parser.add_argument('--all-data', action='store_true')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--hours', type=int)
    parser.add_argument('--days', type=int)
    parser.add_argument('--date')

    argv = vars(parser.parse_args())
    DEBUG = argv['debug']
    if argv.get('all_data'):
        process_orders_df()
        sys.exit()

    if argv.get('date'):
        start_datetime = datetime.strptime(argv['date'], '%Y-%m-%d')
    else:
        start_datetime = datetime.now()
        if argv.get('hours'):
            start_datetime -= timedelta(hours=argv['hours'])
        if argv.get('days'):
            start_datetime -= timedelta(days=argv['days'])

    process_orders_df(str(start_datetime))
