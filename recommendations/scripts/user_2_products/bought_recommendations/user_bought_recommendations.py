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
from IPython import embed

DEBUG = False

def insert_recommendations_2_db(recommendation_rows):
    values_str = ", ".join(["(%s, %s, %s, %s, %s)" for i in range(len(recommendation_rows))])
    values = tuple([str(_i) for row in recommendation_rows for _i in row])
    insert_recommendations_query = """ INSERT INTO recommendations_v2(entity_id, entity_type, recommendation_type, algo, recommended_products_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json)
    """ % values_str
    Utils.mysql_write(insert_recommendations_query, values, Utils.mysqlConnection('w'))

def compute_recommendation_rows(customer_ids, entity_type, recommendation_type, algo, recommendations_generation_time, customer_2_product_chunks, recommendation_rows, product_2_recommendations, customer_2_orders, order_2_products):
    for customer_id in customer_ids:
        recommendation_rows.append((customer_id, entity_type, recommendation_type, algo, json.dumps(RecommendationsUtils.customersAlsoBoughtWithMultipleProductsBuckets(customer_2_product_chunks[customer_id], algo, number_of_suggestions=200, product_2_recommendations=product_2_recommendations))))

def process_orders_df(start_datetime=None):
    print(str(datetime.now()))
    if start_datetime:
        query = "SELECT fact_order_new.order_customerid, fact_order_new.nykaa_orderno, fact_order_detail_new.product_id, fact_order_detail_new.product_sku from fact_order_new INNER JOIN fact_order_detail_new ON fact_order_new.nykaa_orderno=fact_order_detail_new.nykaa_orderno WHERE fact_order_new.order_customerid IN (SELECT DISTINCT(order_customerid) FROM fact_order_new WHERE order_date > '%s') AND fact_order_new.nykaa_orderno <> 0 AND product_mrp > 1 AND order_customerid IS NOT NULL ORDER BY order_date DESC" % start_datetime
    else:
        query = "SELECT fact_order_new.order_customerid, fact_order_new.nykaa_orderno, fact_order_detail_new.product_id, fact_order_detail_new.product_sku from fact_order_new INNER JOIN fact_order_detail_new ON fact_order_new.nykaa_orderno=fact_order_detail_new.nykaa_orderno WHERE fact_order_new.nykaa_orderno <> 0 AND product_mrp > 1 AND order_customerid IS NOT NULL ORDER BY order_date DESC"

    print(query)
    rows = Utils.fetchResultsInBatch(Utils.redshiftConnection(), query, 10000)
    if not rows:
        print("No orders to process")
        return
    df = pd.DataFrame(rows)
    df.columns = ['customer_id', 'order_id', 'product_id', 'sku']

    scroll_results = Utils.scrollESForResults()
    sku_2_product_id = scroll_results['sku_2_product_id']
    child_2_parent = scroll_results['child_2_parent']

    df['product_id'] = df.apply(lambda row: sku_2_product_id.get(row['sku'], row['product_id']), axis=1)
    df['product_id'] = df.apply(lambda row: child_2_parent.get(row['product_id'], row['product_id']), axis=1)
    df = df.drop(['sku'], axis=1)
    #df['group_count'] = 1
    #df = df.groupby(['customer_id', 'order_id', 'product_id']).agg({'group_count': 'sum'}).reset_index().drop(['group_count'], axis=1)
    print("Total dataframe rows: %d" % len(df))
    df = df[df.product_id.notnull()]
    print("Total dataframe rows after filtering product_id is not null: %d" % len(df))

    print("Processing for %d customers" % len(df['customer_id'].unique()))
    print("Total orders processing: %d" % len(df['order_id'].unique()))

    customer_2_orders = defaultdict(lambda: [])
    for row in df.filter(['customer_id', 'order_id']).to_dict(orient='records'):
        if row['order_id'] not in customer_2_orders[row['customer_id']] and len(customer_2_orders[row['customer_id']]) <= 10:
            customer_2_orders[int(row['customer_id'])].append(row['order_id'])

    order_2_products = defaultdict(lambda: [])
    for row in df.filter(['order_id', 'product_id']).to_dict(orient='records'):
        if row['product_id'] not in order_2_products[row['order_id']]:
            order_2_products[row['order_id']].append(int(row['product_id']))

    customer_2_product_chunks = defaultdict(lambda: [])
    for customer_id, order_ids in customer_2_orders.items():
        for order_id in order_ids:
            if order_2_products[order_id]:
                customer_2_product_chunks[customer_id].append(order_2_products[order_id])
            else:
                print("No products in order: %d" % order_id)

    recommendations_generation_time = datetime.now()
    recommendation_rows = []
    for algo in ['coccurence_direct']:
        query = "SELECT entity_id, recommended_products_json FROM recommendations_v2 WHERE entity_type='product' AND recommendation_type='bought' AND algo='%s'" % algo
        rows = Utils.fetchResultsInBatch(Utils.mysqlConnection(), query, 10000)
        
        product_2_recommendations = {}
        for row in rows:
            product_2_recommendations[row[0]] = json.loads(row[1])

        print("Computing recommendation rows")
        customer_ids = list(customer_2_product_chunks.keys())
        customer_ids_chunks = [customer_ids[i:i+1000] for i in range(0, len(customer_ids), 1000)]
        Parallel(n_jobs=1, verbose=1, pre_dispatch='1.5*n_jobs', backend="threading")(delayed(compute_recommendation_rows)(customer_ids_chunk, 'user', 'bought', algo, str(recommendations_generation_time), customer_2_product_chunks, recommendation_rows, product_2_recommendations, customer_2_orders, order_2_products) for customer_ids_chunk in customer_ids_chunks)

    print("Total Recommendation rows: %d" % len(recommendation_rows))
    RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection('w'), 'recommendations_v2', recommendation_rows)
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
