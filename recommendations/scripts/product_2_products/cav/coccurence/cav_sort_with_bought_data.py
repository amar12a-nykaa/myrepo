import os
from collections import defaultdict
import sys
import time
import json
import psutil
import argparse
import operator
import pandas as pd
sys.path.append("/nykaa/api")
from pas.v2.utils import Utils, RecommendationsUtils
sys.path.append("/home/ubuntu/nykaa_scripts/utils/")
from pandasutils import parallelize_dataframe
sys.path.append("/home/ubuntu/nykaa_scripts/sharedutils/")
from loopcounter import LoopCounter
from IPython import embed

DEBUG = False

child_2_parent = {}

def convert_2_parent_product(data):
    global child_2_parent
    data['product_id'] = data.apply(lambda row: child_2_parent.get(row['product_id'], row['product_id']), axis=1)
    return data

def get_bought_sorted_df(datetime):
    global child_2_parent 
    print("Using orders data from %s" % str(datetime))
    orders_query = "SELECT order_id, product_id, mrp FROM sales_flat_order_item WHERE order_id <> 0 AND mrp > 1 and product_type='simple' and created_at >= '%s'; " % datetime
    rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), orders_query, 10000)
    print("Total rows extracted: %d" % len(rows))
    df = pd.DataFrame(rows)
    df.head()
    print("Total rows in dataframe: %d" % len(df))
    df.columns = ['order_id', 'product_id', 'mrp']
    df['bought_count'] = 1

    child_product_relation_query = "select child_id, parent_id from catalog_product_relation"

    rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), child_product_relation_query, 50000)
    child_2_parent = {row[0]: row[1] for row in rows}
    common_results = Utils.scrollESForResults()
    child_2_parent.update(common_results['child_2_parent'])

    df_parented = parallelize_dataframe(df, convert_2_parent_product, 40, 16)
    df_parented = df_parented.filter(['product_id', 'mrp', 'bought_count'])
    df_grouped = df_parented.groupby(['product_id']).agg({'mrp': 'sum', 'bought_count': 'count'}).reset_index()
    return df_grouped

def rank_by_bought_data(source_algo, algo, datetime, limit=None):
    global DEBUG
    bought_df = get_bought_sorted_df(datetime)
    print("Got bought_df: len=%d" % len(bought_df))
    if limit:
        query = "SELECT entity_id, recommended_products_json FROM `recommendations_v2` WHERE entity_type='product' and recommendation_type='viewed' and algo='%s' LIMIT %d" % (source_algo, limit)
    else:
        query = "SELECT entity_id, recommended_products_json FROM `recommendations_v2` WHERE entity_type='product' and recommendation_type='viewed' and algo='%s'" % source_algo
    mysql_conn = Utils.mysqlConnection('w')
    cursor = mysql_conn.cursor()
    cursor.execute(query)
    CHUNK_SIZE = 15
    rows = []
    ctr = LoopCounter(name='Reranking the results')
    for row in cursor.fetchall():
        ctr += 1
        if ctr.should_print():
            print(ctr.summary)
        product_id = row[0]
        recommendations = json.loads(row[1])
        if DEBUG:
            print("\n")
            print("Calculating for product id: %d" % product_id)
        new_recommendations = []

        for i in range(0, len(recommendations), CHUNK_SIZE):
            chunk = recommendations[i:i+CHUNK_SIZE]
            if DEBUG:
                print("Sorting products: ")
                print(chunk)
            _df = bought_df[bought_df['product_id'].isin(chunk)].sort_values(by=['bought_count'], ascending=False)
            new_recommendations += [p['product_id'] for p in _df.to_dict(orient='records')]
            if DEBUG:
                print("After sorting products: ")
                print([p['product_id'] for p in _df.to_dict(orient='records')])

        if new_recommendations:
            if DEBUG:
                print("New recommendations: ")
                print(new_recommendations)
            rows.append((str(product_id), 'product', 'viewed', algo, json.dumps(new_recommendations)))
        
    RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection('w'), 'recommendations_v2', rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-algo', '-s', required=True)
    parser.add_argument('--new-algo', '-n', required=True)
    parser.add_argument('--start-bought-datetime', '-b', required=True)
    parser.add_argument('--limit', '-l', type=int)
    parser.add_argument('--debug', '-d', action='store_true')

    argv = vars(parser.parse_args())
    source_algo = argv['source_algo']
    new_algo = argv['new_algo']
    start_bought_datetime = argv['start_bought_datetime']
    rank_by_bought_data(source_algo, new_algo, start_bought_datetime, argv.get('limit'))
    DEBUG = argv.get('debug', False)
