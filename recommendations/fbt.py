import pandas as pd
import argparse
import sys
from pandas.io import sql
from multiprocessing import Pool
import numpy as np
import math
import json
from collections import defaultdict

sys.path.append('/nykaa/api')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils

nykaa_mysql_conn = Utils.nykaaMysqlConnection()

simple_similar_products_dict = {}
direct_similar_products_dict = {}
log_similar_products_dict = {}
sqrt_similar_products_dict = {}


def create_coccurence_matrix(df, df_len):
    BATCH_SIZE = 5000000
    offset = 0
    final_df = pd.DataFrame([])

    while offset < df_len:
        last_idx = offset + BATCH_SIZE if offset + BATCH_SIZE < df_len else df_len
        print("Started processing data from offset %d till %d" % (offset, last_idx - 1))
        temp_df = df[offset:last_idx]
        temp_df = pd.merge(temp_df, temp_df, on='order_id', how='inner')
        temp_df = temp_df[temp_df['product_id_x'] < temp_df['product_id_y']].reset_index(drop=True)
        temp_df = temp_df.drop('order_id', axis=1)
        temp_df['order_count'] = 1
        temp_df = temp_df.groupby(['product_id_x', 'product_id_y']).agg({'order_count': 'sum'}).reset_index()
        final_df = final_df.append(temp_df)
        final_df = final_df.groupby(['product_id_x', 'product_id_y']).agg({'order_count': 'sum'}).reset_index()
        offset = last_idx

    return final_df


def parallelize_dataframe(df, func, num_partitions, num_cores):
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def compute_orders_union_len(data):
    data['order_union'] = data.apply(
        lambda row: product_2_orders_cnt[row['product_id_x']] + product_2_orders_cnt[row['product_id_y']] - row[
            'order_count'], axis=1)
    return data


def compute_similarity(data):
    data['direct_similarity'] = data.apply(lambda row: row['order_count'] / row['order_union'], axis=1)
    data['log_similarity'] = data.apply(lambda row: row['order_count'] / math.log(row['order_union']), axis=1)
    data['sqrt_similarity'] = data.apply(lambda row: row['order_count'] / math.sqrt(row['order_union']), axis=1)
    return data


def get_recommendations(product_id, recommendations_cnt=10, with_name=True, algo='coccurence_direct'):
    if algo == 'coccurence_log':
        temp_df = final_df[
            (final_df['product_id_x'] == product_id) | (final_df['product_id_y'] == product_id)].sort_values(
            ['log_similarity'], ascending=[0])
    elif algo == 'coccurence_sqrt':
        temp_df = final_df[
            (final_df['product_id_x'] == product_id) | (final_df['product_id_y'] == product_id)].sort_values(
            ['sqrt_similarity'], ascending=[0])
    elif algo == 'coccurence_direct':
        temp_df = final_df[
            (final_df['product_id_x'] == product_id) | (final_df['product_id_y'] == product_id)].sort_values(
            ['direct_similarity'], ascending=[0])
    else:
        temp_df = final_df[
            (final_df['product_id_x'] == product_id) | (final_df['product_id_y'] == product_id)].sort_values(
            ['order_intersection'], ascending=[0])
    rows = temp_df.values.tolist()
    recommendation_ids = []
    for row in rows:
        if row[0] == product_id:
            recommendation_ids.append(row[1])
        elif row[1] == product_id:
            recommendation_ids.append(row[0])
    recommendation_ids = recommendation_ids[:recommendations_cnt]
    if not with_name:
        return recommendation_ids
    query = {
        "query": {
            "terms": {"product_id.keyword": recommendation_ids}
        },
        "_source": ['title_text_split', 'product_id']
    }
    response = Utils.makeESRequest(query, index='livecore')
    product_id_2_name = {int(hit['_source']['product_id']): hit['_source']['title_text_split'] for hit in
                         response['hits']['hits']}
    return [(recommendation_id, product_id_2_name[recommendation_id]) for recommendation_id in recommendation_ids]


def create_product_mapping(final_df):
    for row in final_df.values.tolist():
        if not row[0] in simple_similar_products_dict:
            simple_similar_products_dict[row[0]] = []
        if not row[1] in simple_similar_products_dict:
            simple_similar_products_dict[row[1]] = []

        if not row[0] in direct_similar_products_dict:
            direct_similar_products_dict[row[0]] = []
        if not row[1] in direct_similar_products_dict:
            direct_similar_products_dict[row[1]] = []

        if not row[0] in log_similar_products_dict:
            log_similar_products_dict[row[0]] = []
        if not row[1] in log_similar_products_dict:
            log_similar_products_dict[row[1]] = []

        if not row[0] in sqrt_similar_products_dict:
            sqrt_similar_products_dict[row[0]] = []
        if not row[1] in sqrt_similar_products_dict:
            sqrt_similar_products_dict[row[1]] = []

        simple_similar_products_dict[row[0]].append((row[1], row[2]))
        simple_similar_products_dict[row[1]].append((row[0], row[2]))

        direct_similar_products_dict[row[0]].append((row[1], row[4]))
        direct_similar_products_dict[row[1]].append((row[0], row[4]))

        log_similar_products_dict[row[0]].append((row[1], row[5]))
        log_similar_products_dict[row[1]].append((row[0], row[5]))

        sqrt_similar_products_dict[row[0]].append((row[1], row[6]))
        sqrt_similar_products_dict[row[1]].append((row[0], row[6]))


def create_recommendation_table(cursor):
    create_recommendations_table_query = """create table if not exists recommendations_fbt_panda (
                            entity_id int unsigned not null,
                            entity_type varchar(50),
                            recommendation_type varchar(50),
                            algo varchar(50),
                            recommended_products_json JSON,
                            PRIMARY KEY (entity_id, entity_type, recommendation_type, algo)
                        )
    """
    cursor.execute(create_recommendations_table_query)


def write_data_in_table(db, cursor, rows):
    values_str = ", ".join(["(%s, %s, %s, %s, %s)" for i in range(len(rows))])
    values = tuple([str(data) for row in rows for data in row])
    insert_recommendations_query = """insert into recommendations_fbt_panda(entity_id, entity_type, 
                recommendation_type, algo, recommended_products_json) values %s on duplicate key 
                update recommended_products_json = values(recommended_products_json)
    """ % values_str
    cursor.execute(insert_recommendations_query, values)
    db.commit()


def populate_recommendations_in_mysql(unique_products):
    pasdb = Utils.mysqlConnection('w')
    cursor = pasdb.cursor()

    create_recommendation_table(cursor)

    rows = []
    products_count = 0

    for product_id in unique_products:
        if not simple_similar_products_dict[product_id]:
            continue
        simple_similar_products = list(map(lambda e: e[0],
                                           sorted(simple_similar_products_dict[product_id], key=lambda e: e[1],
                                                  reverse=True)[:20]))
        direct_similar_products = list(map(lambda e: e[0],
                                           sorted(direct_similar_products_dict[product_id], key=lambda e: e[1],
                                                  reverse=True)[:20]))
        log_similar_products = list(
            map(lambda e: e[0], sorted(log_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:20]))
        sqrt_similar_products = list(
            map(lambda e: e[0], sorted(sqrt_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:20]))

        rows.append((product_id, 'product', 'fbt', 'coccurence_simple', str(simple_similar_products)))
        rows.append((product_id, 'product', 'fbt', 'coccurence_direct', str(direct_similar_products)))
        rows.append((product_id, 'product', 'fbt', 'coccurence_log', str(log_similar_products)))
        rows.append((product_id, 'product', 'fbt', 'coccurence_sqrt', str(sqrt_similar_products)))

        products_count += 1
        if len(rows) == 40:
            write_data_in_table(pasdb, cursor, rows)
            rows = []
        write_data_in_table(pasdb, cursor, rows)

    cursor.close()
    pasdb.close()
    print('Count of added recommendation : %d' % (products_count))


def populate_simple_variant_recommendation():
    nykaadb = Utils.nykaaMysqlConnection()
    nykaa_cursor = nykaadb.cursor()
    query = "select parent_id, child_id from catalog_product_relation"
    nykaa_cursor.execute(query)

    parent_2_children = {}
    for row in nykaa_cursor.fetchall():
        if not row[0] in parent_2_children:
            parent_2_children[row[0]] = []
        parent_2_children[row[0]].append(row[1])
    with open("child_product_2_parent.json", "r+") as f:
        for key, value in json.load(f).items():
            value = int(value)
            if not value in parent_2_children:
                parent_2_children[value] = []
            parent_2_children[value].append(int(key))
    pasdb = Utils.mysqlConnection('w')
    cursor = pasdb.cursor()
    products_count = 0
    for parent, variants in parent_2_children.items():
        if not parent in simple_similar_products_dict:
            continue
        simple_similar_products = list(
            map(lambda e: e[0], sorted(simple_similar_products_dict[parent], key=lambda e: e[1], reverse=True)[:50]))
        direct_similar_products = list(
            map(lambda e: e[0], sorted(direct_similar_products_dict[parent], key=lambda e: e[1], reverse=True)[:50]))
        log_similar_products = list(
            map(lambda e: e[0], sorted(log_similar_products_dict[parent], key=lambda e: e[1], reverse=True)[:50]))
        sqrt_similar_products = list(
            map(lambda e: e[0], sorted(sqrt_similar_products_dict[parent], key=lambda e: e[1], reverse=True)[:50]))

        rows = []
        for variant in variants:
            rows.append((variant, 'product', 'fbt', 'coccurence_simple', str(simple_similar_products)))
            rows.append((variant, 'product', 'fbt', 'coccurence_direct', str(direct_similar_products)))
            rows.append((variant, 'product', 'fbt', 'coccurence_log', str(log_similar_products)))
            rows.append((variant, 'product', 'fbt', 'coccurence_sqrt', str(sqrt_similar_products)))
            products_count += 1

        add_recommendations_in_mysql(pasdb, cursor, rows)

    nykaa_cursor.close()
    nykaadb.close()

    cursor.close()
    pasdb.close()
    print(products_count)

def create_child_2_parent_map():
    query = "select child_id, parent_id from catalog_product_relation"
    rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), query, 50000)
    child_2_parent = {row[0]: row[1] for row in rows}

    with open("child_product_2_parent.json", "r+") as f:
        child_2_parent.update({int(key): int(value) for key, value in json.load(f).items()})

    return child_2_parent

def convert_2_parent_product(data):
    data['product_id'] = data.apply(lambda row: child_2_parent.get(row['product_id'], row['product_id']), axis=1)
    return data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--filename", default='', help = 'csv file name')
    parser.add_argument("-d", "--datasource", default='file', help = 'datasource type to be used')

    argv = vars(parser.parse_args())
    if argv['datasource'] == 'file':
        file_name = argv['filename']
        df = pd.read_csv(file_name)
    else:
        df = pd.DataFrame()

        orders_query = """SELECT order_id, product_id FROM sales_flat_order_item WHERE order_id <> 0 AND mrp > 1;"""
        rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), orders_query, 10000)
        df.append(rows)
        orders_archive_query = """SELECT order_id, product_id FROM sales_flat_order_item_archive WHERE order_id <> 0 AND mrp > 1;"""
        rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), orders_archive_query, 10000)
        df.append(rows)
        df.columns = ['order_id', 'product_id']

        child_2_parent = create_child_2_parent_map()
        df = parallelize_dataframe(df, convert_2_parent_product, 40, 16)

    df['product_count'] = 1
    df = df.groupby(['order_id', 'product_id']).agg({'product_count': 'sum'}).reset_index()
    df.drop(['product_count'], axis=1, inplace=True)

    product_2_orders_cnt_df = df.groupby(['product_id']).agg({'order_id': 'count'}).reset_index()
    product_2_orders_cnt = dict(zip(product_2_orders_cnt_df.product_id, product_2_orders_cnt_df.order_id))

    unique_products = list(df['product_id'].unique())

    final_df = create_coccurence_matrix(df, len(df))
    print('length of final df : %d' % (len(final_df)))

    # threshold of 2 is used
    final_df = final_df[final_df['order_count'] >= 2]

    final_df = parallelize_dataframe(final_df, compute_orders_union_len, 40, 16)

    final_df = parallelize_dataframe(final_df, compute_similarity, 40, 16)

    final_df.rename(index=str, columns={'order_count': 'order_intersection'}, inplace=True)

    # write data in file
    final_df.to_csv('fbt_item_similarity.csv', encoding='utf-8', index=False)

    create_product_mapping(final_df)

    populate_recommendations_in_mysql(unique_products)
