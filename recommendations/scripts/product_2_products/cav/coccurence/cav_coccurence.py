import os
import sys
import psycopg2
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import concat, col, lit, udf, isnan
from contextlib import closing
import math
import boto3
import json
import pandas
from collections import defaultdict
import mysql.connector
import argparse
from elasticsearch import helpers, Elasticsearch

class RecommendationsUtils:

    @staticmethod
    def _add_recommendations_in_mysql(cursor, table, rows):
        values_str = ", ".join(["(%s, %s, %s, %s, %s)" for i in range(len(rows))])
        values = tuple([_i for row in rows for _i in row])
        insert_recommendations_query = """ INSERT INTO %s(entity_id, entity_type, recommendation_type, algo, recommended_products_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json)
        """ % (table, values_str)
        values = tuple([str(_i) for row in rows for _i in row])
        cursor.execute(insert_recommendations_query, values)

    @staticmethod
    def add_recommendations_in_mysql(db, table, rows):
        cursor = db.cursor()
        for i in range(0, len(rows), 100):
            RecommendationsUtils._add_recommendations_in_mysql(cursor, table, rows[i:i+100])
            db.commit()

class Utils:

    @staticmethod
    def mysqlConnection(env, connection_details=None):
        if env == 'prod':
            host = "dbmaster.ciel4c1bqlwh.ap-southeast-1.rds.amazonaws.com"
        elif env in ['non_prod', 'preprod']:
            host = 'price-api-preprod.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
        elif env == 'qa':
            host = 'price-api-qa.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
        else:
            raise Exception('Unknow env')
        user = 'recommendation'
        password = 'yNKNy33xG'
        #user = 'api'
        #password = 'aU%v#sq1'
        db = 'nykaa'
        for i in [0, 1, 2]:
            try:
                if connection_details is not None and isinstance(connection_details, dict):
                    connection_details['host'] = host
                    connection_details['user'] = user
                    connection_details['password'] = password
                    connection_details['database'] = db
                return mysql.connector.connect(host=host, user=user, password=password, database=db)
            except:
                print("MySQL connection failed! Retyring %d.." % i)
                if i == 2:
                    print(traceback.format_exc())
                    print("MySQL connection failed 3 times. Giving up..")
                    raise


    @staticmethod
    def esConn(env):
        if env == 'prod':
            ES_ENDPOINT = 'vpc-prod-api-vzcc4i4e4zk2w4z45mqkisjo4u.ap-southeast-1.es.amazonaws.com'
        elif env in ['non_prod', 'preprod']:
            ES_ENDPOINT = 'search-preprod-api-ub7noqs5xxaerxm6vhv5yjuc7u.ap-southeast-1.es.amazonaws.com'
        elif env == 'qa':
            ES_ENDPOINT = 'search-qa-api-fvmcnxoaknewsdvt6gxgdtmodq.ap-southeast-1.es.amazonaws.com'
        else:
            raise Exception('Unknown env')
        print(ES_ENDPOINT)
        es = Elasticsearch(['http://%s:80' % ES_ENDPOINT])
        return es

    @staticmethod
    def scrollESForResults(env):
        es_conn = Utils.esConn(env)
        ES_BATCH_SIZE = 10000
        scroll_id = None
        luxe_products = []
        product_2_mrp = {}
        child_2_parent = {}
        primary_categories = {}
        brand_facets = {}
        sku_2_product_id = {}
        product_2_image = {}

        while True:
            if not scroll_id:
                query = { 
                    "size": ES_BATCH_SIZE,
                    "query": { "match_all": {} },
                    "_source": ["product_id", "is_luxe", "mrp", "parent_id", "primary_categories", "brand_facet", "sku", "media"]
                }
                response = es_conn.search(index='livecore', body=query, scroll='2m')
            else:
                response = es_conn.scroll(scroll_id=scroll_id, scroll='2m')

            if not response['hits']['hits']:
                break

            scroll_id = response['_scroll_id']
            luxe_products += [int(p['_source']['product_id']) for p in response['hits']['hits'] if p["_source"]["is_luxe"]]
            product_2_mrp.update({int(p["_source"]["product_id"]): p["_source"]["mrp"] for p in response["hits"]["hits"]})
            child_2_parent.update({int(p["_source"]["product_id"]): int(p["_source"]["parent_id"]) for p in response["hits"]["hits"]})
            primary_categories.update({int(p["_source"]["product_id"]): p["_source"]["primary_categories"] for p in response["hits"]["hits"]})
            brand_facets.update({int(p["_source"]["product_id"]): p["_source"].get("brand_facet") for p in response["hits"]["hits"] if p["_source"].get("brand_facet")})
            sku_2_product_id.update({p["_source"]["sku"]: int(p["_source"]["product_id"]) for p in response["hits"]["hits"]})
            product_2_image.update({int(p["_source"]["product_id"]): p['_source']['media'] for p in response['hits']['hits'] if p['_source'].get('media')})

        return {'luxe_products': luxe_products, 'product_2_mrp': product_2_mrp, 'child_2_parent': child_2_parent, 'primary_categories': primary_categories, 'brand_facets': brand_facets, 'sku_2_product_id': sku_2_product_id, 'product_2_image': product_2_image}


    @staticmethod
    def redshiftConnection(env):
        if env == 'prod':
            host = 'dwhcluster.cy0qwrxs0juz.ap-southeast-1.redshift.amazonaws.com'
        elif env in ['non_prod', 'preprod', 'qa']:
            host = 'nka-preprod-dwhcluster.c742iibw9j1g.ap-southeast-1.redshift.amazonaws.com'
        else:
            raise Exception('Unknown env')
        port = 5439
        username = 'dwh_redshift_ro'
        password = 'GSrjC7hYPC9V'
        dbname = 'datawarehouse'
        con = psycopg2.connect(dbname=dbname, host=host, port=port, user=username, password=password)
        return con

    @staticmethod
    def fetchResultsInBatch(connection, query, batch_size):
        rows= []
        with closing(connection.cursor()) as cursor:
            cursor.execute(query)
            while True:
                batch_empty = True
                for row in cursor.fetchmany(batch_size):
                    batch_empty = False
                    rows.append(row)
                if batch_empty:
                    break
        return rows

def prepare_data(files, desktop):
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Visitor_ID", StringType(), True),
        StructField("Visit Number", IntegerType(), True),
        StructField("Products", IntegerType(), True),
        StructField("Product Views", IntegerType(), True)])

    df = spark.read.load(files[0], header=True, format='csv', schema=schema)

    for i in range(1, len(files)):
        df = df.union(spark.read.load(files[i], header=True, format='csv', schema=schema))
    df = df.cache()
    print("Rows count: " + str(df.count()))

    print('Cleaning out null or empty Visitor_ID and Visit Number')
    df = df.filter((df["Visitor_ID"] != "") & df["Visitor_ID"].isNotNull() & df["Visit Number"].isNotNull() & ~isnan(df["Visit Number"]))
    print("Rows count: " + str(df.count()))

    df = df.withColumn('session_id', concat(col("Visitor_ID"), lit("_"), col("Visit Number")))

    print('Cleaning out null products and zero product views')
    df = df.filter(df['Products'].isNotNull() & (df['Product Views'] > 0))
    print("Rows count: " + str(df.count()))
    df = df.withColumnRenamed('Products', 'product_id')
    print('Only taking distinct product_id and session_id')
    df = df.select(['product_id', 'session_id']).distinct()
    print("Rows count: " + str(df.count()))

    print('Scrolling ES for results')
    results = Utils.scrollESForResults(env)
    print('Scrolling ES done')
    child_2_parent = results['child_2_parent']

    def convert_to_parent(product_id):
        return child_2_parent.get(product_id, product_id)

    convert_to_parent_udf = udf(convert_to_parent, IntegerType())
    print('Converting product_id to parent')
    df = df.withColumn("product_id", convert_to_parent_udf(df['product_id']))
    df = df.na.drop()
    df = df.select(['product_id', 'session_id']).distinct()
    print("Distinct Product counts: " + str(df.select('product_id').distinct().count()))
    print('Data preparation done, returning dataframe')
    return df, results

def compute_cav(files, desktop):
    df, results = prepare_data(files, desktop)

    print("Self joining the dataframe: Computing all the non product to product pairs")
    merged_df = df.withColumnRenamed('product_id', 'product_id_x').join(df.withColumnRenamed('product_id', 'product_id_y'), on="session_id", how="inner")

    print("Product Pairs count: " + str(merged_df.count()))

    print("Filtering out duplicate pairs")
    merged_df = merged_df[merged_df.product_id_x < merged_df.product_id_y]
    print("Product Pairs count: " + str(merged_df.count()))

    print("Computing sessions for product pairs")
    merged_df = merged_df.groupBy(['product_id_x', 'product_id_y']).agg({'session_id': 'count'})
    print("Product pairs count: " + str(merged_df.count()))

    final_df = merged_df.withColumnRenamed("count(session_id)", 'sessions_intersection')

    del df

    print("Filtering out all the product pairs with a threshold of atleast 2 sessions")
    final_df = final_df[final_df['sessions_intersection'] >= 2]
    print("Product Pairs count: " + str(final_df.count()))

    simple_similar_products_dict = defaultdict(lambda: [])

    simple_similar_products_mrp_cons_dict = defaultdict(lambda: [])

    luxe_dict = {p: True for p in results['luxe_products']}

    product_2_mrp = results['product_2_mrp']

    for row in final_df.collect():
        if not (luxe_dict.get(row['product_id_x'], False) ^ luxe_dict.get(row['product_id_y'], False)):
            simple_similar_products_dict[row['product_id_x']].append((row['product_id_y'], row['sessions_intersection']))
            simple_similar_products_dict[row['product_id_y']].append((row['product_id_x'], row['sessions_intersection']))

            if product_2_mrp.get(str(row['product_id_x'])) and product_2_mrp.get(str(row['product_id_y'])):
                product_x_mrp = product_2_mrp[str(row['product_id_x'])]
                product_y_mrp = product_2_mrp[str(row['product_id_y'])]
                if abs((product_x_mrp - product_y_mrp)/product_x_mrp) <= 0.3 or abs((product_x_mrp - product_y_mrp)/product_y_mrp) <= 0.3:
                    simple_similar_products_mrp_cons_dict[row['product_id_x']].append((row['product_id_y'], row['sessions_intersection']))
                    simple_similar_products_mrp_cons_dict[row['product_id_y']].append((row['product_id_x'], row['sessions_intersection']))

    parent_2_children = defaultdict(lambda: [])
    for child, parent in results['child_2_parent'].items():
        parent_2_children[parent].append(child)

    print('Total Number of parent products: %d' % len(parent_2_children))
    print('Total Number of child products: %d' % sum([len(variants) for parent, variants in parent_2_children.items()]))

    rows = []
    product_ids_updated = []
    for product_id in simple_similar_products_dict:
        product_ids_updated.append(product_id)
        simple_similar_products = list(map(lambda e: int(e[0]), sorted(simple_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        if desktop:
            rows.append((product_id, 'product', 'viewed', 'coccurence_simple_desktop', json.dumps(simple_similar_products)))
        else:
            rows.append((product_id, 'product', 'viewed', 'coccurence_simple', json.dumps(simple_similar_products)))
        variants = parent_2_children.get(product_id, [])
        for variant in variants:
            product_ids_updated.append(variant)
            if desktop:
                rows.append((variant, 'product', 'viewed', 'coccurence_simple_desktop', str(simple_similar_products)))
            else:
                rows.append((variant, 'product', 'viewed', 'coccurence_simple', str(simple_similar_products)))

    print('Adding recommendations for %d products with algo=coccurence_simple in DB' % len(set(product_ids_updated)))

    product_ids_updated = []
    for product_id in simple_similar_products_mrp_cons_dict:
        product_ids_updated.append(product_id)
        simple_similar_products = list(map(lambda e: e[0], sorted(simple_similar_products_mrp_cons_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        if desktop:
            rows.append((product_id, 'product', 'viewed', 'coccurence_simple_mrp_cons_desktop', str(simple_similar_products)))
        else:
            rows.append((product_id, 'product', 'viewed', 'coccurence_simple_mrp_cons', str(simple_similar_products)))
        variants = parent_2_children.get(product_id, [])
        for variant in variants:
            product_ids_updated.append(variant)
            if desktop:
                rows.append((variant, 'product', 'viewed', 'coccurence_simple_mrp_cons_desktop', str(simple_similar_products)))
            else:
                rows.append((variant, 'product', 'viewed', 'coccurence_simple_mrp_cons', str(simple_similar_products)))

    print('Adding recommendations for %d products with algo=coccurence_simple_mrp_cons in DB' % len(set(product_ids_updated)))

    RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection(env), 'recommendations_v2', rows)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for CAV script')
    parser.add_argument('--desktop', action='store_true')
    parser.add_argument('--files', nargs='+')
    parser.add_argument('--env', required=True)

    argv = vars(parser.parse_args())
    files = argv['files']
    desktop = argv.get('desktop')
    env = argv.get('env')

    s3 = boto3.client('s3')

    spark = SparkSession.builder.appName("CAV").getOrCreate()
    sc = spark.sparkContext

    print("Printing Configurations:")
    print(sc.getConf().getAll())

    compute_cav(files, desktop)
