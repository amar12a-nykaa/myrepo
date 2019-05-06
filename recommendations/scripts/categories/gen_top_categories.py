import json
import traceback
import psycopg2
import argparse
import os
import sys
import psycopg2
import sys
from collections import defaultdict
from contextlib import closing
import mysql.connector
from elasticsearch import helpers, Elasticsearch

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, BooleanType
from pyspark.sql.functions import udf, col, desc
import pyspark.sql.functions as func
from pyspark.sql.window import Window

#spark = SparkSession.builder.appName("U2P").getOrCreate()
spark = SparkSession.builder \
            .master("local[6]") \
            .appName("TOP 50 CATS") \
            .config("spark.executor.memory", "4G") \
            .config("spark.storage.memoryFraction", 0.4) \
            .config("spark.driver.memory", "26G") \
            .getOrCreate()

sc = spark.sparkContext
print(sc.getConf().getAll())

ORDER_SOURCE_NYKAA = ['Nykaa', 'Nykaa(Old)', 'NYKAA', 'CS-Manual']
ORDER_SOURCE_NYKAAMEN = ['NykaaMen']

class Utils:

    @staticmethod
    def mysqlConnection(env, connection_details=None):
        if env == 'prod':
            host = "dbmaster.ciel4c1bqlwh.ap-southeast-1.rds.amazonaws.com"
            password = 'Cheaj92pDHtDq8hU'
        elif env in ['non_prod', 'preprod']:
            host = 'price-api-preprod.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
            password = 'yNKNy33xG'
        elif env == 'qa':
            host = 'price-api-qa.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
            password = 'yNKNy33xG'
        else:
            raise Exception('Unknow env')
        user = 'recommendation'
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
    def mlMysqlConnection(env, connection_details=None):
        if env == 'prod':
            host = "ml-db-master.nykaa-internal.com"
            password = 'fjxcneXnq2gptsTs'
        elif env in ['non_prod', 'preprod', 'qa']:
            host = 'nka-preprod-ml.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
            password = 'JKaPHGB4JWXM'
        else:
            raise Exception('Unknow env')
        user = 'ml'
        db = 'nykaa_ml'
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
                response = es_conn.search(index='livecore', body=query, scroll='15m')
            else:
                response = es_conn.scroll(scroll_id=scroll_id, scroll='15m')

            if not response['hits']['hits']:
                break

            scroll_id = response['_scroll_id']
            luxe_products += [int(p['_source']['product_id']) for p in response['hits']['hits'] if p["_source"].get("is_luxe") and p["_source"]["is_luxe"]]
            product_2_mrp.update({int(p["_source"]["product_id"]): p["_source"]["mrp"] for p in response["hits"]["hits"] if p["_source"].get("mrp")})
            child_2_parent.update({int(p["_source"]["product_id"]): int(p["_source"]["parent_id"]) for p in response["hits"]["hits"] if p["_source"].get("parent_id")})
            primary_categories.update({int(p["_source"]["product_id"]): p["_source"]["primary_categories"] for p in response["hits"]["hits"] if p["_source"].get("primary_categories")})
            brand_facets.update({int(p["_source"]["product_id"]): p["_source"].get("brand_facet") for p in response["hits"]["hits"] if p["_source"].get("brand_facet")})
            sku_2_product_id.update({p["_source"]["sku"]: int(p["_source"]["product_id"]) for p in response["hits"]["hits"] if p["_source"].get("sku")})
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

def generate_top_categories(env, platform, n, start_datetime, end_datetime):
    if platform == 'men':
        order_sources = ORDER_SOURCE_NYKAAMEN
    else:
        order_sources = ORDER_SOURCE_NYKAA

    customer_orders_query = "SELECT fact_order_new.nykaa_orderno as order_id, fact_order_new.order_customerid as customer_id, fact_order_detail_new.product_id, fact_order_detail_new.product_sku, order_date from fact_order_new INNER JOIN fact_order_detail_new ON fact_order_new.nykaa_orderno=fact_order_detail_new.nykaa_orderno WHERE fact_order_new.order_status<>'Cancelled' AND fact_order_new.nykaa_orderno <> 0 AND product_mrp > 1 AND order_source IN (" + ",".join([("'%s'" % source) for source in order_sources]) + ") AND order_customerid IS NOT NULL %s %s " % (" AND order_date <= '%s' " % end_datetime if end_datetime else "", " AND order_date >= '%s' " % start_datetime if start_datetime else "")

    print(customer_orders_query)
    print('Fetching Data from Redshift')
    rows = Utils.fetchResultsInBatch(Utils.redshiftConnection(env), customer_orders_query, 10000)
    print('Data fetched')

    schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("product_sku", StringType(), True),
            StructField("order_date", TimestampType(), True)])

    df = spark.createDataFrame(rows, schema)
    print('Total number of rows fetched: %d' % df.count())
    df.printSchema()
    print('Total number of rows extracted: %d' % df.count())
    print('Total number of products: %d' % df.select('product_id').distinct().count())
    print('Scrolling ES for results')
    results = Utils.scrollESForResults(env)
    print('Scrolling ES done')
    product_2_l3_category = {product_id: json.loads(categories[0])['l3']['id'] for product_id, categories in results['primary_categories'].items()}
    l3_cat_2_name = {cat_obj['id']: cat_obj['name'] for l in results['primary_categories'].values() for ele in l for cat_type, cat_obj in json.loads(ele).items() if cat_type == 'l3'}

    l3_udf = udf(lambda product_id: product_2_l3_category.get(product_id), StringType())
    df = df.withColumn('l3_category', l3_udf('product_id'))
    df = df.select('order_id', 'l3_category').distinct()
    df = df.dropna()
    df = df.groupBy(['l3_category']).agg({'order_id': 'count'}).withColumnRenamed('count(order_id)', 'orders_count').sort(desc('orders_count'))
    l3_name_udf = udf(lambda cat_id: l3_cat_2_name.get(cat_id), StringType())
    df = df.withColumn('name', l3_name_udf('l3_category'))
    df.show(n, False)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-datetime')
    parser.add_argument('--end-datetime')
    parser.add_argument('--platform', required=True, choices=['nykaa','men'])
    parser.add_argument('--env', required=True)
    parser.add_argument('-n', '--n', default=50, type=int)
    
    argv = vars(parser.parse_args())
    start_datetime = argv.get('start_datetime')
    end_datetime = argv.get('end_datetime')
    platform = argv.get('platform')
    env = argv.get('env')
    n = argv.get('n')

    generate_top_categories(env, platform, n, start_datetime, end_datetime)
