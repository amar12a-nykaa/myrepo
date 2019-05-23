import json
import traceback
import psycopg2
import argparse
import time
import boto3
import os
import sys
import psycopg2
import sys
from collections import defaultdict
from gensim import corpora, models, similarities
from contextlib import closing
import mysql.connector
from elasticsearch import helpers, Elasticsearch
from IPython import embed
import tempfile
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import udf, col, desc
import pyspark.sql.functions as func
from pyspark.sql.window import Window

import sys
sys.path.append("/home/ubuntu/nykaa_scripts/utils")
sys.path.append("/home/hadoop/nykaa_scripts/utils")
from s3utils import S3Utils
import constants as Constants

spark = SparkSession.builder.appName("Gen Top Categories").getOrCreate()
#spark = SparkSession.builder \
#            .master("local[6]") \
#            .appName("TOP 50 CATS") \
#            .config("spark.executor.memory", "4G") \
#            .config("spark.storage.memoryFraction", 0.4) \
#            .config("spark.driver.memory", "26G") \
#            .getOrCreate()

sc = spark.sparkContext
print(sc.getConf().getAll())

s3_client = boto3.client('s3')

ORDER_SOURCE_NYKAA = ['Nykaa', 'Nykaa(Old)', 'NYKAA', 'CS-Manual']
ORDER_SOURCE_NYKAAMEN = ['NykaaMen']

TOP_CATEGORIES_JSON = 'categories_page/top_categories.json'
CATEGORY_2_NAME_JSON = 'categories_page/category_2_name.json'
CATEGORIES_LSI_MODEL_DIR = 'categories_page/models'

class CategoriesUtils:

    @staticmethod
    def _add_categories_in_mysql(cursor, table, rows):
        values_str = ", ".join(["(%s, %s, %s)" for i in range(len(rows))])
        values = tuple([_i for row in rows for _i in row])
        insert_query = """ INSERT INTO %s(algo, user_id, recommended_categories_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_categories_json=VALUES(recommended_categories_json)
        """ % (table, values_str)
        values = tuple([str(_i) for row in rows for _i in row])
        cursor.execute(insert_query, values)

    @staticmethod
    def add_categories_in_mysql(db, table, rows):
        cursor = db.cursor()
        for i in range(0, len(rows), 500):
            CategoriesUtils._add_categories_in_mysql(cursor, table, rows[i:i+500])
            db.commit()

    @staticmethod
    def add_categories_in_ups(rows):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('user_profile_service')
        last_wait_ctr = 0
        for i, row in enumerate(rows):
            table.update_item(Key={'user_id': '%s' % row['customer_id']}, UpdateExpression='SET categories = :val', ExpressionAttributeValues={':val': row['value']})
            if i - last_wait_ctr >= 100000:
                last_wait_ctr = i
                time.sleep(120)

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
        es = Elasticsearch(['http://%s:80' % ES_ENDPOINT], timeout=120)
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

orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_sku", StringType(), True),
        StructField("order_date", TimestampType(), True)])

def prepare_dataframe(env, platform, start_datetime, end_datetime, customer_start_datetime):
    if platform == 'men':
        order_sources = ORDER_SOURCE_NYKAAMEN
    else:
        order_sources = ORDER_SOURCE_NYKAA

    customer_orders_query = "SELECT fact_order_new.nykaa_orderno as order_id, fact_order_new.order_customerid as customer_id, fact_order_detail_new.product_id, fact_order_detail_new.product_sku, order_date from fact_order_new INNER JOIN fact_order_detail_new ON fact_order_new.nykaa_orderno=fact_order_detail_new.nykaa_orderno WHERE fact_order_new.order_status<>'Cancelled' AND fact_order_new.nykaa_orderno <> 0 AND product_mrp > 1 AND order_source IN (" + ",".join([("'%s'" % source) for source in order_sources]) + ") AND order_customerid IS NOT NULL %s %s " % (" AND order_date <= '%s' " % end_datetime if end_datetime else "", " AND order_date >= '%s' " % start_datetime if start_datetime else "")
    if customer_start_datetime:
        customer_orders_query += " AND fact_order_new.order_customerid IN (SELECT DISTINCT(order_customerid) FROM fact_order_new WHERE order_date > '%s')" % customer_start_datetime

    print(customer_orders_query)
    print('Fetching Data from Redshift')
    rows = Utils.fetchResultsInBatch(Utils.redshiftConnection(env), customer_orders_query, 10000)
    print('Data fetched')

    df = spark.createDataFrame(rows, orders_schema)
    print('Total number of rows fetched: %d' % df.count())
    df.printSchema()
    print('Total number of rows extracted: %d' % df.count())
    print('Total number of products: %d' % df.select('product_id').distinct().count())
    print('Scrolling ES for results')
    results = Utils.scrollESForResults(env)
    print('Scrolling ES done')
    product_2_l3_category = {product_id: json.loads(categories[0])['l3']['id'] for product_id, categories in results['primary_categories'].items()}

    l3_udf = udf(lambda product_id: product_2_l3_category.get(product_id), StringType())
    df = df.withColumn('l3_category', l3_udf('product_id'))
    #TODO need to remove the below line
    df = df.filter(col('l3_category') != 'LEVEL')
    df = df.withColumn('l3_category', col('l3_category').cast('int'))
    return df, results

def _generate_top_categories(df, es_scroll_results):
    l3_cat_2_name = {cat_obj['id']: cat_obj['name'] for l in es_scroll_results['primary_categories'].values() for ele in l for cat_type, cat_obj in json.loads(ele).items() if cat_type == 'l3'}
    df = df.select('order_id', 'l3_category').distinct()
    df = df.dropna()
    df = df.groupBy(['l3_category']).agg({'order_id': 'count'}).withColumnRenamed('count(order_id)', 'orders_count').sort(desc('orders_count'))
    l3_name_udf = udf(lambda cat_id: l3_cat_2_name.get(cat_id), StringType())
    df = df.withColumn('name', l3_name_udf('l3_category'))
    top_categories = df.select(['l3_category']).limit(n).rdd.flatMap(lambda x: x).collect()
    s3_client.put_object(Bucket=bucket_name, Key=CATEGORY_2_NAME_JSON, Body=json.dumps(l3_cat_2_name), ContentType='application/json')
    s3_client.put_object(Bucket=bucket_name, Key=TOP_CATEGORIES_JSON, Body=json.dumps(top_categories), ContentType='application/json')
    return df

def generate_top_categories(env, platform, n, start_datetime, end_datetime):
    df, results = prepare_dataframe(env, platform, start_datetime, end_datetime)
    top_categories_df = _generate_top_categories(df, results)
    return top_categories_df

def generate_customer_cat_corpus(df):
    df = df.select(['customer_id', 'order_id', 'l3_category']).distinct().dropna().groupBy(['customer_id', 'l3_category']).agg({'order_id': 'count'}).withColumnRenamed('count(order_id)', 'orders_count')
    df = df.groupBy(['customer_id']).agg(func.collect_list(func.struct(col('l3_category'), col('orders_count'))).alias('l3_category_freq'))
    return df.select(['customer_id', 'l3_category_freq']).rdd.map(lambda row: (row['customer_id'], [[category_count['l3_category'], category_count['orders_count']] for category_count in row['l3_category_freq']])).toDF(['customer_id', 'l3_category_freq'])
    #return {row['customer_id']: row['l3_category_freq'] for row in customer_cat_corpus_rows}

def make_and_save_model(corpus):
    s3 = boto3.client('s3')
    with tempfile.TemporaryDirectory() as temp_dir:
        norm_model = models.NormModel()
        normalized_corpus = []
        print('Normalizing the corpus')
        for doc in corpus:
            normalized_corpus.append(norm_model.normalize(doc))
        print('Making lsi model')
        lsi = models.LsiModel(normalized_corpus, num_topics=200)
        lsi.save(temp_dir + '/model.lsi')
        print('Uploading lsi model onto s3')
        S3Utils.upload_dir(temp_dir, bucket_name, CATEGORIES_LSI_MODEL_DIR)
        return CATEGORIES_LSI_MODEL_DIR

def load_model(s3_path):
    with tempfile.TemporaryDirectory() as temp_dir:
        S3Utils.download_dir(s3_path, s3_path, temp_dir, bucket_name)
        model = models.LsiModel.load(temp_dir + '/model.lsi')
        return model

def generate_category_corpus(categories):
    return [[[category, 1]] for category in categories]

#def generate_top_categories_for_user(env, platform, n, start_datetime, end_datetime):
#    df, results = prepare_dataframe(env, platform, start_datetime, end_datetime) 
#    top_categories_df = _generate_top_categories(df, results)
#    top_categories = top_categories_df.select(['l3_category']).limit(n).rdd.flatMap(lambda x: x).collect()
#    print('Top Categories')
#    print(top_categories)
#    customer_2_cat_corpus_dict = generate_customer_cat_corpus(df)
#    path = make_and_save_model(list(customer_2_cat_corpus_dict.values()))
#    lsi_model = load_model(path)
#    corpus_lsi = lsi_model[generate_category_corpus(top_categories)]
#    idx_2_cat = {key: cat for key, cat in enumerate(top_categories)}
#    index = similarities.MatrixSimilarity(corpus_lsi)
#    sorted_cats = list(map(lambda ele: idx_2_cat[ele[0]], sorted(enumerate(index[lsi_model[customer_2_cat_corpus_dict['1006620']]]), key=lambda e: -e[1])))
#    embed()

def generate_top_categories_for_user(env, platform, start_datetime, end_datetime, top_categories, lsi_model, orders_count):
    df, results = prepare_dataframe(env, platform, None, None, start_datetime) 
    df = df.select(['customer_id', 'order_id', 'l3_category', 'order_date']).distinct().withColumn('rank', func.dense_rank().over(Window.partitionBy('customer_id').orderBy(desc('order_date')))).filter(col('rank') <= orders_count)
    customer_2_last_order_categories = {row['customer_id']: row['categories'] for row in df.filter(col('rank') == 1).select(['customer_id', 'l3_category']).distinct().groupBy(['customer_id']).agg(func.collect_list('l3_category').alias('categories')).collect()}
    df = df.select(['customer_id', 'l3_category', 'order_id']).distinct()
    df = generate_customer_cat_corpus(df)
    idx_2_cat = {key: cat for key, cat in enumerate(top_categories)}
    corpus_lsi = lsi_model[generate_category_corpus(top_categories)]
    index = similarities.MatrixSimilarity(corpus_lsi)
    norm_model = models.NormModel()
    sorted_cats_udf = udf(lambda customer_id, user_cat_doc: list(filter(lambda x: x not in customer_2_last_order_categories[customer_id], map(lambda ele: idx_2_cat[ele[0]], sorted(enumerate(index[lsi_model[norm_model.normalize(user_cat_doc)]]), key=lambda e: -e[1])))) + customer_2_last_order_categories[customer_id], ArrayType(IntegerType()))
    df = df.withColumn('sorted_cats', sorted_cats_udf('customer_id', 'l3_category_freq'))
    #rows = [('lsi', row['customer_id'], row['sorted_cats']) for row in df.collect()]
    rows = [{'customer_id': row['customer_id'], 'value': {'lsi': row['sorted_cats']}} for row in df.collect()]
    #rows = [('lsi', row['customer_id'], row['sorted_cats']) for row in df.collect()]
    CategoriesUtils.add_categories_in_ups(rows)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gen-top-cats', action='store_true')
    parser.add_argument('--prepare-model', action='store_true')
    parser.add_argument('--gen-user-categories', action='store_true')
    parser.add_argument('--start-datetime')
    parser.add_argument('--end-datetime')
    parser.add_argument('--use-months', type=int, default=6)
    parser.add_argument('--hours', type=int)
    parser.add_argument('--days', type=int)
    parser.add_argument('--orders-count', type=int, default=10)
    parser.add_argument('--platform', required=True, choices=['nykaa','men'])
    parser.add_argument('--env', required=True)
    parser.add_argument('--bucket-name', '-b', required=True) 
    parser.add_argument('-n', '--n', default=50, type=int)
    
    argv = vars(parser.parse_args())
    do_gen_top_cats = argv.get('gen_top_cats')
    do_prepare_model = argv.get('prepare_model')
    gen_user_categories = argv.get('gen_user_categories')
    start_datetime = argv.get('start_datetime')
    if not start_datetime and (argv.get('hours') or argv.get('days')):
        start_datetime = datetime.now()
        if argv.get('hours'):
            start_datetime -= timedelta(hours=argv['hours'])
        if argv.get('days'):
            start_datetime -= timedelta(days=argv['days'])
    end_datetime = argv.get('end_datetime')
    use_months = argv.get('use_months')
    platform = argv.get('platform')
    env = argv.get('env')
    n = argv.get('n')
    orders_count = argv.get('orders_count')
    bucket_name = argv['bucket_name']

    computation_start_datetime = datetime.now() - timedelta(days=use_months*30)
    if do_gen_top_cats | do_prepare_model:
        df, results = prepare_dataframe(env, platform, str(computation_start_datetime), None, None) 
        if do_gen_top_cats:
            _generate_top_categories(df, results)
        if do_prepare_model:
            customer_2_cat_corpus_dict = {row['customer_id']: row['l3_category_freq'] for row in generate_customer_cat_corpus(df).collect()}
            make_and_save_model(list(customer_2_cat_corpus_dict.values()))

    if gen_user_categories:
        #json.loads(s3_client.get_object(Bucket='nykaa-dev-recommendations', Key='cool.json')['Body'].read().decode('utf-8'))
        print('Loading top categories')
        top_categories = json.loads(s3_client.get_object(Key=TOP_CATEGORIES_JSON, Bucket=bucket_name)['Body'].read().decode('utf-8'))
        print('Loading LSI Model')
        lsi_model = load_model(CATEGORIES_LSI_MODEL_DIR)
        print('Generate top categories for user')
        generate_top_categories_for_user(env, platform, start_datetime, end_datetime, top_categories, lsi_model, orders_count)
    #generate_top_categories_for_user(env, platform, n, prepare_model, start_datetime, end_datetime)
    #top_categories_df = generate_top_categories(env, platform, n, start_datetime, end_datetime)
    #top_categories_df.show(n, False)
