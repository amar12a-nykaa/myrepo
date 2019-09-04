import json
import traceback
import psycopg2
import argparse
import time
import boto3
import os
import sys
import glob
import psycopg2
import sys
from collections import defaultdict
from gensim import corpora, models, similarities
from contextlib import closing
import mysql.connector
from elasticsearch import helpers, Elasticsearch
from IPython import embed
import tempfile
from datetime import datetime, timedelta, date

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, BooleanType, ArrayType
from pyspark.sql.functions import udf, col, desc
import pyspark.sql.functions as func
from pyspark.sql.window import Window

import sys
sys.path.append("/home/ubuntu/nykaa_scripts/utils")
sys.path.append("/home/hadoop/nykaa_scripts/utils")
from s3utils import S3Utils
import constants as Constants
from recoutils import RecoUtils
from datautils import DataUtils
from mysqlredshiftutils import MysqlRedshiftUtils
from sparkutils import SparkUtils
from ftputils import FTPUtils
from esutils import ESUtils
from upsutils import UPSUtils

spark = SparkUtils.get_spark_instance('Gen Top Categories')

sc = spark.sparkContext
print(sc.getConf().getAll())

s3_client = boto3.client('s3')

ORDER_SOURCE_NYKAA = ['Nykaa', 'Nykaa(Old)', 'NYKAA', 'CS-Manual']
ORDER_SOURCE_NYKAAMEN = ['NykaaMen']

CAT_PAGE_TOP_CATEGORIES_JSON = 'categories_page/top_categories.json'
CAT_PAGE_CATEGORY_2_NAME_JSON = 'categories_page/category_2_name.json'
CAT_PAGE_CATEGORIES_LSI_MODEL_DIR = 'categories_page/models'

SEARCH_BAR_TOP_CATEGORIES_JSON = 'search_bar/categories/top_categories.json'
SEARCH_BAR_CATEGORY_2_NAME_JSON = 'search_bar/categories/category_2_name.json'
SEARCH_BAR_CATEGORIES_LSI_MODEL_DIR = 'search_bar/categories/models/lsi_views'

DAILY_SYNC_FILE_PREFIX= 'Nykaa_CustomerInteractions_'
VIEWS_CA_S3_PREFIX = 'categories_page/data/'

def _generate_top_categories(df, es_scroll_results, bucket_name):
    l3_cat_2_name = {cat_obj['id']: cat_obj['name'] for l in es_scroll_results['primary_categories'].values() for ele in l for cat_type, cat_obj in json.loads(ele).items() if cat_type == 'l3'}
    df = df.select('order_id', 'l3_category').distinct()
    df = df.dropna()
    df = df.groupBy(['l3_category']).agg({'order_id': 'count'}).withColumnRenamed('count(order_id)', 'orders_count').sort(desc('orders_count'))
    l3_name_udf = udf(lambda cat_id: l3_cat_2_name.get(cat_id), StringType())
    df = df.withColumn('name', l3_name_udf('l3_category'))
    top_categories = df.select(['l3_category']).limit(n).rdd.flatMap(lambda x: x).collect()
    s3_client.put_object(Bucket=bucket_name, Key=category_2_name_json, Body=json.dumps(l3_cat_2_name), ContentType='application/json')
    s3_client.put_object(Bucket=bucket_name, Key=top_categories_json, Body=json.dumps(top_categories), ContentType='application/json')
    return df

def generate_top_categories(platform, bucket_name, n, start_datetime, end_datetime):
    df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, start_datetime, end_datetime)
    top_categories_df = _generate_top_categories(df, results, bucket_name)
    return top_categories_df

def generate_customer_cat_corpus(df):
    df = df.select(['customer_id', 'order_id', 'l3_category']).distinct().dropna().groupBy(['customer_id', 'l3_category']).agg({'order_id': 'count'}).withColumnRenamed('count(order_id)', 'orders_count')
    df = df.groupBy(['customer_id']).agg(func.collect_list(func.struct(col('l3_category'), col('orders_count'))).alias('l3_category_freq'))
    return df.select(['customer_id', 'l3_category_freq']).rdd.map(lambda row: (row['customer_id'], [[category_count['l3_category'], category_count['orders_count']] for category_count in row['l3_category_freq']])).toDF(['customer_id', 'l3_category_freq'])
    #return {row['customer_id']: row['l3_category_freq'] for row in customer_cat_corpus_rows}

def make_and_save_model(bucket_name, corpus):
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
        S3Utils.upload_dir(temp_dir, bucket_name, categories_lsi_model_dir)
        return categories_lsi_model_dir

def load_model(bucket_name, s3_path):
    with tempfile.TemporaryDirectory() as temp_dir:
        S3Utils.download_dir(s3_path, s3_path, temp_dir, bucket_name)
        model = models.LsiModel.load(temp_dir + '/model.lsi')
        return model

def generate_category_corpus(categories):
    return [[[category, 1]] for category in categories]

def generate_top_categories_for_user(platform, start_datetime, end_datetime, top_categories, lsi_model, orders_count):
    df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, None, None, start_datetime) 
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
    print("Writing the categories recommendations " + str(datetime.now()))
    print("Total number of customers to be updated: %d" % len(rows))
    print("Few users getting updated are listed below")
    print([row['customer_id'] for row in rows[:10]])
    #CategoriesUtils.add_categories_in_ups(rows)
    UPSUtils.add_recommendations_in_ups(ups_field_name, rows)
    print("Done Writing the categories recommendations " + str(datetime.now()))

def prepare_views_ca_dataframe(files):
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Customer ID (evar23)", StringType(), True),
        StructField("Products", IntegerType(), True),
        StructField("Product Views", IntegerType(), True),
        StructField("Cart Additions", IntegerType(), True)])

    if not env_details['is_emr']:
        S3Utils.download_dir(VIEWS_CA_S3_PREFIX, VIEWS_CA_S3_PREFIX, '/data/categories_page/data/', env_details['bucket_name'])
        files = [f for f in glob.glob('~/categories_page/data/' + "**/*.csv", recursive=True)]

    print("Using files: " + str(files))
    df = spark.read.load(files[0], header=True, format='csv', schema=schema)
    for i in range(1, len(files)):
        df = df.union(spark.read.load(files[i], header=True, format='csv', schema=schema))

    df = df.withColumnRenamed("Date", "date").withColumnRenamed("Customer ID (evar23)", "customer_id").withColumnRenamed("Products", "product_id").withColumnRenamed("Product Views", "views").withColumnRenamed("Cart Additions", "cart_additions")

    print("Total Number of rows: %d" % df.count())
    print("Dropping nulls")
    df = df.dropna()
    print("Total Number of rows now: %d" % df.count())

    print("Filtering out junk data")
    df = df.filter((col('cart_additions') <= 5) & (col('views') <= 5))
    print("Total Number of rows now: %d" % df.count())

    print('Scrolling ES for results')
    results = ESUtils.scrollESForResults()
    print('Scrolling ES done')

    product_2_l3_category = {product_id: json.loads(categories[0])['l3']['id'] for product_id, categories in results['primary_categories'].items()}
    l3_udf = udf(lambda product_id: product_2_l3_category.get(product_id), StringType())
    df = df.withColumn('l3_category', l3_udf('product_id'))
    df = df.filter(col('l3_category') != 'LEVEL')
    df = df.withColumn('l3_category', col('l3_category').cast('int'))

    print("Dropping nulls after l3 category addition for products")
    df = df.dropna()
    print("Total Number of rows now: %d" % df.count())

    df = df.withColumn("date", udf(lambda d: datetime.strptime(d, '%B %d, %Y'), DateType())(col('date')))

    print("Dropping nulls after changing the type of date")
    df = df.dropna()
    print("Total Number of rows now: %d" % df.count())
    return df

def generate_top_categories_from_views(top_categories, lsi_model, days=None, limit=None):
    FTPUtils.sync_ftp_data(DAILY_SYNC_FILE_PREFIX, env_details['bucket_name'], VIEWS_CA_S3_PREFIX, [])
    csvs_path = S3Utils.ls_file_paths(env_details['bucket_name'], VIEWS_CA_S3_PREFIX, True)
    csvs_path = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")).days <= 31 , csvs_path))
    print(csvs_path)
    #update_csvs_path = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")) >= (datetime.now() + timedelta(hours=5, minutes=30) - timedelta(hours=24)).date() , csvs_path))
    df = prepare_views_ca_dataframe(csvs_path)
    customer_ids_need_update = [] 
    if days:
        customer_ids_need_update = df.filter(col('date') >= (datetime.now() + timedelta(hours=5, minutes=30) - timedelta(hours=24*days)).date()).select(["customer_id"]).distinct().rdd.flatMap(lambda x: x).collect()
        if limit:
            customer_ids_need_update = customer_ids_need_update[:limit]
        print("Only taking customer_ids which need to be updated")
        print("Total number of customer_id=%d" % len(customer_ids_need_update))
        df = df.filter(col('customer_id').isin(customer_ids_need_update))
        print("Total Number of rows now: %d" % df.count())
        

    # TODO need to take the offset of timezone
    def calculate_weight(row_date, views, cart_additions):
        weight = views*0.5 + cart_additions*0.5
        #row_date = datetime.strptime(row_date, '%Y-%m-%d').date()
        today = date.today()
        if (today - row_date).days <= 7:
            return 2*weight
        elif (today - row_date).days <= 14:
            return 1.5*weight
        else:
            return weight

    print("Dropping duplicates on date, customer_id, product_id")
    df = df.dropDuplicates(subset=['date', 'customer_id', 'product_id'])
    print("Total Number of rows now: %d" % df.count())

    #TODO need to add code for cleaning the data such as views and cart additions are more than 100 etc, need to remove duplicates 
    df = df.withColumn('weight', udf(calculate_weight, FloatType())(col('date'), col('views'), col('cart_additions')))
    df = df.filter(col('weight') != 0.0)
    df = df.groupBy(['customer_id', 'l3_category']).agg(func.sum(col('weight')).alias('weight'))
    df = df.groupBy(['customer_id']).agg(func.collect_list(func.struct('l3_category', 'weight')).alias('vec'))

    views_rdd = df.select(['customer_id', 'vec']).rdd.map(lambda row: Row(customer_id=row['customer_id'], vec=[[category_count['l3_category'], category_count['weight']] for category_count in row['vec']]))
    #df = df.select(['customer_id', 'vec']).rdd.map(lambda row: (row['customer_id'], [[category_count['l3_category'], category_count['weight']] for category_count in row['vec']])).toDF(['customer_id', 'vec'])

    # Preparing customer_2_last_order_categories
    if len(customer_ids_need_update) <= 5000:
        orders_df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, None, None, None, customer_ids_need_update) 
    else:
        orders_df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, None, None, None, []) 

    orders_df = orders_df.select(['customer_id', 'order_id', 'l3_category', 'order_date']).distinct().withColumn('rank', func.dense_rank().over(Window.partitionBy('customer_id').orderBy(desc('order_date')))).filter(col('rank') <= 1)
    customer_2_last_order_categories = {int(row['customer_id']): row['categories'] for row in orders_df.select(['customer_id', 'l3_category']).distinct().groupBy(['customer_id']).agg(func.collect_list('l3_category').alias('categories')).collect()}


    idx_2_cat = {key: cat for key, cat in enumerate(top_categories)}
    top_cats_corpus_lsi = lsi_model[generate_category_corpus(top_categories)]
    index = similarities.MatrixSimilarity(top_cats_corpus_lsi)
    norm_model = models.NormModel()
    sorted_cats_rdd = views_rdd.map(lambda row: Row(customer_id=row['customer_id'], vec=row['vec'], sorted_cats=list(filter(lambda x: x not in customer_2_last_order_categories.get(row['customer_id'], []), map(lambda ele: idx_2_cat[ele[0]], sorted(enumerate(index[lsi_model[norm_model.normalize(row['vec'])]]), key=lambda e: -e[1])))) + customer_2_last_order_categories.get(row['customer_id'], [])))
    #sorted_cats_udf = udf(lambda customer_id, user_cat_doc: list(filter(lambda x: x not in customer_2_last_order_categories[customer_id], map(lambda ele: idx_2_cat[ele[0]], sorted(enumerate(index[lsi_model[norm_model.normalize(user_cat_doc)]]), key=lambda e: -e[1])))) + customer_2_last_order_categories[customer_id], ArrayType(IntegerType()))
    rows = [{'customer_id': row['customer_id'], 'value': {'lsi_views': row['sorted_cats']}} for row in sorted_cats_rdd.collect()]
    print("Writing the categories recommendations " + str(datetime.now()))
    print("Total number of customers to be updated: %d" % len(rows))
    print("Few users getting updated are listed below")
    print([row['customer_id'] for row in rows[:10]])
    #CategoriesUtils.add_categories_in_ups(rows)
    UPSUtils.add_recommendations_in_ups(ups_field_name, rows)
    print("Done Writing the categories recommendations " + str(datetime.now()))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gen-top-cats', action='store_true')
    parser.add_argument('--prepare-model', action='store_true')
    parser.add_argument('--gen-user-categories', action='store_true')
    parser.add_argument('--views', action='store_true')
    parser.add_argument('--start-datetime')
    parser.add_argument('--end-datetime')
    parser.add_argument('--use-months', type=int, default=6)
    parser.add_argument('--hours', type=int)
    parser.add_argument('--days', type=int)
    parser.add_argument('--orders-count', type=int, default=10)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--platform', default='nykaa', choices=['nykaa','men'])
    parser.add_argument('--target-widget', default='categories', choices=['categories','search_bar'])
    parser.add_argument('-n', '--n', default=20, type=int)
    
    argv = vars(parser.parse_args())
    do_gen_top_cats = argv.get('gen_top_cats')
    do_prepare_model = argv.get('prepare_model')
    gen_user_categories = argv.get('gen_user_categories')
    views = argv.get('views')
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
    n = argv.get('n')
    orders_count = argv.get('orders_count')
    target_widget = argv.get('target_widget')
    limit = argv.get('limit')

    env_details = RecoUtils.get_env_details()
    computation_start_datetime = datetime.now() - timedelta(days=use_months*30)

    if target_widget == 'categories':
        top_categories_json = CAT_PAGE_TOP_CATEGORIES_JSON
        category_2_name_json = CAT_PAGE_CATEGORY_2_NAME_JSON
        categories_lsi_model_dir = CAT_PAGE_CATEGORIES_LSI_MODEL_DIR
        ups_field_name = 'categories'
    elif target_widget == 'search_bar':
        top_categories_json = SEARCH_BAR_TOP_CATEGORIES_JSON
        category_2_name_json = SEARCH_BAR_CATEGORY_2_NAME_JSON
        categories_lsi_model_dir = SEARCH_BAR_CATEGORIES_LSI_MODEL_DIR
        ups_field_name = 'search_bar_categories'
    else:
        raise Exception('unknown target widget')

    print("Printing the various variable values custom to target widget")
    print("top_categories_json=%s" % top_categories_json)
    print("category_2_name_json=%s" % category_2_name_json)
    print("categories_lsi_model_dir=%s" % categories_lsi_model_dir)
    print("ups_field_name=%s" % ups_field_name)
 
    if do_gen_top_cats | do_prepare_model:
        df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, str(computation_start_datetime), None, None) 
        if do_gen_top_cats:
            _generate_top_categories(df, results, env_details['bucket_name'])
        if do_prepare_model:
            customer_2_cat_corpus_dict = {row['customer_id']: row['l3_category_freq'] for row in generate_customer_cat_corpus(df).collect()}
            make_and_save_model(env_details['bucket_name'], list(customer_2_cat_corpus_dict.values()))

    if gen_user_categories:
        #json.loads(s3_client.get_object(Bucket='nykaa-dev-recommendations', Key='cool.json')['Body'].read().decode('utf-8'))
        print('Loading top categories')
        top_categories = json.loads(s3_client.get_object(Key=top_categories_json, Bucket=env_details['bucket_name'])['Body'].read().decode('utf-8'))
        print('Loading LSI Model')
        lsi_model = load_model(env_details['bucket_name'], categories_lsi_model_dir)
        print('Generate top categories for user')
        #generate_top_categories_for_user(platform, start_datetime, end_datetime, top_categories, lsi_model, orders_count)
        if views:
            generate_top_categories_from_views(top_categories, lsi_model, days=argv.get('days'), limit=limit)
        else:
            generate_top_categories_for_user(platform, start_datetime, end_datetime, top_categories, lsi_model, orders_count)
