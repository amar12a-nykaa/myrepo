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
from recoutils import RecoUtils
from datautils import DataUtils
from mysqlredshiftutils import MysqlRedshiftUtils
from sparkutils import SparkUtils

spark = SparkUtils.get_spark_instance('Gen Top Categories')
#spark = SparkSession.builder.appName("Gen Top Categories").getOrCreate()
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

    def add_categories_in_ups(rows):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('user_profile_service')
        for i, row in enumerate(rows):
            table.update_item(Key={'user_id': '%s' % row['customer_id']}, UpdateExpression='SET categories = :val', ExpressionAttributeValues={':val': row['value']})

def _generate_top_categories(df, es_scroll_results, bucket_name):
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
        S3Utils.upload_dir(temp_dir, bucket_name, CATEGORIES_LSI_MODEL_DIR)
        return CATEGORIES_LSI_MODEL_DIR

def load_model(bucket_name, s3_path):
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
    CategoriesUtils.add_categories_in_ups(rows)
    print("Done Writing the categories recommendations " + str(datetime.now()))

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
    parser.add_argument('--platform', default='nykaa', choices=['nykaa','men'])
    parser.add_argument('-n', '--n', default=20, type=int)
    
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
    n = argv.get('n')
    orders_count = argv.get('orders_count')

    env_details = RecoUtils.get_env_details()
    computation_start_datetime = datetime.now() - timedelta(days=use_months*30)
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
        top_categories = json.loads(s3_client.get_object(Key=TOP_CATEGORIES_JSON, Bucket=env_details['bucket_name'])['Body'].read().decode('utf-8'))
        print('Loading LSI Model')
        lsi_model = load_model(env_details['bucket_name'], CATEGORIES_LSI_MODEL_DIR)
        print('Generate top categories for user')
        generate_top_categories_for_user(platform, start_datetime, end_datetime, top_categories, lsi_model, orders_count)
    #generate_top_categories_for_user(env, platform, n, prepare_model, start_datetime, end_datetime)
    #top_categories_df = generate_top_categories(env, platform, n, start_datetime, end_datetime)
    #top_categories_df.show(n, False)
