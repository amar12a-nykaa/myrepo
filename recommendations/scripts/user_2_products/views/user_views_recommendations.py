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

spark = SparkUtils.get_spark_instance('Gen User Views Recommendations')

sc = spark.sparkContext
print(sc.getConf().getAll())

s3_client = boto3.client('s3')

ORDER_SOURCE_NYKAA = ['Nykaa', 'Nykaa(Old)', 'NYKAA', 'CS-Manual']
ORDER_SOURCE_NYKAAMEN = ['NykaaMen']

U2P_LSI_MODEL_DIR = 'u2p/models'

DAILY_SYNC_FILE_PREFIX= 'Nykaa_CustomerInteractions_'
VIEWS_CA_S3_PREFIX = 'u2p/data/'


def generate_customer_purchases_corpus(df):
    df = df.select(['customer_id', 'order_id', 'product_id']).distinct().dropna().groupBy(['customer_id', 'product_id']).agg({'order_id': 'count'}).withColumnRenamed('count(order_id)', 'orders_count')
    df = df.groupBy(['customer_id']).agg(func.collect_list(func.struct(col('product_id'), col('orders_count'))).alias('product_freq'))
    return df.select(['customer_id', 'product_freq']).rdd.map(lambda row: (row['customer_id'], [[product_count['product_id'], product_count['orders_count']] for product_count in row['product_freq']])).toDF(['customer_id', 'product_freq'])
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
        S3Utils.upload_dir(temp_dir, bucket_name, U2P_LSI_MODEL_DIR)
        return U2P_LSI_MODEL_DIR

def load_model(bucket_name, s3_path):
    with tempfile.TemporaryDirectory() as temp_dir:
        S3Utils.download_dir(s3_path, s3_path, temp_dir, bucket_name)
        model = models.LsiModel.load(temp_dir + '/model.lsi')
        return model

def generate_products_corpus(product_ids):
    return [[[product_id, 1]] for product_id in product_ids]

def prepare_views_ca_dataframe(files):
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Customer ID (evar23)", StringType(), True),
        StructField("Products", IntegerType(), True),
        StructField("Product Views", IntegerType(), True),
        StructField("Cart Additions", IntegerType(), True)])

    if not env_details['is_emr']:
        S3Utils.download_dir(VIEWS_CA_S3_PREFIX, VIEWS_CA_S3_PREFIX, '/data/u2p/views/data/', env_details['bucket_name'])
        files = [f for f in glob.glob('/data/u2p/views/data/' + VIEWS_CA_S3_PREFIX + "**/*.csv", recursive=True)]

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
    df = df.filter((col('cart_additions') <= 2) & (col('views') <= 5))
    print("Total Number of rows now: %d" % df.count())

    print('Scrolling ES for results')
    results = ESUtils.scrollESForResults()
    print('Scrolling ES done')

    child_2_parent = results['child_2_parent']
    def convert_to_parent(product_id):
        return child_2_parent.get(product_id, product_id)

    convert_to_parent_udf = udf(convert_to_parent, IntegerType())
    print('Converting product_id to parent')
    df = df.withColumn("product_id", convert_to_parent_udf(df['product_id']))

    product_2_l3_category = {product_id: json.loads(categories[0])['l3']['id'] for product_id, categories in results['primary_categories'].items()}
    results['product_2_l3_category'] = product_2_l3_category
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
    return df, results

def generate_recommendations(all_products, lsi_model, days=None, limit=None):
    FTPUtils.sync_ftp_data(DAILY_SYNC_FILE_PREFIX, env_details['bucket_name'], VIEWS_CA_S3_PREFIX, ['Interaction_one_time_20190501-20190620.zip'])
    csvs_path = S3Utils.ls_file_paths(env_details['bucket_name'], VIEWS_CA_S3_PREFIX, True)
    csvs_path = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")).days <= 30 , csvs_path))
    print(csvs_path)
    #update_csvs_path = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")) >= (datetime.now() + timedelta(hours=5, minutes=30) - timedelta(hours=24)).date() , csvs_path))
    df, results = DataUtils.prepare_views_ca_dataframe(spark, csvs_path, VIEWS_CA_S3_PREFIX, '/data/u2p/views/data/', True)
    customer_ids_need_update = []
    if days:
        customer_ids_need_update = df.filter(col('date') >= (datetime.now() + timedelta(hours=5, minutes=30) - timedelta(hours=24*days)).date()).select(["customer_id"]).distinct().rdd.flatMap(lambda x: x).collect()
        if limit:
            customer_ids_need_update = customer_ids_need_update[:limit]
        print("Only taking customer_ids which need to be updated")
        print("Total number of customer_id=%d" % len(customer_ids_need_update))
        df = df.filter(col('customer_id').isin(customer_ids_need_update))
        print("Total Number of rows now: %d" % df.count())
 

    # TODO need to take into account the timezone offset
    def calculate_weight(row_date, views, cart_additions):
        weight = views*0.3 + cart_additions*0.7
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
    df = df.groupBy(['customer_id', 'product_id']).agg(func.sum(col('weight')).alias('weight'))
    df = df.groupBy(['customer_id']).agg(func.collect_list(func.struct('product_id', 'weight')).alias('vec'))

    views_rdd = df.select(['customer_id', 'vec']).rdd.map(lambda row: Row(customer_id=row['customer_id'], vec=[[product_count['product_id'], product_count['weight']] for product_count in row['vec']]))

    # Preparing customer_2_last_order_categories
    start_datetime = (datetime.now() - timedelta(hours=24*30))
    orders_df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, start_datetime, None, None, customer_ids_need_update) 
    # TODO instead of just removing the products, we can give neg weightage to products purchased
    customer_2_last_month_purchase_categories = {int(row['customer_id']): row['categories'] for row in orders_df.select(['customer_id', 'l3_category']).distinct().groupBy(['customer_id']).agg(func.collect_list('l3_category').alias('categories')).collect()}
    #customer_2_last_month_purchases = {int(row['customer_id']): row['products'] for row in orders_df.select(['customer_id', 'product_id']).distinct().groupBy(['customer_id']).agg(func.collect_list('product_id').alias('products')).collect()}


    product_2_l3_category = results['product_2_l3_category']
    idx_2_product_id = {key: product_id for key, product_id in enumerate(all_products)}
    products_corpus_lsi = lsi_model[generate_products_corpus(all_products)]
    index = similarities.MatrixSimilarity(products_corpus_lsi)
    norm_model = models.NormModel()
    sorted_products_rdd = views_rdd.map(lambda row: Row(customer_id=row['customer_id'], vec=row['vec'], sorted_products=list(filter(lambda x: (not product_2_l3_category.get(x) ) or product_2_l3_category[x] not in customer_2_last_month_purchase_categories.get(row['customer_id'], []), map(lambda ele: idx_2_product_id[ele[0]], sorted(enumerate(index[lsi_model[norm_model.normalize(row['vec'])]]), key=lambda e: -e[1]))))[:200])) 
    #sorted_cats_udf = udf(lambda customer_id, user_cat_doc: list(filter(lambda x: x not in customer_2_last_order_categories[customer_id], map(lambda ele: idx_2_cat[ele[0]], sorted(enumerate(index[lsi_model[norm_model.normalize(user_cat_doc)]]), key=lambda e: -e[1])))) + customer_2_last_order_categories[customer_id], ArrayType(IntegerType()))
    rows = [(platform, row['customer_id'], 'user', 'bought', 'views_lsi_model', json.dumps(row['sorted_products'])) for row in sorted_products_rdd.collect()]
    print("Writing the user recommendations " + str(datetime.now()))
    print("Total number of customers to be updated: %d" % len(rows))
    print("Few users getting updated are listed below")
    print([row[1] for row in rows[:10]])
    MysqlRedshiftUtils.add_recommendations_in_mysql(MysqlRedshiftUtils.mlMysqlConnection(), 'recommendations_v2', rows)
    print("Done Writing the user recommendations " + str(datetime.now()))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prepare-model', action='store_true')
    parser.add_argument('--gen-user-recommendations', action='store_true')
    parser.add_argument('--use-months', type=int, default=6)
    parser.add_argument('--hours', type=int)
    parser.add_argument('--days', type=int)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--platform', default='nykaa', choices=['nykaa','men'])
 
    argv = vars(parser.parse_args())
    do_prepare_model = argv.get('prepare_model')
    gen_user_recommendations = argv.get('gen_user_recommendations')
    use_months = argv.get('use_months')
    days = argv.get('days')
    hours = argv.get('hours')
    platform = argv.get('platform')
    limit = argv.get('limit')

    env_details = RecoUtils.get_env_details()
    computation_start_datetime = datetime.now() - timedelta(days=use_months*30)


    if do_prepare_model:
        df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, str(computation_start_datetime), None, None) 
        print('Total number of product_ids: %d' % df.select('product_id').distinct().count())
        customer_2_product_corpus_dict = {row['customer_id']: row['product_freq'] for row in generate_customer_purchases_corpus(df).collect()}
        make_and_save_model(env_details['bucket_name'], list(customer_2_product_corpus_dict.values()))

    if gen_user_recommendations:
        print('Loading LSI Model')
        lsi_model = load_model(env_details['bucket_name'], U2P_LSI_MODEL_DIR)
        results = ESUtils.scrollESForResults()
        all_products = list(set(results['sku_2_product_id'].values()))
        print('Generate recommendations for user')
        generate_recommendations(all_products, lsi_model, days=argv.get('days'), limit=limit)
