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
from pyspark.sql.functions import concat, col, lit, udf, isnan, desc
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

spark = SparkUtils.get_spark_instance('Gen User Views Recommendations')

sc = spark.sparkContext
print(sc.getConf().getAll())

s3_client = boto3.client('s3')

ORDER_SOURCE_NYKAA = ['Nykaa', 'Nykaa(Old)', 'NYKAA', 'CS-Manual']
ORDER_SOURCE_NYKAAMEN = ['NykaaMen']

U2P_LSI_MODEL_DIR = 'u2p/models/lsi_views'

DAILY_SYNC_FILE_PREFIX= 'Nykaa_CustomerInteractions_'
VIEWS_CA_S3_PREFIX = 'u2p/data/'

CAV_APP_DATA_PATH = 'cav/data/app/'
DAILY_SYNC_FILE_APP_PREFIX = 'cav_app_daily'

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

    df = df.filter(df['Date'].isNotNull())
    df = df.withColumn("Date", udf(lambda d: datetime.strptime(d, '%B %d, %Y'), DateType())(col('Date')))
    # Check below
    df = df.filter(((date.today() - timedelta(days=10)) <= col('Date')))

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
    results = ESUtils.scrollESForResults()
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

def compute_cav(platform, files):
    df, results = prepare_data(files, False)

    print("Self joining the dataframe: Computing all the non product to product pairs")
    df = df.withColumnRenamed('product_id', 'product_id_x').join(df.withColumnRenamed('product_id', 'product_id_y'), on="session_id", how="inner")

    print("Product Pairs count: " + str(df.count()))

    print("Filtering out duplicate pairs")
    df = df[df.product_id_x < df.product_id_y]
    print("Product Pairs count: " + str(df.count()))

    print("Computing sessions for product pairs")
    df = df.groupBy(['product_id_x', 'product_id_y']).agg({'session_id': 'count'})
    print("Product pairs count: " + str(df.count()))

    df = df.withColumnRenamed("count(session_id)", 'similarity')

    print("Filtering out all the product pairs with a threshold of atleast 2 sessions")
    df = df[df['similarity'] >= 2]
    print("Product Pairs count: " + str(df.count()))

    product_2_mrp = results['product_2_mrp']

    df = df.withColumnRenamed('product_id_x', 'product').withColumnRenamed('product_id_y', 'recommendation').select(['product', 'recommendation', 'similarity']).union(df.withColumnRenamed('product_id_y', 'product').withColumnRenamed('product_id_x', 'recommendation').select(['product', 'recommendation', 'similarity'])).select(['product', 'recommendation', 'similarity']).withColumn('rank', func.dense_rank().over(Window.partitionBy('product').orderBy(desc('similarity')))).filter(col('rank') < 25)
    #df = df.groupby(['product']).agg(func.collect_list(func.struct('recommendation', 'similarity')).alias('recommendation_struct_list'))
    return df
    #sort_udf = udf(lambda l: [ele[0] for ele in sorted(l, key=lambda e: -e[1])], ArrayType(IntegerType()))
    #df = df.select("product", sort_udf("recommendation_struct_list").alias("recommendations"))

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
    print('Done Converting product_id to parent')

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

def generate_recommendations(cav_df, days=None, limit=None):
    FTPUtils.sync_ftp_data(DAILY_SYNC_FILE_PREFIX, env_details['bucket_name'], VIEWS_CA_S3_PREFIX, [])
    csvs_path = S3Utils.ls_file_paths(env_details['bucket_name'], VIEWS_CA_S3_PREFIX, True)
    # TODO check below
    csvs_path = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")).days <= 15 , csvs_path))
    print(csvs_path)
    df, results = DataUtils.prepare_views_ca_dataframe(spark, csvs_path, VIEWS_CA_S3_PREFIX, '/data/u2p/views/data/', True)
    customer_ids_need_update = []
    if days:
        customer_ids_need_update = df.filter(col('date') >= (datetime.now() + timedelta(hours=5, minutes=30) - timedelta(hours=24*days)).date()).select(["customer_id"]).distinct().rdd.flatMap(lambda x: x).collect()
        print("Total customers: %d" % len(customer_ids_need_update))
        if limit:
            customer_ids_need_update = customer_ids_need_update[:limit]
            print("Limit: %d" % limit)
            print(customer_ids_need_update)
        print("Only taking customer_ids which need to be updated")
        print("Total number of customer_id=%d" % len(customer_ids_need_update))
        df = df.filter(col('customer_id').isin(customer_ids_need_update))
        print("Total Number of rows now: %d" % df.count())
 

    # TODO need to take into account the timezone offset
    def calculate_weight(row_date, views, cart_additions):
        weight = views*0.3 + cart_additions*0.7
        #row_date = datetime.strptime(row_date, '%Y-%m-%d').date()
        today = date.today()
        if (today - row_date).days <= 5:
            return 2*weight
        elif (today - row_date).days <= 10:
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
    df = df.join(cav_df.withColumnRenamed('product', 'product_id'), on="product_id", how="inner")
    #df = df.withColumn("Date", udf(lambda d: datetime.strptime(d, '%B %d, %Y'), DateType())(col('Date')))
    df = df.withColumn('weighted_similarity', udf(lambda sim, w: sim*w, FloatType())(col('similarity'), col('weight'))).groupBy(['customer_id', 'recommendation']).agg({'weighted_similarity': 'sum'}).withColumnRenamed('sum(weighted_similarity)', 'weighted_similarity').groupBy(['customer_id']).agg(func.collect_list(func.struct('recommendation', 'weighted_similarity')).alias('vec'))

    print("Total number of customers to vector: %d" % df.count())
    views_rdd = df.select(['customer_id', 'vec']).rdd.map(lambda row: Row(customer_id=row['customer_id'], vec=[[product_count['recommendation'], product_count['weighted_similarity']] for product_count in row['vec']]))

    # Preparing customer_2_last_order_categories
    start_datetime = (datetime.now() - timedelta(hours=24*30))
    orders_df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, start_datetime, None, None, customer_ids_need_update) 
    # TODO instead of just removing the products, we can give neg weightage to products purchased
    customer_2_last_month_purchase_categories = {int(row['customer_id']): row['categories'] for row in orders_df.select(['customer_id', 'l3_category']).dropna().distinct().groupBy(['customer_id']).agg(func.collect_list('l3_category').alias('categories')).collect()}

    product_2_l3_category = results['product_2_l3_category']
    sorted_products_rdd = views_rdd.map(lambda row: Row(customer_id=row['customer_id'], vec=row['vec'], sorted_products=list(filter(lambda x: (not product_2_l3_category.get(x) ) or product_2_l3_category[x] not in customer_2_last_month_purchase_categories.get(row['customer_id'], []), map(lambda ele: ele[0], sorted(row['vec'], key=lambda e: -e[1]))))[:200])) 
    rows = [{'customer_id': row['customer_id'], 'value': {'coccurence_simple_views': row['sorted_products']}} for row in sorted_products_rdd.collect()]
    print("Writing the user recommendations " + str(datetime.now()))
    print("Total number of customers to be updated: %d" % len(rows))
    print("Few users getting updated are listed below")
    print([row['customer_id'] for row in rows[:10]])
    #MysqlRedshiftUtils.add_recommendations_in_mysql(MysqlRedshiftUtils.mlMysqlConnection(), 'recommendations_v2', rows)
    UPSUtils.add_recommendations_in_ups('recommendations', rows)
    #UPSUtils.add_recommendations_in_ups('', rows)
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
    use_months = argv.get('use_months')
    days = argv.get('days')
    hours = argv.get('hours')
    platform = argv.get('platform')
    limit = argv.get('limit')

    env_details = RecoUtils.get_env_details()
    computation_start_datetime = datetime.now() - timedelta(days=use_months*30)
    FTPUtils.sync_ftp_data(DAILY_SYNC_FILE_APP_PREFIX, env_details['bucket_name'], CAV_APP_DATA_PATH)
    data_path = CAV_APP_DATA_PATH

    if not env_details['is_emr']:
        S3Utils.download_dir(CAV_APP_DATA_PATH, CAV_APP_DATA_PATH, '/data/cav/app/data/', env_details['bucket_name'])
        files = [f for f in glob.glob('/data/cav/app/data/' + CAV_APP_DATA_PATH + "**/*.csv", recursive=True)]
    else:
        files = S3Utils.ls_file_paths(env_details['bucket_name'], data_path, True)

    #files = S3Utils.ls_file_paths(env_details['bucket_name'], data_path, True)
    print('Filtering out last 3 months data')
    # TODO check below
    files = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")).days <= 90 , files))

    cav_df = compute_cav(platform, files)
    generate_recommendations(cav_df, days=argv.get('days'), limit=limit)


