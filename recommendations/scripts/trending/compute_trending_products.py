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
import math

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

#ORDER_SOURCE_NYKAA = ['Nykaa', 'Nykaa(Old)', 'NYKAA', 'CS-Manual']
#ORDER_SOURCE_NYKAAMEN = ['NykaaMen']

#CAT_PAGE_TOP_CATEGORIES_JSON = 'categories_page/top_categories.json'
#CAT_PAGE_TOP_CATEGORIES_CUSTOM_N_JSON = 'categories_page/top_categories_%d.json'
#CAT_PAGE_CATEGORY_2_NAME_JSON = 'categories_page/category_2_name.json'
#CAT_PAGE_CATEGORIES_LSI_MODEL_DIR = 'categories_page/models'
#CAT_PAGE_CATEGORIES_LSI_MODEL_CUSTOM_N_DIR = 'categories_page/models_%d'

#SEARCH_BAR_TOP_CATEGORIES_JSON = 'search_bar/categories/top_categories.json'
#SEARCH_BAR_CATEGORY_2_NAME_JSON = 'search_bar/categories/category_2_name.json'
#SEARCH_BAR_CATEGORIES_LSI_MODEL_DIR = 'search_bar/categories/models/lsi_views'

DAILY_SYNC_FILE_PREFIX= 'App_data_all_metrics_'
VIEWS_CA_S3_PREFIX = 'files/'

def prepare_views_ca_dataframe(files):
    schema = StructType([
        StructField("Date", StringType(), True),
        #StructField("Customer ID (evar23)", StringType(), True),
        StructField("Products", IntegerType(), True),
        StructField("Product Views", IntegerType(), True),
        StructField("Cart Additions", IntegerType(), True),
        StructField("Orders", IntegerType(), True),
        StructField("Revenue", FloatType(), True),
        StructField("Units", IntegerType(), True)])
    if not env_details['is_emr']:
        #TODO: remove in prod
        S3Utils.download_dir(VIEWS_CA_S3_PREFIX, VIEWS_CA_S3_PREFIX, '/data/categories_page/data/', env_details['bucket_name'])
        files = [f for f in glob.glob('/data/categories_page/data/' + "**/*.csv", recursive=True)]
    print("Using files: " + str(files))
    #from IPython import embed; embed()
    df = spark.read.load(files[0], header=True, format='csv', schema=schema)
    #print("df lenght first: ", df.count())
    for i in range(1, len(files)):
        df = df.union(spark.read.load(files[i], header=True, format='csv', schema=schema))
        #print("df lenght first: ", df.count())
    df = df[['Date', 'Products', 'Product Views', 'Cart Additions']].withColumnRenamed("Date", "date").withColumnRenamed("Products", "product_id").withColumnRenamed("Product Views", "views").withColumnRenamed("Cart Additions", "cart_additions")
    print("Total Number of rows: %d" % df.count())
    print("Dropping nulls")
    df = df.dropna()
    #print("Total Number of rows now: %d" % df.count())
    print("Filtering out junk data")
    #df = df.filter((col('cart_additions') <= 5) & (col('views') <= 5))
    #print("Total Number of rows now: %d" % df.count())
    #print('Scrolling ES for results')
    #results = ESUtils.scrollESForResults()
    #print('Scrolling ES done')
    #product_2_l3_category = {product_id: json.loads(categories[0])['l3']['id'] for product_id, categories in results['primary_categories'].items()}
    #l3_udf = udf(lambda product_id: product_2_l3_category.get(product_id), StringType())
    #df = df.withColumn('l3_category', l3_udf('product_id'))
    #df = df.filter(col('l3_category') != 'LEVEL')
    #df = df.withColumn('l3_category', col('l3_category').cast('int'))
    #print("Dropping nulls after l3 category addition for products")
    df = df.dropna()
    print("Total Number of rows now: %d" % df.count())
    #df.withColumn("date_1", udf(lambda d: datetime.strptime(d, '%B %d, %Y'), DateType())(col('date'))).show()
    df = df.withColumn("date", udf(lambda d: datetime.strptime(d, '%B %d, %Y'), DateType())(col('date')))
    #df.show()
    print("Dropping nulls after changing the type of date")
    df = df.dropna()
    #df.show()
    print("Total Number of rows now: %d" % df.count())
    return df

def generate_trending_products_from_CA(env_details,floating=False, algo="ca", days=4, limit=None, views=False, threshold=2, latest_threshold=10):
    FTPUtils.sync_ftp_data_from_search(DAILY_SYNC_FILE_PREFIX, env_details['bucket_name'], VIEWS_CA_S3_PREFIX, [])
    csvs_path = S3Utils.ls_file_paths(env_details['bucket_name'], VIEWS_CA_S3_PREFIX, True)
    csvs_path = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")).days <= 31 , csvs_path))
    print(csvs_path)
    #update_csvs_path = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")) >= (datetime.now() + timedelta(hours=5, minutes=30) - timedelta(hours=24)).date() , csvs_path))
    df = prepare_views_ca_dataframe(csvs_path)
    #df.show()
    if views:
        df = df.filter(col('views')>threshold)
    else:
        df = df.filter(col('cart_additions')>threshold)
    df.show()
    #df = df.filter(col('cart_additions')!=0)
    #df.show()
    customer_ids_need_update = [] 
    print("min date:", df.agg({"date": "min"}).collect()[0])
    print("max date:", df.agg({"date": "max"}).collect()[0])
    df.printSchema()
    if days:
        if limit:
            customer_ids_need_update = df.filter(col('date') >= (datetime.now() + timedelta(hours=5, minutes=30) - timedelta(hours=24*days)).date()).select(["customer_id"]).distinct().rdd.flatMap(lambda x: x).collect()
            customer_ids_need_update = customer_ids_need_update[:limit]
            print("Only taking customer_ids which need to be updated")
            #print("Total number of customer_id=%d" % len(/))
            df = df.filter(col('customer_id').isin(customer_ids_need_update))
        else:
            df = df.filter(col('date') >= (datetime.now() + timedelta(hours=5, minutes=30) - timedelta(hours=24*days)).date())
    print("Total Number of rows now: %d" % df.count())
    if views:
        product_ids = df.groupBy("product_id").agg(func.count("views").alias("ca_count")).where(col("ca_count")>=15).select("product_id").collect()
        asdf = df.groupBy("product_id").agg(func.count("cart_additions").alias("ca_count")).orderBy(['ca_count'], ascending=False)
        print("TOtal prods:", asdf.count())
        print("eligible prds: ", asdf.filter(col("ca_count")>=15).count())
    else:
        product_ids = df.groupBy("product_id").agg(func.count("cart_additions").alias("ca_count")).where(col("ca_count")>=15).select("product_id").collect()
    #print("df after cart_additions filter")
    #df.filter(col('cart_additions')>0)

    product_ids = list(map(lambda x: x.product_id, product_ids))
    if not len(product_ids):
        print("max days data: ", df.groupBy("product_id").agg(func.count("cart_additions").alias("ca_count")).groupby().max('ca_count').first().asDict()['max(ca_count)'])
        print("ALERT!!!! products count having 15 days data is 0")
    print("product IDS: ", len(product_ids))
    df = df.filter(col('product_id').isin(product_ids)).orderBy(['product_id', 'date'], ascending=False)
    if views:
        df = df.groupBy("product_id").agg(func.collect_list("views").alias("past_ca"))
    else:
        df = df.groupBy("product_id").agg(func.collect_list("cart_additions").alias("past_ca"))
    def get_floating_z_score(ls, decay=0.9):
        obs = ls[0]
        ls1 = ls[2:]
        avg = float(ls[1])
        sqrAvg = float((ls[1] ** 2))
        for value in ls1:
            avg = avg * decay + value * (1-decay)
            sqrAvg = sqrAvg * decay + (value ** 2) * (1 - decay)
        std = math.sqrt(sqrAvg - avg ** 2)
        if std == 0:
            return (obs - avg) 
        else:
            return (obs - avg)/std

    def get_z_score(ls):
        if (len(ls)>=15):
            pop = ls[1:14]
        else:
            pop = ls[1:]
        obs = ls[0]
        if obs<latest_threshold:
            return 0
        number = float(len(pop))
        avg = sum(pop) / number
        std = math.sqrt(sum(((c - avg) ** 2) for c in pop) / number)
        if std==0:
            return 0;
        return (obs - avg) / std
    if floating:
        df = df.withColumn("z_score", udf(get_floating_z_score, FloatType())(col("past_ca")))
    else:
        df = df.withColumn("z_score", udf(get_z_score, FloatType())(col("past_ca")))
    
    df = df.orderBy(desc("z_score"))
    df.show(20, False)







    #    return (obs - self.avg()) / self.std()

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

    env_details = RecoUtils.get_env_details()
    generate_trending_products_from_CA(env_details, views=True, threshold=15, latest_threshold=100, days=30)
    
    argv = vars(parser.parse_args())


