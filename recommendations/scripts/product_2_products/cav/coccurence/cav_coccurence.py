import os
import sys
import psycopg2
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType, ArrayType, DateType
from pyspark.sql.functions import concat, col, lit, udf, isnan, desc
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from contextlib import closing
import math
import boto3
import json
import pandas
from collections import defaultdict
import mysql.connector
import argparse
from elasticsearch import helpers, Elasticsearch
from ftplib import FTP
import zipfile
from datetime import datetime, timedelta, date

sys.path.append('/home/ubuntu/nykaa_scripts/utils')
sys.path.append('/home/hadoop/nykaa_scripts/utils')
from recoutils import RecoUtils
from datautils import DataUtils
from sparkutils import SparkUtils
from mysqlredshiftutils import MysqlRedshiftUtils
from esutils import ESUtils
from s3utils import S3Utils
from ftputils import FTPUtils

spark = SparkUtils.get_spark_instance('CAV')
sc = spark.sparkContext

s3 = boto3.client('s3')

env_details = RecoUtils.get_env_details()

CAV_APP_DATA_PATH = 'cav/data/app/'
CAV_WEB_DATA_PATH = 'cav/data/web/'
CAV_AGG_DATA_PATH = 'cav/data/agg/'

DAILY_SYNC_FILE_APP_PREFIX = 'cav_app_daily'
DAILY_SYNC_FILE_WEB_PREFIX = 'cav_web_daily'

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
    df = df.filter(((date.today() - timedelta(days=90)) <= col('Date')))

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

def compute_cav(platform, files, desktop, algo_name):
    df, results = prepare_data(files, desktop)

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

    luxe_products_dict = {p:True for p in results['luxe_products']}

    def is_luxe(product_id):
        return luxe_products_dict.get(product_id, False)

    is_luxe_udf = udf(is_luxe, BooleanType())
    print('Is Luxe')
    df = df.withColumn("is_luxe_x", is_luxe_udf(df['product_id_x']))
    df = df.withColumn("is_luxe_y", is_luxe_udf(df['product_id_y']))

    product_2_mrp = results['product_2_mrp']
    #product_in_stock = results['product_in_stock']

    df = df.withColumnRenamed('product_id_x', 'product').withColumnRenamed('product_id_y', 'recommendation').withColumnRenamed('is_luxe_x', 'product_luxe').withColumnRenamed('is_luxe_y', 'reco_luxe').select(['product', 'recommendation', 'similarity', 'product_luxe', 'reco_luxe']).union(df.withColumnRenamed('product_id_y', 'product').withColumnRenamed('product_id_x', 'recommendation').withColumnRenamed('is_luxe_y', 'product_luxe').withColumnRenamed('is_luxe_x', 'reco_luxe').select(['product', 'recommendation', 'similarity', 'product_luxe', 'reco_luxe'])).select(['product', 'recommendation', 'similarity', 'product_luxe', 'reco_luxe']).filter(((col('product_luxe') & col('reco_luxe')) | (col('product_luxe') == False))).withColumn('rank', func.dense_rank().over(Window.partitionBy('product').orderBy(desc('similarity')))).filter(col('rank') < 100)
    df = df.groupby(['product']).agg(func.collect_list(func.struct('recommendation', 'similarity')).alias('recommendation_struct_list'))
    sort_udf = udf(lambda l: [ele[0] for ele in sorted(l, key=lambda e: -e[1])], ArrayType(IntegerType()))
    df = df.select("product", sort_udf("recommendation_struct_list").alias("recommendations"))

    #for row in final_df.collect():
    #    if (luxe_dict.get(row['product_id_x'], False) and luxe_dict.get(row['product_id_y'], False))  or not luxe_dict.get(row['product_id_x'], False):
    #    #if not (luxe_dict.get(row['product_id_x'], False) ^ luxe_dict.get(row['product_id_y'], False)):
    #        simple_similar_products_dict[row['product_id_x']].append((row['product_id_y'], row['sessions_intersection']))
    #    if (luxe_dict.get(row['product_id_x'], False) and luxe_dict.get(row['product_id_y'], False))  or not luxe_dict.get(row['product_id_y'], False):
    #        simple_similar_products_dict[row['product_id_y']].append((row['product_id_x'], row['sessions_intersection']))

    #        if product_2_mrp.get(row['product_id_x']) and product_2_mrp.get(row['product_id_y']):
    #            product_x_mrp = product_2_mrp[row['product_id_x']]
    #            product_y_mrp = product_2_mrp[row['product_id_y']]
    #            if ((product_x_mrp - product_y_mrp)/product_x_mrp) <= 0.1 and product_in_stock.get(row['product_id_y'], False):
    #                simple_similar_products_mrp_cons_dict[row['product_id_x']].append((row['product_id_y'], row['sessions_intersection']))
    #            if ((product_y_mrp - product_x_mrp)/product_y_mrp) <= 0.1 and product_in_stock.get(row['product_id_x'], False):
    #                simple_similar_products_mrp_cons_dict[row['product_id_y']].append((row['product_id_x'], row['sessions_intersection']))

    parent_2_children = defaultdict(lambda: [])
    for child, parent in results['child_2_parent'].items():
        parent_2_children[parent].append(child)

    print('Total Number of parent products: %d' % len(parent_2_children))
    print('Total Number of child products: %d' % sum([len(variants) for parent, variants in parent_2_children.items()]))

    rows = []
    product_ids_updated = []
    for row in df.collect():
        product_ids_updated.append(row['product'])
        if desktop:
            rows.append((platform, row['product'], 'product', 'viewed', 'coccurence_simple_desktop', json.dumps(row['recommendations'])))
        else:
            rows.append((platform, row['product'], 'product', 'viewed', algo_name, json.dumps(row['recommendations'])))
        variants = parent_2_children.get(row['product'], [])
        for variant in variants:
            product_ids_updated.append(variant)
            if desktop:
                rows.append((platform, row['product'], 'product', 'viewed', 'coccurence_simple_desktop', json.dumps(row['recommendations'])))
            else:
                rows.append((platform, row['product'], 'product', 'viewed', algo_name, json.dumps(row['recommendations'])))

    #for product_id in simple_similar_products_dict:
    #    product_ids_updated.append(product_id)
    #    simple_similar_products = list(map(lambda e: int(e[0]), sorted(simple_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
    #    if desktop:
    #        rows.append((platform, product_id, 'product', 'viewed', 'coccurence_simple_desktop', json.dumps(simple_similar_products)))
    #    else:
    #        rows.append((platform, product_id, 'product', 'viewed', 'coccurence_simple', json.dumps(simple_similar_products)))
    #    variants = parent_2_children.get(product_id, [])
    #    for variant in variants:
    #        product_ids_updated.append(variant)
    #        if desktop:
    #            rows.append((platform, variant, 'product', 'viewed', 'coccurence_simple_desktop', str(simple_similar_products)))
    #        else:
    #            rows.append((platform, variant, 'product', 'viewed', 'coccurence_simple', str(simple_similar_products)))

    print('Adding recommendations for %d products with algo=coccurence_simple in DB' % len(set(product_ids_updated)))

    #product_ids_updated = []
    #for product_id in simple_similar_products_mrp_cons_dict:
    #    product_ids_updated.append(product_id)
    #    _simple_similar_products = list(map(lambda e: int(e[0]), sorted(simple_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
    #    simple_similar_products = list(map(lambda e: e[0], sorted(simple_similar_products_mrp_cons_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
    #    simple_similar_products = simple_similar_products[:2] + [p for p in _simple_similar_products if p not in simple_similar_products[:2]]
    #    if desktop:
    #        rows.append((platform, product_id, 'product', 'viewed', 'premium_cs_2', str(simple_similar_products)))
    #    else:
    #        rows.append((platform, product_id, 'product', 'viewed', 'premium_cs_2', str(simple_similar_products)))
    #    variants = parent_2_children.get(product_id, [])
    #    for variant in variants:
    #        product_ids_updated.append(variant)
    #        if desktop:
    #            rows.append((platform, variant, 'product', 'viewed', 'premium_cs_2', str(simple_similar_products)))
    #        else:
    #            rows.append((platform, variant, 'product', 'viewed', 'premium_cs_2', str(simple_similar_products)))

    #print('Adding recommendations for %d products with algo=premium_cs in DB' % len(set(product_ids_updated)))

    #product_ids_updated = []
    #for product_id in simple_similar_products_mrp_cons_dict:
    #    product_ids_updated.append(product_id)
    #    _simple_similar_products = list(map(lambda e: int(e[0]), sorted(simple_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
    #    simple_similar_products = list(map(lambda e: e[0], sorted(simple_similar_products_mrp_cons_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
    #    simple_similar_products = simple_similar_products[:4] + [p for p in _simple_similar_products if p not in simple_similar_products[:4]]
    #    if desktop:
    #        rows.append((platform, product_id, 'product', 'viewed', 'premium_cs_4', str(simple_similar_products)))
    #    else:
    #        rows.append((platform, product_id, 'product', 'viewed', 'premium_cs_4', str(simple_similar_products)))
    #    variants = parent_2_children.get(product_id, [])
    #    for variant in variants:
    #        product_ids_updated.append(variant)
    #        if desktop:
    #            rows.append((platform, variant, 'product', 'viewed', 'premium_cs_4', str(simple_similar_products)))
    #        else:
    #            rows.append((platform, variant, 'product', 'viewed', 'premium_cs_4', str(simple_similar_products)))

    #print('Adding recommendations for %d products with algo=premium_cs in DB' % len(set(product_ids_updated)))

    #MysqlRedshiftUtils.add_recommendations_in_mysql(MysqlRedshiftUtils.mysqlConnection(), 'recommendations_v2', rows)
    MysqlRedshiftUtils.add_recommendations_in_mysql(MysqlRedshiftUtils.mlMysqlConnection(), 'recommendations_v2', rows)
    #RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection(), 'recommendations_v2', rows)

def cav_sync_data():
    ftp = FTPUtils.get_handler()
    cav_app_files, cav_web_files = [], []

    def fill_cav_files(f):
        if f.startswith('cav_app_daily'):
            cav_app_files.append(f)
        if f.startswith('cav_web_daily'):
            cav_web_files.append(f)

    ftp.retrlines('NLST', fill_cav_files)
    cav_app_csvs = list(map(lambda f: f.replace('zip', 'csv'), cav_app_files))
    cav_web_csvs = list(map(lambda f: f.replace('zip', 'csv'), cav_web_files))

    s3_cav_app_csvs = S3Utils.ls_file_paths(env_details['bucket_name'], CAV_APP_DATA_PATH)
    s3_cav_web_csvs = S3Utils.ls_file_paths(env_details['bucket_name'], CAV_WEB_DATA_PATH)

    s3_cav_app_csvs = [s[s.rfind("/") + 1:] for s in s3_cav_app_csvs]
    s3_cav_web_csvs = [s[s.rfind("/") + 1:] for s in s3_cav_web_csvs]
    app_csvs_needed_to_be_pushed = list(set(cav_app_csvs) - set(s3_cav_app_csvs))
    web_csvs_needed_to_be_pushed = list(set(cav_web_csvs) - set(s3_cav_web_csvs))

    S3Utils.transfer_ftp_2_s3(ftp, list(map(lambda f: f.replace('csv', 'zip'), app_csvs_needed_to_be_pushed)), env_details['bucket_name'], CAV_APP_DATA_PATH)
    S3Utils.transfer_ftp_2_s3(ftp, list(map(lambda f: f.replace('csv', 'zip'), web_csvs_needed_to_be_pushed)), env_details['bucket_name'], CAV_WEB_DATA_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for CAV script')
    parser.add_argument('--desktop', action='store_true')
    parser.add_argument('--data-file')
    parser.add_argument('--algo', default='coccurence_simple')
    #parser.add_argument('--files', nargs='+')
    parser.add_argument('--platform', default='nykaa', choices=['nykaa','men'])

    argv = vars(parser.parse_args())
    #files = argv['files']
    desktop = argv.get('desktop')
    platform = argv.get('platform')
    algo_name = argv.get('algo')

    print("Printing Configurations:")
    print(sc.getConf().getAll())

    if argv.get('data_file'):
        data_file = argv.get('data_file')
        ftp = FTPUtils.get_handler()
        S3Utils.transfer_ftp_2_s3(ftp, [data_file], env_details['bucket_name'], CAV_AGG_DATA_PATH)
        files = ['s3://%s/%s%s' % (env_details['bucket_name'], CAV_AGG_DATA_PATH, data_file.replace('zip', 'csv'))]
    else:
        if desktop:
            FTPUtils.sync_ftp_data(DAILY_SYNC_FILE_WEB_PREFIX, env_details['bucket_name'], CAV_WEB_DATA_PATH)
            data_path = CAV_WEB_DATA_PATH
        else:
            FTPUtils.sync_ftp_data(DAILY_SYNC_FILE_APP_PREFIX, env_details['bucket_name'], CAV_APP_DATA_PATH)
            data_path = CAV_APP_DATA_PATH

        files = S3Utils.ls_file_paths(env_details['bucket_name'], data_path, True)
        print('Filtering out last 3 months data')
        files = list(filter(lambda f: (datetime.now() - datetime.strptime(("%s-%s-%s" % (f[-12:-8], f[-8:-6], f[-6:-4])), "%Y-%m-%d")).days <= 90 , files))
    print("Using files")
    print(files)

    compute_cav(platform, files, desktop, algo_name)
