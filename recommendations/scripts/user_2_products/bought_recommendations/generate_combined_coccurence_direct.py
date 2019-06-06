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
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, BooleanType
from pyspark.sql.functions import udf, col, desc
import pyspark.sql.functions as func
from pyspark.sql.window import Window

sys.path.append('/home/ubuntu/nykaa_scripts/utils')
sys.path.append('/home/hadoop/nykaa_scripts/utils')
from recoutils import RecoUtils
from datautils import DataUtils
from mysqlredshiftutils import MysqlRedshiftUtils
from sparkutils import SparkUtils

#sys.path.append("/var/www/pds_api/")
#from pas.v2.utils import Utils, RecommendationsUtils

spark = SparkUtils.get_spark_instance()
#spark = SparkSession.builder \
#            .master("local[6]") \
#            .appName("U2P") \
#            .config("spark.executor.memory", "4G") \
#            .config("spark.storage.memoryFraction", 0.4) \
#            .config("spark.driver.memory", "26G") \
#            .getOrCreate()
 
sc = spark.sparkContext
print(sc.getConf().getAll())

#def prepare_orders_dataframe(env, platform, p2p_start_datetime, p2p_end_datetime, limit, separate_parent=False):
    #print("Preparing orders data")
#
    #if platform == 'men':
        #order_sources = ORDER_SOURCE_NYKAAMEN
    #else:
        #order_sources = ORDER_SOURCE_NYKAA
#
    #customer_orders_query = "SELECT fact_order_new.nykaa_orderno as order_id, fact_order_new.order_customerid as customer_id, fact_order_detail_new.product_id, fact_order_detail_new.product_sku, order_date from fact_order_new INNER JOIN fact_order_detail_new ON fact_order_new.nykaa_orderno=fact_order_detail_new.nykaa_orderno WHERE fact_order_new.order_status<>'Cancelled' AND fact_order_new.nykaa_orderno <> 0 AND product_mrp > 1 AND order_source IN (" + ",".join([("'%s'" % source) for source in order_sources]) + ") AND order_customerid IS NOT NULL %s %s " % (" AND order_date <= '%s' " % p2p_end_datetime if p2p_end_datetime else "", " AND order_date >= '%s' " % p2p_start_datetime if p2p_start_datetime else "")
    #print(customer_orders_query)
    #print('Fetching Data from Redshift')
    #rows = Utils.fetchResultsInBatch(Utils.redshiftConnection(env), customer_orders_query, 10000)
    #print('Data fetched')
    #schema = StructType([
            #StructField("order_id", StringType(), True),
            #StructField("customer_id", StringType(), True),
            #StructField("product_id", IntegerType(), True),
            #StructField("product_sku", StringType(), True),
            #StructField("order_date", TimestampType(), True)])
#
    #df = spark.createDataFrame(rows, schema)
    #print('Total number of rows fetched: %d' % df.count())
    #df.printSchema()
    #print('Total number of rows extracted: %d' % df.count())
    #print('Total number of products: %d' % df.select('product_id').distinct().count())
    #print('Scrolling ES for results')
    #results = Utils.scrollESForResults(env)
    #print('Scrolling ES done')
    #child_2_parent = results['child_2_parent']
    #sku_2_product_id = results['sku_2_product_id']
#
    #def convert_sku_to_product_id(sku, product_id):
        #return sku_2_product_id.get(sku, product_id)
#
    #convert_sku_to_product_id_udf = udf(convert_sku_to_product_id, IntegerType())
    #print('Converting sku to product_id')
    #df = df.withColumn("product_id", convert_sku_to_product_id_udf(df['product_sku'], df['product_id']))
    #print('Total number of rows extracted: %d' % df.count())
    #print('Total number of products: %d' % df.select('product_id').distinct().count())
#
    #def convert_to_parent(product_id):
        #return child_2_parent.get(product_id, product_id)
#
    #convert_to_parent_udf = udf(convert_to_parent, IntegerType())
    #if separate_parent:
        #print('Adding separate parent for the product')
        #df = df.withColumn("parent_product_id", convert_to_parent_udf(df['product_id']))
    #else:
        #print('Converting product_id to parent')
        #df = df.withColumn("product_id", convert_to_parent_udf(df['product_id']))
    #
    #print('Total number of rows extracted: %d' % df.count())
    #print('Total number of products: %d' % df.select('product_id').distinct().count())
#
    #print('Dropping na')
    #df = df.na.drop()
    #print('Total number of rows extracted: %d' % df.count())
    #print('Total number of products: %d' % df.select('product_id').distinct().count())
    #df = df.drop("product_sku")
#
    #if separate_parent:
        #print('Selecting distinct(order_id, customer_id, parent_product_id, product_id)')
        #df = df.select(['order_id', 'customer_id', 'parent_product_id', 'product_id', 'order_date']).distinct()
    #else:
        #print('Selecting distinct(order_id, customer_id, product_id)')
        #df = df.select(['order_id', 'customer_id', 'product_id', 'order_date']).distinct()
    #print('Total number of rows extracted: %d' % df.count())
    #print('Total number of products: %d' % df.select('product_id').distinct().count())
#
    #print('Data preparation done, returning dataframe')
    #return df, results

def compute_recommendations(algo, platform, computation_start_datetime, p2p_start_datetime=None, p2p_end_datetime=None, customer_id=None, limit=None, orders_count=10):
    print("Computing u2p")
    df, results = DataUtils.prepare_orders_dataframe(spark, platform, True, p2p_start_datetime, p2p_end_datetime, None)
    luxe_products_dict = {p:True for p in results['luxe_products']}
    df = df.select(['product_id', 'customer_id']).distinct()
    df.printSchema()
    print('Preparing product to customers count')
    #product_to_customers_count_df = df.groupBy('product_id').agg(func.countDistinct('customer_id')).withColumnRenamed('count(DISTINCT customer_id)', 'customers_count').toPandas()
    product_to_customers_count_df = df.groupBy('product_id').count().withColumnRenamed('count', 'customers_count').toPandas()
    product_to_customers_count = dict(zip(product_to_customers_count_df.product_id, product_to_customers_count_df.customers_count))
    print('Doing essential steps in computation')

    #_df = df
    #if customer_id:
    #    _df = _df.filter(col('customer_id') == customer_id)

    #customer_2_products_purchased = {row['customer_id']: row['products_purchased'] for row in _df.groupby('customer_id').agg(func.collect_list('product_id').alias('products_purchased')).collect()}

    df = df.withColumnRenamed('product_id', 'product_id_x').join(df.withColumnRenamed('product_id', 'product_id_y'), on="customer_id", how="inner")
    #df = df.select(['product_id_x', 'product_id_y', 'customer_id']).distinct()
    df = df[df.product_id_x < df.product_id_y]
    df = df.groupBy(['product_id_x', 'product_id_y']).agg({'customer_id': 'count'})
    df = df.withColumnRenamed("count(customer_id)", 'customers_intersection')

    df = df[df.customers_intersection >= 2]

    def is_luxe(product_id):
        return luxe_products_dict.get(product_id, False)

    is_luxe_udf = udf(is_luxe, BooleanType())
    print('Is Luxe')
    df = df.withColumn("is_luxe_x", is_luxe_udf(df['product_id_x']))
    df = df.withColumn("is_luxe_y", is_luxe_udf(df['product_id_y']))
    #df = df[(((df['is_luxe_x'] == True) & (df['is_luxe_y'] == True)) | ((df['is_luxe_x'] == False) & (df['is_luxe_y'] == False)))]

    def compute_union_len(product_id_x, product_id_y, customers_intersection):
        return product_to_customers_count[product_id_x] + product_to_customers_count[product_id_y] - customers_intersection

    compute_union_len_udf = udf(compute_union_len, IntegerType())
    df = df.withColumn("customers_union", compute_union_len_udf(df['product_id_x'], df['product_id_y'], df['customers_intersection']))

    #print(product_to_customers_count)
    
    def compute_similarity(customers_intersection, customers_union):
        return customers_intersection/customers_union

    compute_similarity_udf = udf(compute_similarity, FloatType())
    print('Computing similarity')
    df = df.withColumn("similarity", compute_similarity_udf(df['customers_intersection'], df['customers_union']))
    df = df.withColumnRenamed('product_id_x', 'product').withColumnRenamed('product_id_y', 'recommendation').select(['product', 'recommendation', 'similarity']).union(df.withColumnRenamed('product_id_y', 'product').withColumnRenamed('product_id_x', 'recommendation').select(['product', 'recommendation', 'similarity'])).select(['product', 'recommendation', 'similarity']).withColumn('rank', func.dense_rank().over(Window.partitionBy('product').orderBy(desc('similarity')))).filter(col('rank') < 100)

    #df = df.withColumnRenamed('product_id_x', 'product').withColumnRenamed('product_id_y', 'recommendation').union(df.withColumnRenamed('product_id_y', 'product').withColumnRenamed('product_id_x', 'recommendation'))

#    direct_similar_products_dict = defaultdict(lambda: [])

#    for row in df.collect():
#        if (row['is_luxe_x'] and row['is_luxe_y'])  or not row['is_luxe_x']:
#            direct_similar_products_dict[row['product_id_x']].append((row['product_id_y'], row['similarity']))
#        if (row['is_luxe_y'] and row['is_luxe_x'])  or not row['is_luxe_y']:
#            direct_similar_products_dict[row['product_id_y']].append((row['product_id_x'], row['similarity']))


    rows = []

    c2p_df, _ = DataUtils.prepare_orders_dataframe(spark, platform, True, None, None, computation_start_datetime, customer_id)
    #if customer_id:
    #    c2p_df = c2p_df.filter(col('customer_id') == customer_id)

#    customer_2_products_purchased = {row['customer_id']: row['products'] for row in c2p_df.select(['customer_id', 'order_id', 'product_id', 'order_date']).distinct().withColumn('rank', func.dense_rank().over(Window.partitionBy('customer_id').orderBy(desc('order_date')))).filter(col('rank') <= orders_count).groupBy('customer_id').agg(func.collect_list('product_id').alias('products')).collect()}

    if limit:
        customer_ids = list(c2p_df.agg(func.collect_set('customer_id')).collect()[0]['collect_set(customer_id)'])[:limit]
        c2p_df = c2p_df.filter(col('customer_id').isin(customer_ids))


    c2p_df = c2p_df.select(['customer_id', 'order_id', 'product_id', 'order_date']).distinct().withColumn('rank', func.dense_rank().over(Window.partitionBy('customer_id').orderBy(desc('order_date')))).filter(col('rank') <= orders_count).select(['customer_id', 'product_id']).distinct().withColumnRenamed('product_id', 'product')

    c2r_df = c2p_df.join(df, on='product', how='inner').groupBy(['customer_id', 'recommendation']).agg({'similarity': 'avg'}).withColumnRenamed('avg(similarity)', 'similarity').withColumn('rank', func.dense_rank().over(Window.partitionBy('customer_id').orderBy(desc('similarity')))).filter(col('rank') <= 100)
    c2r_df = c2r_df.groupBy("customer_id").agg(func.collect_list(func.struct('recommendation', 'similarity')).alias('recommendation_struct_list'))
    customer_2_products = {row['customer_id']: row['products'] for row in c2p_df.groupBy(['customer_id']).agg(func.collect_list('product').alias('products')).collect()}
    sort_udf = udf(lambda customer_id, l: [ele[0] for ele in sorted(l, key=lambda e: -e[1]) if ele[0] not in customer_2_products[customer_id]])
    c2r_df = c2r_df.select("customer_id", sort_udf("customer_id", "recommendation_struct_list").alias("recommendations"))

    rows = []
    for row in c2r_df.collect():
        rows.append((platform, row['customer_id'], 'user', 'bought', algo, str(row['recommendations'])))

    print('Total number of customers: %d' % len(rows))
    MysqlRedshiftUtils.add_recommendations_in_mysql(MysqlRedshiftUtils.mlMysqlConnection(), 'recommendations_v2', rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--hours', type=int)
    parser.add_argument('--days', type=int)
    parser.add_argument('--start-datetime')

    parser.add_argument('--end-datetime')
    parser.add_argument('--customer-id')
    parser.add_argument('--algo', default='average_coccurence_direct')
    parser.add_argument('--use-months', type=int, default=6)
    parser.add_argument('--orders-count', type=int, default=10)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--platform', choices=['nykaa','men'], default='nykaa')

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    limit = argv.get('limit')
    platform = argv.get('platform')
    algo = argv.get('algo')
    customer_id = argv.get('customer_id')
    orders_count = argv.get('orders_count')
    use_months = argv.get('use_months')

    start_datetime = argv.get('start_datetime')
    if not start_datetime and (argv.get('hours') or argv.get('days')):
        start_datetime = datetime.now()
        if argv.get('hours'):
            start_datetime -= timedelta(hours=argv['hours'])
        if argv.get('days'):
            start_datetime -= timedelta(days=argv['days'])
    end_datetime = argv.get('end_datetime')


    computation_start_datetime = datetime.now() - timedelta(days=use_months*30)

    print(RecoUtils.get_env_details())
    compute_recommendations(algo, platform, start_datetime, computation_start_datetime, None, customer_id, limit, orders_count)
