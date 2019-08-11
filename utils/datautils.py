import json
import glob
from datetime import datetime, timedelta, date

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, BooleanType
from pyspark.sql.functions import udf, col, desc
import pyspark.sql.functions as func
from pyspark.sql.window import Window

import sys
sys.path.append('/home/ubuntu/nykaa_scripts/utils')
sys.path.append('/home/hadoop/nykaa_scripts/utils')
from mysqlredshiftutils import MysqlRedshiftUtils
from esutils import ESUtils
from sparkutils import SparkUtils
from recoutils import RecoUtils
from s3utils import S3Utils
import constants as Constants

env_details = RecoUtils.get_env_details()

class DataUtils:

    def prepare_orders_dataframe(spark, platform, parented, start_datetime, end_datetime, customer_start_datetime, customer_ids=None):
        if platform == 'men':
            order_sources = Constants.ORDER_SOURCE_NYKAAMEN
        else:
            order_sources = Constants.ORDER_SOURCE_NYKAA

        customer_orders_query = "SELECT fact_order_new.nykaa_orderno as order_id, fact_order_new.order_customerid as customer_id, fact_order_detail_new.product_id, fact_order_detail_new.product_sku, order_date from fact_order_new INNER JOIN fact_order_detail_new ON fact_order_new.nykaa_orderno=fact_order_detail_new.nykaa_orderno WHERE fact_order_new.order_status<>'Cancelled' AND fact_order_new.nykaa_orderno <> 0 AND product_mrp > 1 AND order_source IN (" + ",".join([("'%s'" % source) for source in order_sources]) + ") AND order_customerid IS NOT NULL %s %s " % (" AND order_date <= '%s' " % end_datetime if end_datetime else "", " AND order_date >= '%s' " % start_datetime if start_datetime else "")
        if customer_start_datetime:
            customer_orders_query += " AND fact_order_new.order_customerid IN (SELECT DISTINCT(order_customerid) FROM fact_order_new WHERE order_date > '%s')" % customer_start_datetime
        if customer_ids:
            customer_orders_query += " AND fact_order_new.order_customerid IN (%s)" % ",".join([str(c) for c in customer_ids])

        print(customer_orders_query)
        print('Fetching Data from Redshift')
        rows = MysqlRedshiftUtils.fetchResultsInBatch(MysqlRedshiftUtils.redshiftConnection(), customer_orders_query, 10000)
        print('Data fetched')

        df = spark.createDataFrame(rows, SparkUtils.ORDERS_SCHEMA)
        print('Total number of rows fetched: %d' % df.count())
        df.printSchema()
        print("Drop null values")
        df = df.dropna()
        print('Total number of rows extracted: %d' % df.count())
        print('Total number of products: %d' % df.select('product_id').distinct().count())
        print('Scrolling ES for results')
        results = ESUtils.scrollESForResults()
        print('Scrolling ES done')
        child_2_parent = results['child_2_parent']
        sku_2_product_id = results['sku_2_product_id']

        def convert_sku_to_product_id(sku, product_id):
            return sku_2_product_id.get(sku, product_id)

        convert_sku_to_product_id_udf = udf(convert_sku_to_product_id, IntegerType())
        print('Converting sku to product_id')
        df = df.withColumn("product_id", convert_sku_to_product_id_udf(df['product_sku'], df['product_id']))
        print('Total number of rows extracted: %d' % df.count())
        print('Total number of products: %d' % df.select('product_id').distinct().count())

        df = df.dropna()
        def convert_to_parent(product_id):
            return child_2_parent.get(product_id, product_id)

        convert_to_parent_udf = udf(convert_to_parent, IntegerType())
        if parented:
            print('Converting product_id to parent')
            df = df.withColumn("product_id", convert_to_parent_udf(df['product_id']))
        else:
            print('Adding separate parent for the product')
            df = df.withColumn("parent_product_id", convert_to_parent_udf(df['product_id']))
 
        product_2_l3_category = {product_id: json.loads(categories[0])['l3']['id'] for product_id, categories in results['primary_categories'].items()}
        results['product_2_l3_category'] = product_2_l3_category

        l3_udf = udf(lambda product_id: product_2_l3_category.get(product_id), StringType())
        df = df.withColumn('l3_category', l3_udf('product_id'))
        #TODO need to remove the below line
        df = df.filter(col('l3_category') != 'LEVEL')
        df = df.withColumn('l3_category', col('l3_category').cast('int'))
        return df, results

    def prepare_views_ca_dataframe(spark, files, s3_prefix, local_dir_path, parented=True, data_file=False, drop_l3_nulls=False):
        schema = StructType([
            StructField("Date", StringType(), True),
            StructField("Customer ID (evar23)", IntegerType(), True),
            StructField("Products", IntegerType(), True),
            StructField("Product Views", IntegerType(), True),
            StructField("Cart Additions", IntegerType(), True)])

        if not data_file:
            if not env_details['is_emr']:
                S3Utils.download_dir(s3_prefix, s3_prefix, local_dir_path, env_details['bucket_name'])
                files = [f for f in glob.glob(local_dir_path + s3_prefix + "**/*.csv", recursive=True)]

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
        if parented:
            print('Converting product_id to parent')
            df = df.withColumn("product_id", convert_to_parent_udf(df['product_id']))
        else:
            print('Adding separate parent for the product')
            df = df.withColumn("parent_product_id", convert_to_parent_udf(df['product_id']))

        product_2_l3_category = {product_id: json.loads(categories[0])['l3']['id'] for product_id, categories in results['primary_categories'].items()}
        results['product_2_l3_category'] = product_2_l3_category
        l3_udf = udf(lambda product_id: product_2_l3_category.get(product_id), StringType())
        df = df.withColumn('l3_category', l3_udf('product_id'))
        print("Filtering out products with l3_category = LEVEL")
        df = df.filter(col('l3_category') != 'LEVEL')
        print("Total Number of rows now: %d" % df.count())
        df = df.withColumn('l3_category', col('l3_category').cast('int'))

        if drop_l3_nulls:
            print("Dropping nulls after l3 category addition for products")
            df = df.dropna()
            print("Total Number of rows now: %d" % df.count())

        df = df.withColumn("date", udf(lambda d: datetime.strptime(d, '%B %d, %Y'), DateType())(col('date')))

        return df, results
