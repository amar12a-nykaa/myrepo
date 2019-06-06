from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, BooleanType, ArrayType

import sys
sys.path.append('/home/ubuntu/nykaa_scripts/utils')
sys.path.append('/home/hadoop/nykaa_scripts/utils')
from recoutils import RecoUtils

class SparkUtils:
    ORDERS_SCHEMA = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("product_sku", StringType(), True),
            StructField("order_date", TimestampType(), True)])

    def get_spark_instance(name='Spark Instance'):
        env_details = RecoUtils.get_env_details()
        if env_details['is_emr'] == True:
            return SparkSession.builder.appName(name).getOrCreate()
        else:
            return SparkSession.builder \
                    .master("local[10]") \
                    .appName(name) \
                    .config("spark.executor.memory", "4G") \
                    .config("spark.storage.memoryFraction", 0.4) \
                    .config("spark.driver.memory", "12G") \
                    .getOrCreate()
