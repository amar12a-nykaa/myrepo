import os
import socket

from pyspark.sql import SparkSession

if os.environ.get('NYKAA_EMR_ENVIRONMENT'):
    env = os.environ['NYKAA_EMR_ENVIRONMENT']
else:
    env = socket.gethostname()

class RecoUtils:

    def get_env_details():
        if env in ['admin', 'prod-emr']:
            return {'bucket_name': 'nykaa-recommendations', 'key_name': 'nka-prod-emr', 'subnet_id': 'subnet-7c467d18', 'env': 'prod'}
        else:
            return {'bucket_name': 'nykaa-dev-recommendations', 'key_name': 'nka-qa-emr', 'subnet_id': 'subnet-6608c22f', 'env': 'non_prod'}

    def get_spark_instance(name='Spark Instance'):
        if env in ['prod-emr', 'dev-emr']:
            return SparkSession.builder.appName(name).getOrCreate()
        else:
            return SparkSession.builder \
                    .master("local[10]") \
                    .appName(name) \
                    .config("spark.executor.memory", "4G") \
                    .config("spark.storage.memoryFraction", 0.4) \
                    .config("spark.driver.memory", "12G") \
                    .getOrCreate()


