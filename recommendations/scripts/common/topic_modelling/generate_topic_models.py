# Usage

#aws emr create-cluster --name "Topic Modelling customer_id:product_id:100" --tags "Owner=Ashwin" --release-label emr-5.14.0 --instance-type m5.24xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=nka-qa-emr,SubnetId=subnet-2b4c085c --ebs-root-volume-size 100 --bootstrap-actions Path="s3://nykaa-dev-recommendations/topic_modelling_download.sh" --log-uri "s3://nykaa-dev-recommendations/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://nykaa-dev-recommendations/generate_topic_models.py,"--input-csv","s3://nykaa-dev-recommendations/raw_cab_2018_till_6_oct.csv","--bucket-name","nykaa-dev-recommendations","--output-dir","gensim_models/raw_cab_2018_till_6_oct","--metric","customer_id","--similarity-metric","product_id","--discarded-metric","order_id","--num-topics","100","-m","tfidf","lsi","tfidf_lsi"] --use-default-roles --auto-terminate --configurations file://config.json

#python generate_topic_models.py --input-csv='s3://nykaa-dev-recommendations/raw_cab_july.csv' --bucket-name=nykaa-dev-recommendations --output-dir='gensims_models' --metric=product_id --similarity-metric=order_id --discarded-metric=customer_id --num-topics=300 -m tfidf lsi

import os
import boto3
import json
import pandas as pd
from collections import defaultdict
import argparse
from gensim import corpora, models, similarities
from contextlib import closing
import traceback
import tempfile
import psycopg2
import sys
from elasticsearch import helpers, Elasticsearch


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType
from pyspark.sql.functions import udf
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("Topic Modelling").getOrCreate()
 
sc = spark.sparkContext
print(sc.getConf().getAll())

class Utils:

    @staticmethod
    def mysqlConnection(env, connection_details=None):
        if env == 'prod':
            host = "dbmaster.ciel4c1bqlwh.ap-southeast-1.rds.amazonaws.com"
            password = 'Cheaj92pDHtDq8hU'
        elif env in ['non_prod', 'preprod']:
            host = 'price-api-preprod.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
            password = 'yNKNy33xG'
        elif env == 'qa':
            host = 'price-api-qa.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
            password = 'yNKNy33xG'
        else:
            raise Exception('Unknow env')
        user = 'recommendation'
        #user = 'api'
        #password = 'aU%v#sq1'
        db = 'nykaa'
        for i in [0, 1, 2]:
            try:
                if connection_details is not None and isinstance(connection_details, dict):
                    connection_details['host'] = host
                    connection_details['user'] = user
                    connection_details['password'] = password
                    connection_details['database'] = db
                return mysql.connector.connect(host=host, user=user, password=password, database=db)
            except:
                print("MySQL connection failed! Retyring %d.." % i)
                if i == 2:
                    print(traceback.format_exc())
                    print("MySQL connection failed 3 times. Giving up..")
                    raise


    @staticmethod
    def esConn(env):
        if env == 'prod':
            ES_ENDPOINT = 'vpc-prod-api-vzcc4i4e4zk2w4z45mqkisjo4u.ap-southeast-1.es.amazonaws.com'
        elif env in ['non_prod', 'preprod']:
            ES_ENDPOINT = 'search-preprod-api-ub7noqs5xxaerxm6vhv5yjuc7u.ap-southeast-1.es.amazonaws.com'
        elif env == 'qa':
            ES_ENDPOINT = 'search-qa-api-fvmcnxoaknewsdvt6gxgdtmodq.ap-southeast-1.es.amazonaws.com'
        else:
            raise Exception('Unknown env')
        print(ES_ENDPOINT)
        es = Elasticsearch(['http://%s:80' % ES_ENDPOINT])
        return es

    @staticmethod
    def scrollESForResults(env):
        es_conn = Utils.esConn(env)
        ES_BATCH_SIZE = 10000
        scroll_id = None
        luxe_products = []
        product_2_mrp = {}
        child_2_parent = {}
        primary_categories = {}
        brand_facets = {}
        sku_2_product_id = {}
        product_2_image = {}

        while True:
            if not scroll_id:
                query = { 
                    "size": ES_BATCH_SIZE,
                    "query": { "match_all": {} },
                    "_source": ["product_id", "is_luxe", "mrp", "parent_id", "primary_categories", "brand_facet", "sku", "media"]
                }
                response = es_conn.search(index='livecore', body=query, scroll='2m')
            else:
                response = es_conn.scroll(scroll_id=scroll_id, scroll='2m')

            if not response['hits']['hits']:
                break

            scroll_id = response['_scroll_id']
            luxe_products += [int(p['_source']['product_id']) for p in response['hits']['hits'] if p["_source"]["is_luxe"]]
            product_2_mrp.update({int(p["_source"]["product_id"]): p["_source"]["mrp"] for p in response["hits"]["hits"]})
            child_2_parent.update({int(p["_source"]["product_id"]): int(p["_source"]["parent_id"]) for p in response["hits"]["hits"]})
            primary_categories.update({int(p["_source"]["product_id"]): p["_source"]["primary_categories"] for p in response["hits"]["hits"]})
            brand_facets.update({int(p["_source"]["product_id"]): p["_source"].get("brand_facet") for p in response["hits"]["hits"] if p["_source"].get("brand_facet")})
            sku_2_product_id.update({p["_source"]["sku"]: int(p["_source"]["product_id"]) for p in response["hits"]["hits"]})
            product_2_image.update({int(p["_source"]["product_id"]): p['_source']['media'] for p in response['hits']['hits'] if p['_source'].get('media')})

        return {'luxe_products': luxe_products, 'product_2_mrp': product_2_mrp, 'child_2_parent': child_2_parent, 'primary_categories': primary_categories, 'brand_facets': brand_facets, 'sku_2_product_id': sku_2_product_id, 'product_2_image': product_2_image}


    @staticmethod
    def redshiftConnection(env):
        if env in ['prod', 'non_prod']:
            host = 'dwhcluster.cy0qwrxs0juz.ap-southeast-1.redshift.amazonaws.com'
        elif env in ['non_prod', 'preprod', 'qa']:
            host = 'nka-preprod-dwhcluster.c742iibw9j1g.ap-southeast-1.redshift.amazonaws.com'
        else:
            raise Exception('Unknown env')
        port = 5439
        username = 'dwh_redshift_ro'
        password = 'GSrjC7hYPC9V'
        dbname = 'datawarehouse'
        con = psycopg2.connect(dbname=dbname, host=host, port=port, user=username, password=password)
        return con

    @staticmethod
    def fetchResultsInBatch(connection, query, batch_size):
        rows= []
        with closing(connection.cursor()) as cursor:
            cursor.execute(query)
            while True:
                batch_empty = True
                for row in cursor.fetchmany(batch_size):
                    batch_empty = False
                    rows.append(row)
                if batch_empty:
                    break
        return rows

def s3_upload_dir(s3, local_directory, bucket_name, destination):
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            print('Uploading file: %s' % filename)
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(destination, relative_path)
            s3.upload_file(local_path, bucket_name, s3_path)

def prepare_orders_dataframe(env, start_datetime, limit):
    print("Preparing orders data")
    customer_orders_query = "SELECT fact_order_new.nykaa_orderno as order_id, fact_order_new.order_customerid as customer_id, fact_order_detail_new.product_id, fact_order_detail_new.product_sku from fact_order_new INNER JOIN fact_order_detail_new ON fact_order_new.nykaa_orderno=fact_order_detail_new.nykaa_orderno WHERE fact_order_new.nykaa_orderno <> 0 AND product_mrp > 1 AND order_customerid IS NOT NULL %s %s" % (" AND order_date >= '%s' " % start_datetime if start_datetime else "", " limit %d" % limit if limit else "")
    print(customer_orders_query)
    print('Fetching Data from Redshift')
    rows = Utils.fetchResultsInBatch(Utils.redshiftConnection(env), customer_orders_query, 10000)
    print('Data fetched')
    schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("product_sku", StringType(), True)])

    df = spark.createDataFrame(rows, schema)
    df.printSchema()
    print('Scrolling ES for results')
    results = Utils.scrollESForResults(env)
    print('Scrolling ES done')
    child_2_parent = results['child_2_parent']
    sku_2_product_id = results['sku_2_product_id']

    def convert_sku_to_product_id(sku, product_id):
        return sku_2_product_id.get(sku, product_id)

    convert_sku_to_product_id_udf = udf(convert_sku_to_product_id, IntegerType())
    print('Converting sku to product_id')
    df = df.withColumn("product_id", convert_sku_to_product_id_udf(df['product_sku'], df['product_id']))

    def convert_to_parent(product_id):
        return child_2_parent.get(product_id, product_id)

    convert_to_parent_udf = udf(convert_to_parent, IntegerType())
    print('Converting product_id to parent')
    df = df.withColumn("product_id", convert_to_parent_udf(df['product_id']))
    df = df.na.drop()
    df = df.drop("product_sku")
    df = df.select(['order_id', 'customer_id', 'product_id']).distinct()
    print('Data preparation done, returning dataframe')
    return df, results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for generating topics')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--env', '-e', required=True) 
    parser.add_argument('--start-datetime') 
    parser.add_argument('--limit', '-l', type=int) 
    parser.add_argument('--bucket-name', '-b', required=True) 
    parser.add_argument('--output-dir', '-o', required=True) 
    parser.add_argument('--metric', required=True) 
    parser.add_argument('--similarity-metric', '-s', required=True) 
    parser.add_argument('--discarded-metric', '-d', required=True) 
    parser.add_argument('--num-topics', '-t', type=int, required=True) 
    parser.add_argument('--models', '-m', nargs='+') 

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    #input_csv = argv['input_csv']
    env = argv['env']
    start_datetime = argv['start_datetime']
    limit = argv['limit']
    bucket_name = argv['bucket_name']
    output_dir = argv['output_dir']
    metric = argv['metric']
    similarity_metric = argv['similarity_metric']
    discarded_metric = argv['discarded_metric']
    num_topics = argv['num_topics']
    topic_models = argv['models']
    metric_output_dir = "%s/metric_%s/similarity_%s/topics_%d" % (output_dir, metric, similarity_metric, num_topics)
    complete_metric_output_dir = "s3://%s/%s" % (bucket_name, metric_output_dir)

    s3 = boto3.client('s3')

    df, results = prepare_orders_dataframe(env, start_datetime, limit)

    if verbose:
        print("After filtering out null values from dataframe, total number of rows: %d" % df.count())

    df = df.groupBy([similarity_metric, metric]).count()

    metric_corpus_dict= defaultdict(lambda: [])
    for row in df.collect():
        metric_corpus_dict[row[metric]].append((int(row[similarity_metric]), int(row['count'])))

    metric_corpus_dict_str = defaultdict(lambda: [])

    for key, lt in metric_corpus_dict.items():
        metric_corpus_dict_str[str(key)] = lt

    if verbose:
        print("Uploading metric corpus dict onto s3")
    s3.put_object(Bucket=bucket_name, Key=metric_output_dir+'/metric_corpus_dict.json', Body=json.dumps(metric_corpus_dict_str, indent=4))
    metric_corpus = []
    for key, value in metric_corpus_dict.items():
        metric_corpus.append(value)

    if 'tfidf' in topic_models or 'tfidf_lsi' in topic_models:
        if verbose:
            print('Making tfidf model')
        tfidf = models.TfidfModel(metric_corpus)
        metric_corpus_tfidf = tfidf[metric_corpus]
        with tempfile.TemporaryDirectory() as temp_dir:
            tfidf.save(temp_dir + '/model.tfidf')
            if verbose:
                print('Uploading tfidf onto s3')
            s3_upload_dir(s3, temp_dir, bucket_name, metric_output_dir + '/tfidf')


    if 'tfidf_lsi' in topic_models:
        with tempfile.TemporaryDirectory() as temp_dir:
            if verbose:
                print('Making tfidf_lsi model')
            lsi = models.LsiModel(metric_corpus_tfidf, num_topics=num_topics)
            lsi.save(temp_dir + '/model.tfidf_lsi')
            if verbose:
                print('Uploading tfidf_lsi onto s3')
            s3_upload_dir(s3, temp_dir, bucket_name, metric_output_dir + '/tfidf_lsi')

    if 'lsi' in topic_models:
        with tempfile.TemporaryDirectory() as temp_dir:
            norm_model = models.NormModel()
            normalized_metric_corpus = []
            if verbose:
                print('Normalizing the corpus')
            for doc in metric_corpus:
                normalized_metric_corpus.append(norm_model.normalize(doc))
            if verbose:
                print('Making lsi model')
            lsi = models.LsiModel(normalized_metric_corpus, num_topics=num_topics)
            lsi.save(temp_dir + '/model.lsi')
            if verbose:
                print('Uploading lsi model onto s3')
            s3_upload_dir(s3, temp_dir, bucket_name, metric_output_dir + '/lsi')

    #with tempfile.TemporaryDirectory() as temp_dir:
    #    lda = models.LdaModel(metric_corpus, num_topics=num_topics)
    #    lda.save(temp_dir + '/model.lda')
    #    s3.upload_file(temp_dir + '/model.lda', bucket_name, metric_output_dir + '/lda/model.lda')
