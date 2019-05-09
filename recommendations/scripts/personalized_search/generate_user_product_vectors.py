#Usage
#python generate_user_product_vectors.py --bucket-name=nykaa-dev-recommendations --input-dir="gensim_models/raw_cab_2018_till_11_sept/metric_customer_id/similarity_product_id/topics_100" --vector-len=100 --store-in-db --user-json="user_vectors.json" --product-json="product_vectors.json"
import argparse
import json
import tempfile
import boto3
import os
import re
import json
from gensim import corpora, similarities
from gensim import models, matutils

import traceback
import psycopg2
import psycopg2
from collections import defaultdict
from contextlib import closing
import mysql.connector
from elasticsearch import helpers, Elasticsearch

#from pyspark.sql import SparkSession
#from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, BooleanType
#from pyspark.sql.functions import udf, col, desc
#import pyspark.sql.functions as func
import sys

#test.print_cool()
#from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils
#sys.path.append("/home/ubuntu/nykaa_scripts/utils")
#from gensimutils import GensimUtils
class S3Utils:

    @staticmethod
    def download_dir(client, resource, s3_dir, dist, local, bucket):
        paginator = client.get_paginator('list_objects')
        for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
            if result.get('CommonPrefixes') is not None:
                for subdir in result.get('CommonPrefixes'):
                    S3Utils.download_dir(client, resource, s3_dir, subdir.get('Prefix'), local, bucket)
            if result.get('Contents') is not None:
                for file in result.get('Contents'):
                    if not os.path.exists(os.path.dirname(local + os.sep + file.get('Key'))):
                        os.makedirs(os.path.dirname(local + os.sep + file.get('Key')))
                    local_path = local + os.sep + re.sub('^%s/' % s3_dir, "", file.get('Key'))
                    if not os.path.exists(local_path[0:local_path.rfind("/")]):
                        os.makedirs(local_path[0:local_path.rfind('/')])
                    resource.meta.client.download_file(bucket, file.get('Key'), local_path)

class GensimUtils:

    @staticmethod
    def get_models(bucket_name, input_dir):
        with tempfile.TemporaryDirectory() as temp_dir:
            S3Utils.download_dir(boto3.client('s3'), boto3.resource('s3'), input_dir, input_dir, temp_dir, bucket_name)
            with open(temp_dir + '/metric_corpus_dict.json') as f:
                metric_corpus_dict = json.load(f)
            #tfidf = models.TfidfModel.load(temp_dir + '/tfidf/model.tfidf')
            #tfidf_lsi = models.LsiModel.load(temp_dir + '/tfidf_lsi/model.tfidf_lsi')
            lsi = models.LsiModel.load(temp_dir + '/lsi/model.lsi')
        return (metric_corpus_dict, lsi)#(metric_corpus_dict, tfidf, tfidf_lsi, lsi)

    @staticmethod
    def generate_complete_vectors(vector_tuple, vector_len):
        vector_dict = dict(vector_tuple)
        return [vector_dict.get(i, 0) for i in range(vector_len)]

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
    def mlMysqlConnection(env, connection_details=None):
        if env == 'prod':
            host = "ml-db-master.nykaa-internal.com"
            password = 'fjxcneXnq2gptsTs'
        elif env in ['non_prod', 'preprod', 'qa']:
            host = 'nka-preprod-ml.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
            password = 'JKaPHGB4JWXM'
        else:
            raise Exception('Unknow env')
        user = 'ml'
        db = 'nykaa_ml'
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
        es_conn = DiscUtils.esConn(env)
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
                response = es_conn.search(index='livecore', body=query, scroll='15m')
            else:
                response = es_conn.scroll(scroll_id=scroll_id, scroll='15m')

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

def _add_embedding_vectors_in_mysql(cursor, table, rows):
    values_str = ", ".join(["(%s, %s, %s, %s)" for i in range(len(rows))]) 
    values = tuple([_i for row in rows for _i in row]) 
    insert_recommendations_query = """ INSERT INTO %s(entity_id, entity_type, algo, embedding_vector) 
        VALUES %s ON DUPLICATE KEY UPDATE embedding_vector=VALUES(embedding_vector)
    """ % (table, values_str) 
    values = tuple([str(_i) for row in rows for _i in row])
    cursor.execute(insert_recommendations_query, values) 

def add_embedding_vectors_in_mysql(db, table, rows):
    cursor = db.cursor() 
    for i in range(0, len(rows), 500):
        _add_embedding_vectors_in_mysql(cursor, table, rows[i:i+500]) 
        db.commit()
        print('Added %d rows to DB' % (i+1))

def get_vectors_from_mysql_for_es(connection, algo, sku=True):
    print("Getting vectors from mysql for es")
    query = "SELECT entity_id, embedding_vector FROM embedding_vectors WHERE entity_type='product' AND algo='%s'" % algo
    rows = DiscUtils.fetchResultsInBatch(connection, query, 1000)
    print("Total number of products from mysql: %d" % len(rows))
    embedding_vector_field_name = 'embedding_vector_%s' % algo
    if not sku:
        return [{'product_id': str(row[0]), embedding_vector_field_name: json.loads(row[1])} for row in rows]
    product_id_2_sku = {product_id: sku for sku, product_id in DiscUtils.scrollESForResults(env)['sku_2_product_id'].items()}
    docs = []
    for row in rows:
        if product_id_2_sku.get(row[0]):
            docs.append({'sku': product_id_2_sku[row[0]], embedding_vector_field_name: json.loads(row[1])})
    return docs



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for generating topics')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--algo', required=True)
    parser.add_argument('--add-vectors-from-mysql-to-es', action='store_true') 
    parser.add_argument('--bucket-name') 
    parser.add_argument('--input-dir') 
    parser.add_argument('--vector-len', type=int) 
    parser.add_argument('--product-json') 
    parser.add_argument('--user-json') 
    parser.add_argument('--store-in-db', action='store_true')
    parser.add_argument('--add-only-products', action='store_true')
    parser.add_argument('--add-in-es', action='store_true')
    parser.add_argument('--env', required=True)
    parser.add_argument('--limit', type=int)

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    algo = argv['algo']
    env = argv['env']
    limit = argv.get('limit', -1)

    if argv['add_vectors_from_mysql_to_es']:
        docs = get_vectors_from_mysql_for_es(DiscUtils.mlMysqlConnection(env), algo)
        for i in range(0, len(docs), 1000):
            DiscUtils.updateESCatalog(docs[i:i+1000])
        exit()

    bucket_name = argv['bucket_name']
    input_dir = argv['input_dir']
    vector_len = argv['vector_len']
    product_json = argv.get('product_json')
    user_json = argv.get('user_json')
    store_in_db = argv['store_in_db']
    add_in_es = argv['add_in_es']
    add_only_products = argv['add_only_products']

    print("Downloading the models")
    user_corpus_dict, user_lsi = GensimUtils.get_models(bucket_name, input_dir)
    #user_corpus_dict, user_tfidf, user_tfidf_lsi, user_lsi = GensimUtils.get_models(bucket_name, input_dir)

    print("Generating user vectors")
    user_vectors = {}

    norm_model = models.NormModel()
    if limit > 0:
        for i, (user_id, product_ids_bow) in enumerate(list(user_corpus_dict.items())):
            if i < limit:
                user_vectors[user_id] = matutils.unitvec(matutils.sparse2full(user_lsi[norm_model.normalize(product_ids_bow)], vector_len)).tolist() #GensimUtils.generate_complete_vectors(user_lsi[norm_model.normalize(product_ids_bow)], vector_len)
    else:
        for user_id, product_ids_bow in user_corpus_dict.items():
            user_vectors[user_id] = matutils.unitvec(matutils.sparse2full(user_lsi[norm_model.normalize(product_ids_bow)], vector_len)).tolist() #GensimUtils.generate_complete_vectors(user_lsi[norm_model.normalize(product_ids_bow)], vector_len)

    product_ids = list(set([product_tuple[0] for products_bow in user_corpus_dict.values() for product_tuple in products_bow]))
    print('Products from models: %d' % len(product_ids))

    print("Generating product vectors")
    product_vectors = {}
    for product_id in product_ids:
        product_vectors[str(product_id)] = matutils.unitvec(matutils.sparse2full(user_lsi[[[product_id, 1]]], vector_len)).tolist() #GensimUtils.generate_complete_vectors(user_lsi[[[product_id, 1]]], vector_len)

    if product_json and user_json:
        print("Writing json file")
        with open(user_json, 'w') as f:
            json.dump(user_vectors, f)

        with open(product_json, 'w') as f:
            json.dump(product_vectors, f)

    if store_in_db:
        print("Storing results in mysql DB")
        rows = []
        if not add_only_products:
            for user_id, vector in user_vectors.items():
                rows.append((user_id, 'user', algo, json.dumps(vector)))

        for product_id, vector in product_vectors.items():
            rows.append((product_id, 'product', algo, json.dumps(vector)))

        print('Total number of rows to be added to DB: %s' % len(rows))
        add_embedding_vectors_in_mysql(DiscUtils.mlMysqlConnection(env), 'embedding_vectors', rows)

    if add_in_es:
        print("Adding results in ES")
        product_id_2_sku = {product_id: sku for sku, product_id in DiscUtils.scrollESForResults(env)['sku_2_product_id'].items()}

        docs = []
        embedding_vector_field_name = "embedding_vector_" % algo
        for product_id, vector in product_vectors.items():
            docs.append({'sku': product_id_2_sku['product_id'], embedding_vector_field_name: vector})

        DiscUtils.updateESCatalog(docs)
