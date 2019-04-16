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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, FloatType, BooleanType
from pyspark.sql.functions import udf, col, desc
import pyspark.sql.functions as func
from IPython import embed

#sys.path.append("/home/apis/nykaa")
#from pas.v2.utils import Utils, RecommendationsUtils
ORDER_SOURCE_NYKAA = ['Nykaa', 'Nykaa(Old)', 'NYKAA', 'CS-Manual']
ORDER_SOURCE_NYKAAMEN = ['NykaaMen']

#spark = SparkSession.builder.appName("CAB").getOrCreate()
spark = SparkSession.builder \
            .master("local[6]") \
            .appName("CAB") \
            .config("spark.executor.memory", "4G") \
            .config("spark.storage.memoryFraction", 0.4) \
            .config("spark.driver.memory", "26G") \
            .getOrCreate()
 
sc = spark.sparkContext
print(sc.getConf().getAll())

class RecommendationsUtils:

    @staticmethod
    def _add_recommendations_in_mysql(cursor, table, rows):
        values_str = ", ".join(["(%s, %s, %s, %s, %s, %s)" for i in range(len(rows))])
        values = tuple([_i for row in rows for _i in row])
        insert_recommendations_query = """ INSERT INTO %s(catalog_tag_filter, entity_id, entity_type, recommendation_type, algo, recommended_products_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json)
        """ % (table, values_str)
        values = tuple([str(_i) for row in rows for _i in row])
        print(insert_recommendations_query)
        print(values)
        cursor.execute(insert_recommendations_query, values)

    @staticmethod
    def add_recommendations_in_mysql(db, table, rows):
        cursor = db.cursor()
        for i in range(0, len(rows), 100):
            RecommendationsUtils._add_recommendations_in_mysql(cursor, table, rows[i:i+100])
            db.commit()


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
                response = es_conn.search(index='livecore', body=query, scroll='15m')
            else:
                response = es_conn.scroll(scroll_id=scroll_id, scroll='15m')

            if not response['hits']['hits']:
                break

            scroll_id = response['_scroll_id']
            luxe_products += [int(p['_source']['product_id']) for p in response['hits']['hits'] if p["_source"].get("is_luxe") and p["_source"]["is_luxe"]]
            product_2_mrp.update({int(p["_source"]["product_id"]): p["_source"]["mrp"] for p in response["hits"]["hits"] if p["_source"].get("mrp")})
            child_2_parent.update({int(p["_source"]["product_id"]): int(p["_source"]["parent_id"]) for p in response["hits"]["hits"] if p["_source"].get("parent_id")})
            primary_categories.update({int(p["_source"]["product_id"]): p["_source"]["primary_categories"] for p in response["hits"]["hits"] if p["_source"].get("primary_categories")})
            brand_facets.update({int(p["_source"]["product_id"]): p["_source"].get("brand_facet") for p in response["hits"]["hits"] if p["_source"].get("brand_facet")})
            sku_2_product_id.update({p["_source"]["sku"]: int(p["_source"]["product_id"]) for p in response["hits"]["hits"] if p["_source"].get("sku")})
            product_2_image.update({int(p["_source"]["product_id"]): p['_source']['media'] for p in response['hits']['hits'] if p['_source'].get('media')})

        return {'luxe_products': luxe_products, 'product_2_mrp': product_2_mrp, 'child_2_parent': child_2_parent, 'primary_categories': primary_categories, 'brand_facets': brand_facets, 'sku_2_product_id': sku_2_product_id, 'product_2_image': product_2_image}


    @staticmethod
    def redshiftConnection(env):
        if env == 'prod':
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

def prepare_orders_dataframe(env, platform, start_datetime, end_datetime, limit, separate_parent=False):
    print("Preparing orders data")

    if platform == 'men':
        order_sources = ORDER_SOURCE_NYKAAMEN
    else:
        order_sources = ORDER_SOURCE_NYKAA

    customer_orders_query = "SELECT fact_order_new.nykaa_orderno as order_id, fact_order_new.order_customerid as customer_id, fact_order_detail_new.product_id, fact_order_detail_new.product_sku from fact_order_new INNER JOIN fact_order_detail_new ON fact_order_new.nykaa_orderno=fact_order_detail_new.nykaa_orderno WHERE fact_order_new.order_status<>'Cancelled' AND fact_order_new.nykaa_orderno <> 0 AND product_mrp > 1 AND order_source IN (" + ",".join([("'%s'" % source) for source in order_sources]) + ") AND order_customerid IS NOT NULL %s %s %s" % (" AND order_date <= '%s' " % end_datetime if end_datetime else "", " AND order_date >= '%s' " % start_datetime if start_datetime else "", " limit %d" % limit if limit else "")
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
    print('Total number of rows fetched: %d' % df.count())
    df.printSchema()
    print('Total number of rows extracted: %d' % df.count())
    print('Total number of products: %d' % df.select('product_id').distinct().count())
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
    print('Total number of rows extracted: %d' % df.count())
    print('Total number of products: %d' % df.select('product_id').distinct().count())

    def convert_to_parent(product_id):
        return child_2_parent.get(product_id, product_id)

    convert_to_parent_udf = udf(convert_to_parent, IntegerType())
    if separate_parent:
        print('Adding separate parent for the product')
        df = df.withColumn("parent_product_id", convert_to_parent_udf(df['product_id']))
    else:
        print('Converting product_id to parent')
        df = df.withColumn("product_id", convert_to_parent_udf(df['product_id']))
    
    print('Total number of rows extracted: %d' % df.count())
    print('Total number of products: %d' % df.select('product_id').distinct().count())

    print('Dropping na')
    df = df.na.drop()
    print('Total number of rows extracted: %d' % df.count())
    print('Total number of products: %d' % df.select('product_id').distinct().count())
    df = df.drop("product_sku")

    if separate_parent:
        print('Selecting distinct(order_id, customer_id, parent_product_id, product_id)')
        df = df.select(['order_id', 'customer_id', 'parent_product_id', 'product_id']).distinct()
    else:
        print('Selecting distinct(order_id, customer_id, product_id)')
        df = df.select(['order_id', 'customer_id', 'product_id']).distinct()
    print('Total number of rows extracted: %d' % df.count())
    print('Total number of products: %d' % df.select('product_id').distinct().count())

    print('Data preparation done, returning dataframe')
    return df, results

def compute_fbt(env, platform, start_datetime=None, end_datetime=None, limit=None):
    df, results = prepare_orders_dataframe(env, platform, start_datetime, end_datetime, limit, separate_parent=True)
    popular_variant_df = df.select(['parent_product_id', 'product_id']).groupBy(['parent_product_id', 'product_id']).count().sort(col('count').desc()).drop_duplicates(['parent_product_id'])
    popular_variant = {row['parent_product_id']: row['product_id'] for row in popular_variant_df.collect()}
    df.printSchema()
    product_to_orders_count_df = df.groupBy('parent_product_id').agg(func.countDistinct('order_id')).withColumnRenamed('count(DISTINCT order_id)', 'orders_count').toPandas()
    product_to_orders_count = dict(zip(product_to_orders_count_df.parent_product_id, product_to_orders_count_df.orders_count))
    df = df.withColumnRenamed('parent_product_id', 'parent_product_id_x').join(df.withColumnRenamed('parent_product_id', 'parent_product_id_y'), on="order_id", how="inner")
    df = df.select(['parent_product_id_x', 'parent_product_id_y', 'order_id']).distinct()
    df = df[df.parent_product_id_x < df.parent_product_id_y]
    df = df.groupBy(['parent_product_id_x', 'parent_product_id_y']).agg({'order_id': 'count'})
    df = df.withColumnRenamed("count(order_id)", 'orders_intersection')

    df = df[df.orders_intersection >= 2]

    def compute_union_len(product_id_x, product_id_y, orders_intersection):
        return product_to_orders_count[product_id_x] + product_to_orders_count[product_id_y] - orders_intersection

    compute_union_len_udf = udf(compute_union_len, IntegerType())
    df = df.withColumn("orders_union", compute_union_len_udf(df['parent_product_id_x'], df['parent_product_id_y'], df['orders_intersection']))
    
    def compute_similarity(orders_intersection, orders_union):
        return orders_intersection/orders_union

    compute_similarity_udf = udf(compute_similarity, FloatType())
    df = df.withColumn("similarity", compute_similarity_udf(df['orders_intersection'], df['orders_union']))

    v3_similar_products_dict = defaultdict(lambda: [])
    direct_similar_products_dict = defaultdict(lambda: [])

    categories = defaultdict(lambda: {'l1':[], 'l2': [], 'l3': []})
    brands = defaultdict(lambda: [])
    primary_categories = results['primary_categories']

    for product_id, categories_str_list in primary_categories.items():
        for categories_str in categories_str_list:
            for category, obj in json.loads(categories_str).items():
                if category in ['l1', 'l2', 'l3']:
                    categories[product_id][category].append(obj['id'])

    for product_id, brand_str_list in results['brand_facets'].items():
        for brand_str in brand_str_list:
            brands[product_id].append(json.loads(brand_str)['id'])

    parent_2_children = defaultdict(lambda: [])
    for child, parent in results['child_2_parent'].items():
        parent_2_children[parent].append(child)

    for row in df.collect():
        if set(brands[row['parent_product_id_x']]).intersection(set(brands[row['parent_product_id_y']])) \
            and (not set(categories[row['parent_product_id_x']]['l3']).intersection(set(categories[row['parent_product_id_y']]['l3']))) \
            and set(categories[row['parent_product_id_x']]['l1']).intersection(set(categories[row['parent_product_id_y']]['l1'])):
            v3_similar_products_dict[row['parent_product_id_x']].append((popular_variant.get(row['parent_product_id_y'], row['parent_product_id_y']), row['orders_intersection']))
            v3_similar_products_dict[row['parent_product_id_y']].append((popular_variant.get(row['parent_product_id_x'], row['parent_product_id_x']), row['orders_intersection']))
        direct_similar_products_dict[row['parent_product_id_x']].append((row['parent_product_id_y'], row['similarity']))
        direct_similar_products_dict[row['parent_product_id_y']].append((row['parent_product_id_x'], row['similarity']))

    print('Total Number of parent products: %d' % len(parent_2_children))
    print('Total Number of child products: %d' % sum([len(variants) for parent, variants in parent_2_children.items()]))

    rows = []
    product_ids_updated = []
    for product_id in direct_similar_products_dict:
        product_ids_updated.append(product_id)
        v3_similar_products = list(map(lambda e: int(e[0]), sorted(v3_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        direct_similar_products = list(map(lambda e: int(e[0]), sorted(direct_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        rows.append((platform, product_id, 'product', 'fbt', 'v3', str(v3_similar_products)))
        rows.append((platform, product_id, 'product', 'fbt', 'coccurence_direct', str(direct_similar_products)))
        variants = parent_2_children.get(product_id, [])
        for variant in variants:
            product_ids_updated.append(variant)
            rows.append((platform, variant, 'product', 'fbt', 'v3', str(v3_similar_products)))
            rows.append((platform, variant, 'product', 'fbt', 'coccurence_direct', str(direct_similar_products)))

    print('Adding recommendations for %d products in DB' % len(product_ids_updated))
    RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection(env), 'recommendations_v2', rows)

def compute_recommendations(env, algo, platform, start_datetime=None, end_datetime=None, customer_id=None, limit=None):
    print("Computing u2p")
    df, results = prepare_orders_dataframe(env, platform, start_datetime, end_datetime, limit)
    luxe_products_dict = {p:True for p in results['luxe_products']}
    df = df.select(['product_id', 'customer_id']).distinct()
    df.printSchema()
    print('Preparing product to customers count')
    #product_to_customers_count_df = df.groupBy('product_id').agg(func.countDistinct('customer_id')).withColumnRenamed('count(DISTINCT customer_id)', 'customers_count').toPandas()
    product_to_customers_count_df = df.groupBy('product_id').count().withColumnRenamed('count', 'customers_count').toPandas()
    product_to_customers_count = dict(zip(product_to_customers_count_df.product_id, product_to_customers_count_df.customers_count))
    print('Doing essential steps in computation')

    _df = df
    if customer_id:
        _df = _df.filter(col('customer_id') == customer_id)

    customer_2_products_purchased = {row['customer_id']: row['products_purchased'] for row in _df.groupBy('customer_id').agg(func.collect_list('product_id').alias('products_purchased')).collect()}

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

    direct_similar_products_dict = defaultdict(lambda: [])

    for row in df.collect():
        direct_similar_products_dict[row['product_id_x']].append((row['product_id_y'], row['similarity']))
        direct_similar_products_dict[row['product_id_y']].append((row['product_id_x'], row['similarity']))


    rows = []
    for customer_id, products_purchased in customer_2_products_purchased.items():
        similar_products = []
        for product_purchased in products_purchased:
            similar_products += direct_similar_products_dict[product_purchased]
        similar_products_dict = defaultdict(lambda: 0)
        for p in similar_products:
            similar_products_dict[p[0]] += p[1]
        direct_similar_products = list(map(lambda e: int(e[0]), sorted(similar_products_dict.items(), key=lambda e: e[1], reverse=True)))
        direct_similar_products = list(filter(lambda x: x not in products_purchased, direct_similar_products))[:200]
        print(direct_similar_products)
        rows.append((platform, customer_id, 'user', 'bought', algo, json.dumps(direct_similar_products)))
    print('Total number of customers: %d' % len(rows))
    RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection(env), 'recommendations_v2', rows)

    #parent_2_children = defaultdict(lambda: [])
    #for child, parent in results['child_2_parent'].items():
        #parent_2_children[parent].append(child)

    #print('Total Number of parent products: %d' % len(parent_2_children))
    #print('Total Number of child products: %d' % sum([len(variants) for parent, variants in parent_2_children.items()]))
    #rows = []
    #product_ids_updated = []
    #for product_id in direct_similar_products_dict:
        #product_ids_updated.append(product_id)
        #direct_similar_products = list(map(lambda e: int(e[0]), sorted(direct_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)))
        #rows.append((platform, product_id, 'product', 'bought', cab_algo, json.dumps(direct_similar_products)))
        #variants = parent_2_children.get(product_id, [])
        #for variant in variants:
            #product_ids_updated.append(variant)
            #rows.append((platform, variant, 'product', 'bought', cab_algo, str(direct_similar_products)))

    #print('Adding recommendations for %d products in DB' % len(product_ids_updated))
    #print('Total number of rows: %d' % len(rows))
    #RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection(env), 'recommendations_v2', rows)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--start-datetime')
    parser.add_argument('--end-datetime')
    parser.add_argument('--customer-id')
    parser.add_argument('--algo', default='combined_coccurence_direct')
    parser.add_argument('--limit', type=int)
    parser.add_argument('--env', required=True)
    parser.add_argument('--platform', required=True, choices=['nykaa','men'])

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    start_datetime = argv.get('start_datetime')
    end_datetime = argv.get('end_datetime')
    limit = argv.get('limit')
    env = argv.get('env')
    platform = argv.get('platform')
    algo = argv.get('algo')
    customer_id = argv.get('customer_id')

    compute_recommendations(env, algo, platform, start_datetime, end_datetime, customer_id, limit)
