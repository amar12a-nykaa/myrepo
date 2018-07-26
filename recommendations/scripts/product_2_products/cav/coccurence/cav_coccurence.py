import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import concat, col, lit, udf, isnan
import math
import boto3
import json
import pandas
from collections import defaultdict
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for CAV script')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--files', nargs='+')
    parser.add_argument('--output', '-o', required=True) 
    parser.add_argument('--luxe', nargs='+') 
    parser.add_argument('--mrp', nargs='+') 

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    files = argv['files']
    luxe_bucket = argv['luxe'][0]
    luxe_key = argv['luxe'][1]
    mrp_bucket = argv['mrp'][0]
    mrp_key = argv['mrp'][1]
    output_file = argv['output']

    s3 = boto3.client('s3')

    #spark = SparkSession.builder.master("local[10]").appName("CAV").config("spark.executor.memory", "6G").config("spark.storage.memoryFraction", 0.2).config("spark.driver.memory", "16G").getOrCreate()
    spark = SparkSession.builder.appName("CAV").getOrCreate()
    sc = spark.sparkContext

    print("Printing Configurations:")
    print(sc.getConf().getAll())

    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("session_id", StringType(), True)])

    #files = ['s3://nykaa-dev-recommendations/Report20171001-20180531.csv', 's3://nykaa-dev-recommendations/Report20170101-20170930.csv']
    #files = ['Report_visitor_20000.csv']

    df = spark.read.load(files[0], header=True, format='csv', schema=schema)

    for i in range(1, len(files)):
        df = df.union(spark.read.load(files[i], header=True, format='csv', schema=schema))

    df = df.cache()
    #df = df.withColumn('session_id', concat(col("Visitor_ID"), lit("_"), col("Visit Number")))
    #print("Printing schema:")
    #print(df.printSchema())

    if verbose:
        print("Rows count: " + str(df.count()))

    #print("Filtering out empty Visitor_ID and Visit Number")
    #df = df.filter((df["Visitor_ID"] != "") & df["Visitor_ID"].isNotNull() & df["Visit Number"].isNotNull() & ~isnan(df["Visit Number"]))
    #if verbose:
    #    print("Rows Count: " + str(df.count()))

    #print("Distinct Sessions counts: " + str(df.select('session_id').distinct().count()))

    #print("Filtering out empty Products and zero product views")
    #df = df.filter(df['Products'].isNotNull() & (df['Product Views'] > 0))
    #if verbose:
    #    print("Rows Count: " + str(df.count()))

    #print("Droping Columns")
    #df = df.drop('Visitor_ID', 'Date', 'Visit Number', 'Product Views')
    #print("Renaming Products to product_id")
    #df = df.withColumnRenamed('Products', 'product_id')

    print("Distinct Product counts: " + str(df.select('product_id').distinct().count()))

    print("Removing duplicate product_id for a session")
    df = df.groupBy(['session_id', 'product_id']).agg({'product_id': 'count'})
    df = df.drop('count(product_id)')

    print("Computing Product to sessions count")
    product_to_sessions_count_df = df.groupBy('product_id').agg({'session_id': 'count'}).withColumnRenamed('count(session_id)', 'sessions_count').toPandas()
    product_to_sessions_count = dict(zip(product_to_sessions_count_df.product_id, product_to_sessions_count_df.sessions_count))

    print("Self joining the dataframe: Computing all the non product to product pairs")
    merged_df = df.withColumnRenamed('product_id', 'product_id_x').join(df.withColumnRenamed('product_id', 'product_id_y'), on="session_id", how="inner")

    if verbose:
        print("Product Pairs count: " + str(merged_df.count()))

    print("Filtering out duplicate pairs")
    merged_df = merged_df[merged_df.product_id_x < merged_df.product_id_y]
    if verbose:
        print("Product Pairs count: " + str(merged_df.count()))

    print("Computing sessions for product pairs")
    merged_df = merged_df.groupBy(['product_id_x', 'product_id_y']).agg({'session_id': 'count'})
    if verbose:
        print("Product pairs count: " + str(merged_df.count()))

    final_df = merged_df.withColumnRenamed("count(session_id)", 'sessions_intersection')

    del df

    print("Filtering out all the product pairs with a threshold of atleast 2 sessions")
    final_df = final_df[final_df['sessions_intersection'] >= 2]
    if verbose:
        print("Product Pairs count: " + str(final_df.count()))

    def compute_union_len(product_id_x, product_id_y, sessions_intersection):
         return product_to_sessions_count[product_id_x] + product_to_sessions_count[product_id_y] - sessions_intersection

    compute_union_len_udf = udf(compute_union_len, IntegerType())
    final_df = final_df.withColumn("sessions_union", compute_union_len_udf(final_df['product_id_x'], final_df['product_id_y'], final_df['sessions_intersection']))

    def compute_similarity(customers_intersection, customers_union):
         return customers_intersection/customers_union

    compute_similarity_udf = udf(compute_similarity, FloatType())
    final_df = final_df.withColumn("similarity", compute_similarity_udf(final_df['sessions_intersection'], final_df['sessions_union']))

    def compute_log_similarity(customers_intersection, parent_id, similar_product_id):
         return customers_intersection/product_to_sessions_count[parent_id]/math.log(product_to_sessions_count[similar_product_id])

    #def compute_log_similarity(customers_intersection, customers_union):
    #     return customers_intersection/math.log(customers_union)

    compute_log_similarity_udf = udf(compute_log_similarity, FloatType())
    #final_df = final_df.withColumn("log_similarity", compute_log_similarity_udf(final_df['sessions_intersection'], final_df['sessions_union']))
    final_df = final_df.withColumn("log_similarity_y_x", compute_log_similarity_udf(final_df['sessions_intersection'], final_df['product_id_x'], final_df['product_id_y']))
    final_df = final_df.withColumn("log_similarity_x_y", compute_log_similarity_udf(final_df['sessions_intersection'], final_df['product_id_y'], final_df['product_id_x']))


    # In[27]:


    def compute_sqrt_similarity(customers_intersection, parent_id, similar_product_id):
         return customers_intersection/product_to_sessions_count[parent_id]/math.sqrt(product_to_sessions_count[similar_product_id])

    #def compute_sqrt_similarity(customers_intersection, customers_union):
    #     return customers_intersection/math.sqrt(customers_union)

    compute_sqrt_similarity_udf = udf(compute_sqrt_similarity, FloatType())
    #final_df = final_df.withColumn("sqrt_similarity", compute_sqrt_similarity_udf(final_df['sessions_intersection'], final_df['sessions_union']))
    final_df = final_df.withColumn("sqrt_similarity_y_x", compute_sqrt_similarity_udf(final_df['sessions_intersection'], final_df['product_id_x'], final_df['product_id_y']))
    final_df = final_df.withColumn("sqrt_similarity_x_y", compute_sqrt_similarity_udf(final_df['sessions_intersection'], final_df['product_id_y'], final_df['product_id_x']))

    direct_similar_products_dict = defaultdict(lambda: [])
    log_similar_products_dict = defaultdict(lambda: [])
    sqrt_similar_products_dict = defaultdict(lambda: [])
    simple_similar_products_dict = defaultdict(lambda: [])

    direct_similar_products_mrp_cons_dict = defaultdict(lambda: [])
    log_similar_products_mrp_cons_dict = defaultdict(lambda: [])
    sqrt_similar_products_mrp_cons_dict = defaultdict(lambda: [])
    simple_similar_products_mrp_cons_dict = defaultdict(lambda: [])

    luxe_obj = s3.get_object(Bucket=luxe_bucket, Key=luxe_key)
    luxe_dict = {p: True for p in json.load(luxe_obj['Body'])}

    mrp_obj = s3.get_object(Bucket=mrp_bucket, Key=mrp_key)
    product_2_mrp = json.load(mrp_obj['Body'])

    for row in final_df.collect():
        if not (luxe_dict.get(row['product_id_x'], False) ^ luxe_dict.get(row['product_id_y'], False)):
            direct_similar_products_dict[row['product_id_x']].append((row['product_id_y'], row['similarity']))
            direct_similar_products_dict[row['product_id_y']].append((row['product_id_x'], row['similarity']))

            log_similar_products_dict[row['product_id_x']].append((row['product_id_y'], row['log_similarity_y_x']))
            log_similar_products_dict[row['product_id_y']].append((row['product_id_x'], row['log_similarity_x_y']))

            sqrt_similar_products_dict[row['product_id_x']].append((row['product_id_y'], row['sqrt_similarity_y_x']))
            sqrt_similar_products_dict[row['product_id_y']].append((row['product_id_x'], row['sqrt_similarity_x_y']))

            simple_similar_products_dict[row['product_id_x']].append((row['product_id_y'], row['sessions_intersection']))
            simple_similar_products_dict[row['product_id_y']].append((row['product_id_x'], row['sessions_intersection']))

            if product_2_mrp.get(str(row['product_id_x'])) and product_2_mrp.get(str(row['product_id_y'])):
                product_x_mrp = product_2_mrp[str(row['product_id_x'])]
                product_y_mrp = product_2_mrp[str(row['product_id_y'])]
                if abs((product_x_mrp - product_y_mrp)/product_x_mrp) <= 0.3 or abs((product_x_mrp - product_y_mrp)/product_y_mrp) <= 0.3:
                    direct_similar_products_mrp_cons_dict[row['product_id_x']].append((row['product_id_y'], row['similarity']))
                    direct_similar_products_mrp_cons_dict[row['product_id_y']].append((row['product_id_x'], row['similarity']))

                    log_similar_products_mrp_cons_dict[row['product_id_x']].append((row['product_id_y'], row['log_similarity_y_x']))
                    log_similar_products_mrp_cons_dict[row['product_id_y']].append((row['product_id_x'], row['log_similarity_x_y']))

                    sqrt_similar_products_mrp_cons_dict[row['product_id_x']].append((row['product_id_y'], row['sqrt_similarity_y_x']))
                    sqrt_similar_products_mrp_cons_dict[row['product_id_y']].append((row['product_id_x'], row['sqrt_similarity_x_y']))

                    simple_similar_products_mrp_cons_dict[row['product_id_x']].append((row['product_id_y'], row['sessions_intersection']))
                    simple_similar_products_mrp_cons_dict[row['product_id_y']].append((row['product_id_x'], row['sessions_intersection']))

    rows = []
    for product_id in direct_similar_products_dict:
        simple_similar_products = list(map(lambda e: e[0], sorted(simple_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        direct_similar_products = list(map(lambda e: e[0], sorted(direct_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        log_similar_products = list(map(lambda e: e[0], sorted(log_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        sqrt_similar_products = list(map(lambda e: e[0], sorted(sqrt_similar_products_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))

        rows.append((product_id, 'coccurence_simple', str(simple_similar_products)))
        rows.append((product_id, 'coccurence_direct', str(direct_similar_products)))
        rows.append((product_id, 'coccurence_log', str(log_similar_products)))
        rows.append((product_id, 'coccurence_sqrt', str(sqrt_similar_products)))

    for product_id in direct_similar_products_mrp_cons_dict:
        simple_similar_products = list(map(lambda e: e[0], sorted(simple_similar_products_mrp_cons_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        direct_similar_products = list(map(lambda e: e[0], sorted(direct_similar_products_mrp_cons_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        log_similar_products = list(map(lambda e: e[0], sorted(log_similar_products_mrp_cons_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))
        sqrt_similar_products = list(map(lambda e: e[0], sorted(sqrt_similar_products_mrp_cons_dict[product_id], key=lambda e: e[1], reverse=True)[:50]))

        rows.append((product_id, 'coccurence_simple_mrp_cons', str(simple_similar_products)))
        rows.append((product_id, 'coccurence_direct_mrp_cons', str(direct_similar_products)))
        rows.append((product_id, 'coccurence_log_mrp_cons', str(log_similar_products)))
        rows.append((product_id, 'coccurence_sqrt_mrp_cons', str(sqrt_similar_products)))

    s3.put_object(Bucket='nykaa-dev-recommendations', Key=output_file, Body=json.dumps(rows, indent=4))


