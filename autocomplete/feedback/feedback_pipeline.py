import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import length, sum, lower, col
import math
import boto3
import arrow
import json
import pandas
from collections import defaultdict
import argparse

TYPED_QUERY_LENGTH_THRESHOLD = 3
CLICK_COUNT_THRESHOLD = 3

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for feedback script')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--days', type=int, default=15)
    parser.add_argument('--output', '-o', required=True)

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    days = argv['days']
    output_file = argv['output']

    s3 = boto3.client('s3')

    spark = SparkSession.builder.appName("Feedback").getOrCreate()
    sc = spark.sparkContext

    print("Printing Configurations:")
    print(sc.getConf().getAll())

    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("typed_term", StringType(), True),
        StructField("search_term", StringType(), True),
        StructField("click_count", IntegerType(), True)])

    dfs = []
    for i in range(1, days):
        i = -i
        date = arrow.now().replace(days=i, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(
            tzinfo=None)
        filename = 's3://nykaa-nonprod-feedback-autocomplete/autocompleteFeedback%s.csv' % date.strftime("%Y%m%d")
        try:
            df = spark.read.load(filename, header=True, format='csv', schema=schema)
            if df.count() > 0:
                print("appending for %s"%filename)
                dfs.append(df)
        except:
            continue

    if dfs:
        final_df = dfs[0]
        for df in dfs[1:]:
            final_df = final_df.union(df)

        final_df = final_df.cache()

        if verbose:
            print("Rows count: " + str(final_df.count()))

        print("Filtering out typed_query with length less than threshold")
        final_df = final_df.filter(length('typed_term') >= TYPED_QUERY_LENGTH_THRESHOLD)
        final_df = final_df.withColumn('search_term', lower(col('search_term')))
        final_df = final_df.withColumn('typed_term', lower(col('typed_term')))
        if verbose:
            print("Rows count: " + str(final_df.count()))

        print("Taking distinct pair of typed_term and search_term")
        final_df = final_df.groupBy(['typed_term', 'search_term']).agg(sum('click_count').alias('click_count'))
        final_df = final_df.filter(final_df.click_count > CLICK_COUNT_THRESHOLD)
        if verbose:
            print("Rows count: " + str(final_df.count()))

        final_dict = {}
        final_df = final_df.sort("search_term")
        current_term = ""
        for row in final_df.collect():
            if row['search_term'] != current_term:
                current_term = row['search_term']
                final_dict[current_term] = {}
            final_dict[current_term][row['typed_term']] = row['click_count']

        final_list = []
        for key, value in final_dict.items():
            final_list.append({"search_term" : key, "typed_terms" : value})

        s3.put_object(Bucket='nykaa-nonprod-feedback-autocomplete', Key=output_file, Body=json.dumps(list))
        print("done")