from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import length, sum, lower, col, udf, max, log, lit, pow
from datetime import datetime, timedelta
import boto3
import json
import argparse
import re

TYPED_QUERY_LENGTH_THRESHOLD = 3
IMPRESSION_COUNT_THRESHOLD = 10

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for feedback script')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--days', type=int, default=7)
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
        StructField("typed_term", StringType(), True),
        StructField("visible_term", StringType(), True),
        StructField("visible_freq", IntegerType(), True),
        StructField("click_freq", IntegerType(), True)
    ])
    
    sss_schema = StructType([
        StructField("typed_term", StringType(), True),
        StructField("visible_term", StringType(), True),
        StructField("steady_state_score", IntegerType(), True)
    ])

    dfs = []
    for i in range(1, days):
        date = datetime.now() - timedelta(days=i)
        filename = 's3://nykaa-prod-feedback-autocomplete/dt=%s/autocompleteFeedbackV2.csv' % date.strftime("%Y%m%d")
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
        final_df = final_df.withColumn('visible_term', lower(col('visible_term')))
        final_df = final_df.withColumn('typed_term', lower(col('typed_term')))
        if verbose:
            print("Rows count: " + str(final_df.count()))

        def normalize_data(data):
            return re.sub(pattern='[.$]+', repl="_", string=data)

        normalize_typed_term_udf = udf(normalize_data, StringType())
        final_df = final_df.withColumn("typed_term", normalize_typed_term_udf(final_df['typed_term']))

        print("Taking distinct pair of typed_term and visible_term")
        final_df = final_df.groupBy(['typed_term', 'visible_term']).agg(sum('click_freq').alias('click_count'),
                                                                       sum('visible_freq').alias('impression_count'))
        final_df = final_df.filter(final_df.impression_count > IMPRESSION_COUNT_THRESHOLD)
        if verbose:
            print("Rows count: " + str(final_df.count()))
        
        yesterday_date = datetime.now() - timedelta(days=1)
        filename = 's3://nykaa-prod-feedback-autocomplete/dt=%s/autocompleteSSScore.csv' % yesterday_date.strftime("%Y%m%d")
        try:
            sss_data = spark.read.load(filename, header=True, format='csv', schema=sss_schema)
            if sss_data.count() > 0:
                final_df = final_df.join(sss_data, ['typed_term', 'visible_term'], how='left')
            else:
                final_df = final_df.withColumn('steady_state_score', lit(0))
        except:
            final_df = final_df.withColumn('steady_state_score', lit(0))
            pass
        final_df = final_df.na.fill(0)
        
        final_df = final_df.withColumn('ctr', 1 - (final_df['click_count'] / final_df['impression_count']))
        final_df = final_df.withColumn('click_count', log(1 + final_df['click_count']))
        final_df = final_df.withColumn('click_count', pow(final_df['click_count'], 2))
        final_df = final_df.join(final_df.groupby('typed_term').agg(max('click_count').alias('max_click_count')), 'typed_term')
        final_df = final_df.withColumn('click_count', final_df['click_count'] / final_df['max_click_count'])
        
        final_dict = {}
        final_df = final_df.sort("visible_term")
        current_term = ""
        for row in final_df.collect():
            if row['visible_term'] != current_term:
                current_term = row['visible_term']
                final_dict[current_term] = {}
            final_dict[current_term][row['typed_term']] = {'click': row['click_count'], 'ctr': row['ctr'], 'steady_score': row['steady_state_score']}

        final_list = []
        for key, value in final_dict.items():
            try:
                final_list.append({"search_term": key, "typed_terms" : value})
            except Exception as e:
                print("exception occured for %s", key)

        s3.put_object(Bucket='nykaa-prod-feedback-autocomplete', Key=output_file, Body=json.dumps(final_list))
        print("done")