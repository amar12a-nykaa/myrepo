from datetime import datetime, timedelta
import boto3
import argparse
import io
import sys
import pandas as pd

sys.path.append("/nykaa/scripts/feed_pipeline")
from pipelineUtils import PipelineUtils

TYPED_QUERY_LENGTH_THRESHOLD = 3
IMPRESSION_COUNT_THRESHOLD = 10


def calculate_feedback(days=7, verbose=False):
    dfs = []
    bucket = PipelineUtils.getBucketNameForFeedback()
    pipeline = boto3.session.Session(profile_name='datapipeline')
    s3 = pipeline.resource('s3')
    for i in range(1, days):
        date = datetime.now() - timedelta(days=i)
        filename = 'dt=%s/search_metrics.csv' % date.strftime("%Y%m%d")
        try:
            obj = s3.get_object(Bucket=bucket, Key=filename)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
            df = df[['search_term', 'product_id', 'revenue']]
            df = df[df.revenue >= 1]
            df = df.astype({'product_id': 'int32', 'revenue': 'float16'})
            if not df.empty:
                print("appending for %s" % filename)
                dfs.append(df)
        except Exception as ex:
            print(ex)

    if dfs:
        final_df = pd.concat(dfs)
        if verbose:
            print("Rows count: " + str(final_df.shape[0]))

        print("Taking distinct pair of search_term and product_id")
        final_df = final_df.groupby(['search_term', 'product_id']).agg({'revenue': 'sum'}).reset_index()
        if verbose:
            print("Rows count: " + str(final_df.shape[0]))

        #normalize revenue on search_term
        def normalize(a):
            return ((a - min(a)) / (max(a) - min(a)))*100

        final_df['normalized_revenue'] = final_df.groupby('search_term', as_index=False).revenue.apply(normalize)
        final_df.normalized_revenue = final_df.normalized_revenue.fillna(100)
        outputfile = 'feedback_final.csv'
        final_df.to_csv(outputfile, index=False)
        print("feedback calculation done")
        return outputfile


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for feedback script')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--days', type=int, default=7)

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    days = argv['days']
    filename = calculate_feedback(days=days, verbose=verbose)