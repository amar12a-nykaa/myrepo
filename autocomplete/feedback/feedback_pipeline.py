from datetime import datetime, timedelta
import boto3
import json
import argparse
import re
import sys
import pandas as pd
import numpy as np

sys.path.append("/nykaa/scripts/feed_pipeline")
from pipelineUtils import PipelineUtils

TYPED_QUERY_LENGTH_THRESHOLD = 3
IMPRESSION_COUNT_THRESHOLD = 10

def calculate_feedback(days=7, output_file='feedback_autocomplete_result.json', verbose=False):
    dfs = []
    bucket = PipelineUtils.getBucketNameForFeedback()
    pipeline = boto3.session.Session(profile_name='datapipeline')
    s3 = pipeline.resource('s3')
    for i in range(1, days):
        date = datetime.now() - timedelta(days=i)
        filename = 'dt=%s/autocompleteFeedbackV2.csv' % date.strftime("%Y%m%d")
        outputfile = 'autocompleteFeedbackV2.csv'
        try:
            s3.Bucket(bucket).download_file(filename, outputfile)
            df = pd.read_csv(outputfile)
            df.click_freq = df.click_freq.fillna(0)
            df.visible_freq = df.visible_freq.fillna(0)
            df = df.astype({'click_freq': int, 'visible_freq': int})
            if not df.empty:
                print("appending for %s" % filename)
                dfs.append(df)
        except Exception as ex:
            print(ex)
    
    if dfs:
        final_df = dfs[0]
        for df in dfs[1:]:
            final_df = final_df.append([df])
        if verbose:
            print("Rows count: " + str(final_df.shape[0]))
        
        final_df = final_df.astype({'visible_term': str, 'typed_term': str})
        print("Filtering out typed_query with length less than threshold")
        final_df = final_df[final_df.typed_term.str.len() >= TYPED_QUERY_LENGTH_THRESHOLD]
        final_df['visible_term'] = final_df['visible_term'].apply(lambda x: x.lower())
        if verbose:
            print("Rows count: " + str(final_df.shape[0]))
        
        def normalize_data(data):
            data = data.lower()
            return re.sub(pattern='[.$]+', repl="_", string=data)
        
        final_df['typed_term'] = final_df['typed_term'].apply(normalize_data)
        
        print("Taking distinct pair of typed_term and visible_term")
        final_df = final_df.groupby(['typed_term', 'visible_term']).agg(
            {'click_freq': 'sum', 'visible_freq': 'sum'}).reset_index()
        final_df.rename(columns={'click_freq': 'click_count', 'visible_freq': 'impression_count'}, inplace=True)
        
        final_df = final_df[final_df.impression_count > IMPRESSION_COUNT_THRESHOLD]
        if verbose:
            print("Rows count: " + str(final_df.shape[0]))
        
        yesterday_date = datetime.now() - timedelta(days=1)
        filename = 'dt=%s/autocompleteSSScore.csv' % yesterday_date.strftime("%Y%m%d")
        outputfile = 'autocompleteSSScore.csv'
        try:
            s3.Bucket(bucket).download_file(filename, outputfile)
            sss_data = pd.read_csv(outputfile)
            if not sss_data.empty:
                final_df = pd.merge(final_df, sss_data, on=['typed_term', 'visible_term'], how='left')
                final_df = final_df.na.fill(0)
                final_df = final_df.astype({'steady_state_score': int})
            else:
                final_df['steady_state_score'] = 0
        except:
            final_df['steady_state_score'] = 0
            pass
        final_df.to_csv('final2.csv', index=False)
        final_df = final_df.fillna(0)
        final_df['ctr'] = final_df.apply(lambda x: 1 - (x['click_count'] / x['impression_count']), axis=1)
        final_df['click_count'] = final_df['click_count'].apply(lambda x: (np.log(1 + x)) ** 2)
        max_click_df = final_df.groupby('typed_term').agg({'click_count': 'max'}).reset_index()
        max_click_df.rename(columns={'click_count': 'max_click_count'}, inplace=True)
        final_df = pd.merge(final_df, max_click_df, on='typed_term')
        final_df['click_count'] = final_df.apply(
            lambda x: x['click_count'] / x['max_click_count'] if x['max_click_count'] else 0, axis=1)
        final_df.visible_term = final_df.visible_term.astype(str)
        final_df = final_df.sort_values(by='visible_term')
        
        final_dict = {}
        current_term = ""
        for i, row in final_df.iterrows():
            row = dict(row)
            if row['visible_term'] != current_term:
                current_term = row['visible_term']
                final_dict[current_term] = {}
            final_dict[current_term][row['typed_term']] = {'click': row['click_count'], 'ctr': row['ctr'],
                                                           'steady_score': row['steady_state_score']}
        
        final_list = []
        for key, value in final_dict.items():
            try:
                final_list.append({"search_term": key, "typed_terms": value})
            except Exception as e:
                print("exception occured for %s", key)
        
        s3.Bucket(bucket).put_object(Key=output_file, Body=json.dumps(final_list))
        print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for feedback script')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--output', '-o', default='feedback_autocomplete_result.json')
    
    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    days = argv['days']
    output_file = argv['output']
    calculate_feedback(days=days, output_file=output_file, verbose=verbose)