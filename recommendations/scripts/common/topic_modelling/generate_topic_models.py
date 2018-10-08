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
import tempfile

def s3_upload_dir(s3, local_directory, bucket_name, destination):
    for root, dirs, files in os.walk(local_directory):
        for filename in files:
            print('Uploading file: %s' % filename)
            local_path = os.path.join(root, filename)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(destination, relative_path)
            s3.upload_file(local_path, bucket_name, s3_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argument parser for generating topics')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--input-csv', required=True)
    parser.add_argument('--bucket-name', '-b', required=True) 
    parser.add_argument('--output-dir', '-o', required=True) 
    parser.add_argument('--metric', required=True) 
    parser.add_argument('--similarity-metric', '-s', required=True) 
    parser.add_argument('--discarded-metric', '-d', required=True) 
    parser.add_argument('--num-topics', '-t', type=int, required=True) 
    parser.add_argument('--models', '-m', nargs='+') 

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    input_csv = argv['input_csv']
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
    df = pd.read_csv(input_csv, parse_dates=True)
    if verbose:
        print("Processing %d rows" % len(df))

    df = df.filter([metric, similarity_metric, discarded_metric])
    df = df[~df[metric].isna() & ~df[similarity_metric].isna() & ~df[discarded_metric].isna()]

    if verbose:
        print("After filtering out null values from dataframe, total number of rows: %d" % len(df))

    df['triplet_count'] = 1
    df = df.groupby([metric, similarity_metric, discarded_metric]).agg({'triplet_count': 'sum'}).reset_index().drop(['triplet_count', discarded_metric], axis=1)
    df['bought_count'] = 1
    df = df.groupby([similarity_metric, metric]).agg({'bought_count': 'sum'}).reset_index()

    metric_corpus_dict= defaultdict(lambda: [])
    for row in df.to_dict(orient='records'):
        metric_corpus_dict[row[metric]].append((int(row[similarity_metric]), int(row['bought_count'])))

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
