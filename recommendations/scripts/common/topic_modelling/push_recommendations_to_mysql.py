from gensim import corpora, models, similarities
from joblib import Parallel, delayed
import json
import argparse
import boto3
import smart_open
import os
import sys
import tempfile
import re

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils, RecommendationsUtils
sys.path.append("/home/ubuntu/nykaa_scripts/sharedutils")
from loopcounter import LoopCounter

def download_dir(client, resource, s3_dir, dist, local, bucket):
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
        if result.get('CommonPrefixes') is not None:
            for subdir in result.get('CommonPrefixes'):
                download_dir(client, resource, s3_dir, subdir.get('Prefix'), local, bucket)
        if result.get('Contents') is not None:
            for file in result.get('Contents'):
                if not os.path.exists(os.path.dirname(local + os.sep + file.get('Key'))):
                    os.makedirs(os.path.dirname(local + os.sep + file.get('Key')))
                local_path = local + os.sep + re.sub('^%s/' % s3_dir, "", file.get('Key'))
                if not os.path.exists(local_path[0:local_path.rfind("/")]):
                    os.makedirs(local_path[0:local_path.rfind('/')])
                resource.meta.client.download_file(bucket, file.get('Key'), local_path)

def compute_recommendations(metric_ids, metric_corpus_dict, rows, product2recommendations):
    for metric_id in metric_ids:
        sims = index[lsi[tfidf[metric_corpus_dict[metric_id]]]]
        sorted_products = sorted(enumerate(sims), key=lambda x: x[1], reverse=True)
        recommendations = [int(idx2metric[idx]) for idx, sim in sorted_products[1:51]]
        product2recommendations[int(metric_id)] = recommendations
        rows.append((metric_id, entity_type, recommendation_type, algo, json.dumps(recommendations)))
        if reco_count and len(rows) > reco_count:
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser')
    parser.add_argument('--verbose', action='store_true')
    parser.add_argument('--bucket-name', required=True)
    parser.add_argument('--input-dir', required=True)
    parser.add_argument('--entity_type', required=True)
    parser.add_argument('--recommendation_type', required=True)
    parser.add_argument('--algo', required=True)
    parser.add_argument('--reco-count', type=int)

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    input_dir = argv['input_dir']
    bucket_name = argv['bucket_name']
    input_path = "s3://%s/%s" % (bucket_name, input_dir)
    entity_type = argv['entity_type']
    recommendation_type = argv['recommendation_type']
    algo = argv['algo']

    reco_count = None
    if not argv.get('reco_count'):
        reco_count =argv['reco_count']

    with tempfile.TemporaryDirectory() as temp_dir:
        if verbose:
            print("Downloading necessary files from s3 to temp folder: %s" % temp_dir)
        download_dir(boto3.client('s3'), boto3.resource('s3'), input_dir, input_dir, temp_dir, bucket_name)
        with open(temp_dir + '/metric_corpus_dict.json') as f:
            if verbose:
                print("Making metric corpus dictionary")
            metric_corpus_dict = json.load(f)
        if verbose:
            print("Loading tfidf model")
        tfidf = models.TfidfModel.load(temp_dir + '/tfidf/model.tfidf')
        if verbose:
            print("Loading LSI model")
        lsi = models.LsiModel.load(temp_dir + '/lsi/model.lsi')

    metric_corpus = []
    for key, value in metric_corpus_dict.items():
        metric_corpus.append(value)

    metric_corpus_tfidf = tfidf[metric_corpus]
    metric_corpus_lsi = lsi[metric_corpus_tfidf]
    index = similarities.MatrixSimilarity(metric_corpus_lsi)
    idx2metric = {idx: metric_id for idx, metric_id in enumerate(metric_corpus_dict.keys())}

    if verbose:
        print("Now computing recommendations")
    rows = []

    metric_ids = list(metric_corpus_dict.keys())
    product2recommendations = {}
    Parallel(n_jobs=20, verbose=1, pre_dispatch='1.5*n_jobs', backend="threading")(delayed(compute_recommendations)(metric_ids[i:i+1000], metric_corpus_dict, rows, product2recommendations) for i in range(0, len(metric_ids), 1000))

    results = Utils.scrollESForResults()
    for child_id, parent_id in results['child_2_parent'].items():
        if product2recommendations.get(parent_id):
            rows.append((str(child_id), entity_type, recommendation_type, algo, json.dumps(product2recommendations[parent_id])))

    RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection('w'), 'recommendations_v2', rows)
