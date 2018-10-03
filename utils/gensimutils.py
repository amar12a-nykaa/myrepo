import tempfile
import boto3
import json
from gensim import corpora, models, similarities

import sys
sys.path.append("/home/ubuntu/nykaa_scripts/utils")
from s3utils import S3Utils

class GensimUtils:

    def get_models(bucket_name, input_dir):
        with tempfile.TemporaryDirectory() as temp_dir:
            S3Utils.download_dir(boto3.client('s3'), boto3.resource('s3'), input_dir, input_dir, temp_dir, bucket_name)
            with open(temp_dir + '/metric_corpus_dict.json') as f:
                metric_corpus_dict = json.load(f)
            tfidf = models.TfidfModel.load(temp_dir + '/tfidf/model.tfidf')
            tfidf_lsi = models.LsiModel.load(temp_dir + '/tfidf_lsi/model.tfidf_lsi')
            lsi = models.LsiModel.load(temp_dir + '/lsi/model.lsi')
        return (metric_corpus_dict, tfidf, tfidf_lsi, lsi)

    def generate_complete_vectors(vector_tuple, vector_len):
        vector_dict = dict(vector_tuple)
        return [vector_dict.get(i, 0) for i in range(vector_len)]
