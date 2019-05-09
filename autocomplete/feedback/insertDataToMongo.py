import json
import boto3
import argparse
import sys
import pymongo

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/nykaa/scripts/feed_pipeline")
from pipelineUtils import PipelineUtils

sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils


client = MongoUtils.getClient()
collection_name = 'feedback_data_autocomplete'

def insertFeedBackDataInMongo(filename='feedback_autocomplete_result.json'):
    bucket = PipelineUtils.getBucketNameForFeedback()
    pipeline = boto3.session.Session(profile_name='datapipeline')
    s3 = pipeline.resource('s3')
    try:
        s3.Bucket(bucket).download_file(filename, filename)
    except:
        print("Unable to download file from s3")
        exit()

    with open(filename) as f:
        file_data = json.load(f)

    #ensure collection as well as index is there
    existing_list = client['search'].collection_names()
    if collection_name not in existing_list:
        print("creating collection: ", collection_name)
        feedback_data_autocomplete = client['search'].create_collection(collection_name)
    feedback_data_autocomplete = client['search'][collection_name]
    indices = feedback_data_autocomplete.list_indexes()
    if 'search_term_1' not in [x['name'] for x in indices]:
        print("Creating index on search_term")
        feedback_data_autocomplete.create_index([("search_term", pymongo.ASCENDING)])

    #truncate collection
    feedback_data_autocomplete.remove({})

    #insert data in collection
    feedback_data_autocomplete.insert(file_data, {"ordered": False})

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for feedback result')
    parser.add_argument('--filename', '-f', type=str, default='feedback_autocomplete_result.json')

    argv = vars(parser.parse_args())
    filename = argv['filename']

    insertFeedBackDataInMongo(filename)
