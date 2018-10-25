import json
import boto3
import argparse
import sys

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

client = Utils.mongoClient()
collection_name = 'feedback_data_autocomplete'

def insertInMongo(bucket='nykaa-nonprod-feedback-autocomplete', filename='feedback_autocomplete_result.json'):
    s3 = boto3.resource('s3')
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
        feedback_data_autocomplete.create_index([("search_term")])

    #truncate collection
    feedback_data_autocomplete.remove({})

    #insert data in collection
    feedback_data_autocomplete.insert(file_data, check_keys=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for feedback result')
    parser.add_argument('--bucket', '-b', type=str, default='nykaa-nonprod-feedback-autocomplete')
    parser.add_argument('--filename', '-f', type=str, default='feedback_autocomplete_result.json')

    argv = vars(parser.parse_args())
    bucket = argv['bucket']
    filename = argv['filename']

    insertInMongo(bucket, filename)