import json
import boto3
import argparse

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

client = Utils.mongoClient()
feedback_data_autocomplete = client['search']['feedback_data_autocomplete']

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for feedback result')
    parser.add_argument('--bucket', '-b', type=str, default='nykaa-nonprod-feedback-autocomplete')
    parser.add_argument('--filename', '-f', type=str, default='feedback_autocomplete_result.json')

    argv = vars(parser.parse_args())
    bucket = argv['bucket']
    filename = argv['filename']

    s3 = boto3.client('s3')
    try:
        s3.Bucket(bucket).download_file(filename, filename)
    except:
        print("Unable to download file from s3")
        exit()

    with open(filename) as f:
        file_data = json.load(f)

    feedback_data_autocomplete.insert(file_data)