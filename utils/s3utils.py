import os
import re
import boto3
from boto3.s3.transfer import TransferConfig

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

class S3Utils:

    def multi_part_upload_with_s3(bucket_name, key, file_path):
        # Multipart upload
        config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10, multipart_chunksize=1024 * 25, use_threads=True)
        s3_client.upload_file(file_path, bucket_name, key, ExtraArgs={'ACL': 'private'}, Config=config,)

    def upload_dir(local_directory, bucket_name, destination):
        for root, dirs, files in os.walk(local_directory):
            for filename in files:
                print('Uploading file: %s' % filename)
                local_path = os.path.join(root, filename)
                relative_path = os.path.relpath(local_path, local_directory)
                s3_path = os.path.join(destination, relative_path)
                s3_client.upload_file(local_path, bucket_name, s3_path)

    def download_dir(s3_dir, dist, local, bucket):
        paginator = s3_client.get_paginator('list_objects')
        for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
            if result.get('CommonPrefixes') is not None:
                for subdir in result.get('CommonPrefixes'):
                    S3Utils.download_dir(s3_dir, subdir.get('Prefix'), local, bucket)
            if result.get('Contents') is not None:
                for file in result.get('Contents'):
                    if not os.path.exists(os.path.dirname(local + os.sep + file.get('Key'))):
                        os.makedirs(os.path.dirname(local + os.sep + file.get('Key')))
                    local_path = local + os.sep + re.sub('^%s/' % s3_dir, "", file.get('Key'))
                    if not os.path.exists(local_path[0:local_path.rfind("/")]):
                        os.makedirs(local_path[0:local_path.rfind('/')])
                    s3_resource.meta.client.download_file(bucket, file.get('Key'), local_path)
