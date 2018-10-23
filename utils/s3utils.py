import os
import re

class S3Utils:

    def download_dir(client, resource, s3_dir, dist, local, bucket):
        paginator = client.get_paginator('list_objects')
        for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist):
            if result.get('CommonPrefixes') is not None:
                for subdir in result.get('CommonPrefixes'):
                    S3Utils.download_dir(client, resource, s3_dir, subdir.get('Prefix'), local, bucket)
            if result.get('Contents') is not None:
                for file in result.get('Contents'):
                    if not os.path.exists(os.path.dirname(local + os.sep + file.get('Key'))):
                        os.makedirs(os.path.dirname(local + os.sep + file.get('Key')))
                    local_path = local + os.sep + re.sub('^%s/' % s3_dir, "", file.get('Key'))
                    if not os.path.exists(local_path[0:local_path.rfind("/")]):
                        os.makedirs(local_path[0:local_path.rfind('/')])
                    resource.meta.client.download_file(bucket, file.get('Key'), local_path)
