import boto3

class EMRUtils:

    def get_emr_client():
        return boto3.client('emr', region_name='ap-southeast-1')
