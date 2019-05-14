import os
import socket

if os.environ.get('NYKAA_EMR_ENVIRONMENT'):
    env = os.environ['NYKAA_EMR_ENVIRONMENT']
else:
    env = socket.gethostname()

class RecoUtils:

    def get_env_details():
        if env in ['admin', 'prod-emr']:
            return {'bucket_name': 'nykaa-recommendations', 'key_name': 'nka-prod-emr', 'subnet_id': 'subnet-7c467d18', 'env': 'prod'}
        else:
            return {'bucket_name': 'nykaa-dev-recommendations', 'key_name': 'nka-qa-emr', 'subnet_id': 'subnet-6608c22f', 'env': 'non_prod'}

    def get_download_code_steps():
        env_details = RecoUtils.get_env_details()
        return [
            {
                'Name': 'Changing the hostname',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['sh', '-c', 'export NYKAA_EMR_ENVIRONMENT="dev-emr"' ]
                }
            },{
                'Name': 'Downloading code',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'aws', 's3', 'cp', 's3://%s/nykaa_scripts' % env_details['bucket_name'], '/home/hadoop/nykaa_scripts', '--recursive'
                    ]
                }
        }]

