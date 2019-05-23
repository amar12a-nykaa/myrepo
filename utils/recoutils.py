import os
import socket

if os.environ.get('NYKAA_EMR_ENVIRONMENT'):
    env = os.environ['NYKAA_EMR_ENVIRONMENT']
else:
    env = socket.gethostname()

if env.startswith('admin') or env.startswith('prod-emr'):
    env = 'prod'
else:
    env = 'non_prod'

if env.startswith('prod-emr') or env.startswith('dev-emr'):
    is_emr = True
else:
    is_emr = False

class RecoUtils:

    def get_env_details():
        if env == 'prod':
            return {'bucket_name': 'nykaa-recommendations', 'key_name': 'nka-prod-emr', 'subnet_id': 'subnet-7c467d18', 'env': 'prod', 'is_emr': is_emr}
        else:
            return {'bucket_name': 'nykaa-dev-recommendations', 'key_name': 'nka-qa-emr', 'subnet_id': 'subnet-6608c22f', 'env': 'non_prod', 'is_emr': is_emr}

