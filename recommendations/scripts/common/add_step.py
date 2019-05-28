import boto3
import json
import traceback
import sys
sys.path.append('/home/ashwinpal/nykaa_scripts_2/utils')
import constants as Constants
from s3utils import S3Utils
from recoutils import RecoUtils
from emrutils import EMRUtils

if __name__ == '__main__':
    cluster_id = 'j-ULWY1G9YIYTP'
    steps = [
        {
            'Name': 'Setting variables',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'sudo', 'sh', '-c', 'echo prod-emr > /home/hadoop/cool.conf'
                ] 
            }
        }
    ]
    EMRUtils.add_emr_steps(cluster_id, steps)
