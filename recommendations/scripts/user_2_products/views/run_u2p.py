import boto3
import json
import traceback
import sys
sys.path.append('/home/ubuntu/nykaa_scripts/utils')
import constants as Constants
from s3utils import S3Utils
from recoutils import RecoUtils
from emrutils import EMRUtils


if __name__ == '__main__':
    command_line_arguments = sys.argv[1:]
    config = '%s/%s' % (Constants.HOME_DIR, Constants.BIG_EMR_CONFIG)
    env_details = RecoUtils.get_env_details()
    steps = [
        {
            'Name': 'Generating U2P',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit', '/home/hadoop/%s' % Constants.GEN_U2P_VIEWS_SCRIPT
                ] + command_line_arguments
            }
        }
    ]
    instance_groups = {
        'InstanceGroups': [
            Constants.BIG_MASTER_INSTANCE,
            dict(Constants.BIG_CORE_INSTANCE, **{'InstanceCount': 3})
        ]
    }
    EMRUtils.launch_spark_emr('U2P', config, [], steps, dict(Constants.BIG_INSTANCE_SAMPLE, **instance_groups))

