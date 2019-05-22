# Usage
#  python gen_top_categories.py --gen-top-cats --prepare-model --gen-user-categories --platform nykaa --start-datetime "2018-11-01 00:00:00" -n 20
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
    config = '%s/%s' % (Constants.HOME_DIR, Constants.SMALL_EMR_CONFIG)
    env_details = RecoUtils.get_env_details()
    steps = [
        {
            'Name': 'Generating Top categories',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit', '/home/hadoop/%s' % Constants.GEN_TOP_CATEGORIES_SCRIPT, '--env', env_details['env'], '--bucket-name', env_details['bucket_name'] 
                ] + command_line_arguments
            }
        }
    ]
    EMRUtils.launch_spark_emr('Gen top Categories', config, [], steps, dict(Constants.SMALL_INSTANCE, **{'MasterInstanceType': 'm5.12xlarge'}))
