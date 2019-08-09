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
    EMRUtils.launch_spark_emr('TEST', config, [], [], dict(Constants.SMALL_INSTANCE, **{'MasterInstanceType': 'm5.4xlarge', 'KeepJobFlowAliveWhenNoSteps': True}))
