import boto3
import json
import traceback
import sys
sys.path.append('/home/ubuntu/nykaa_scripts_2/utils')
from s3utils import S3Utils
from recoutils import RecoUtils
import constants as Constants

emr_client = boto3.client('emr', region_name='ap-southeast-1')
home_dir = '/home/ubuntu/nykaa_scripts_2'
config_file = '%s/recommendations/scripts/product_2_products/cab/config.json' % home_dir

if __name__ == '__main__':
    command_line_arguments = sys.argv[1:]
    env_details = RecoUtils.get_env_details()
    print(env_details)
    S3Utils.upload_dir(home_dir, env_details['bucket_name'], 'nykaa_scripts')
    with open(config_file) as f:
        configurations = json.load(f)
    try:
        response = emr_client.run_job_flow(
            Name='Gen Top categories', \
            ReleaseLabel='emr-5.18.0', \
            LogUri='s3://%s/logs' % env_details['bucket_name'], \
            Applications=[{'Name': 'Spark'}], \
            Instances={
                'MasterInstanceType': 'm5.4xlarge', \
                'InstanceCount': 1, \
                'KeepJobFlowAliveWhenNoSteps': True, \
                'TerminationProtected': False, \
                'Ec2KeyName': env_details['key_name'], \
                'Ec2SubnetId': env_details['subnet_id'], \
                #'KeepJobFlowAliveWhenNoSteps': False \
                'KeepJobFlowAliveWhenNoSteps': True \
            }, \
            JobFlowRole='EMR_EC2_DefaultRole', \
            ServiceRole='EMR_DefaultRole', \
            #ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            VisibleToAllUsers=True,
            Steps=RecoUtils.get_download_code_steps() + [
                {
                    'Name': 'Generating Top categories',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit', '/home/hadoop/%s' % Constants.GEN_TOP_CATEGORIES_SCRIPT, '--env', env_details['env'], '--bucket-name', env_details['bucket_name'] 
                        ] + command_line_arguments
                    }
                }
            ],
            Configurations = configurations,
            BootstrapActions=[
                {
                    'Name': 'Bootstrap',
                    'ScriptBootstrapAction': {
                        'Path': 's3://%s/%s' % (env_details['bucket_name'], Constants.GEN_TOP_CATEGORIES_BOOTSTRAP_FILE)
                    }
                }
            ]
        )
        print(response)
    except:
        print(traceback.format_exc())
