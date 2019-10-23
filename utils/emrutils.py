import json
import traceback
import boto3

from s3utils import S3Utils
from recoutils import RecoUtils
import constants as Constants

class EMRUtils:

    def get_emr_client():
        return boto3.client('emr', region_name='ap-south-1')

    def get_emr_setup_steps():
        env_details = RecoUtils.get_env_details()
        return [ 
            {
                'Name': 'Setting environment',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'sudo', 'sh', '-c', 'echo %s > /home/hadoop/env.conf' % ('prod-emr' if env_details['env'] == 'prod' else 'dev-emr')
                    ]
                }
            },
            {
                'Name': 'Downloading code',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'aws', 's3', 'cp', 's3://%s/nykaa_scripts' % env_details['bucket_name'], '/home/hadoop/nykaa_scripts', '--recursive'
                    ]
                }
        }]

    def launch_spark_emr(name, config, before_steps, after_steps, instances = Constants.SMALL_INSTANCE):
        env_details = RecoUtils.get_env_details()
        print(env_details)
        S3Utils.upload_dir(Constants.HOME_DIR, env_details['bucket_name'], 'nykaa_scripts', ['.git'])
        if env_details['env'] == 'prod':
            jobflow_role = 'DataPipelineDefaultResourceRole'
            service_role = 'DataPipelineDefaultRole'
        else:
            jobflow_role = 'EMR_EC2_DefaultRole'
            service_role = 'EMR_DefaultRole'

        with open(config) as f:
            configurations = json.load(f)
        try:
            response = EMRUtils.get_emr_client().run_job_flow(
                Name=name, \
                ReleaseLabel='emr-5.18.0', \
                LogUri='s3://%s/logs' % env_details['bucket_name'], \
                Applications=[{'Name': 'Spark'}], \
                Instances=instances, \
                JobFlowRole=jobflow_role, \
                ServiceRole=service_role, \
                #JobFlowRole='EMR_EC2_DefaultRole', \
                #ServiceRole='EMR_DefaultRole', \
                ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
                VisibleToAllUsers=True,
                Steps= before_steps + EMRUtils.get_emr_setup_steps() + after_steps,
                Configurations = configurations,
                Tags=[
                    {
                        'Key': 'Category',
                        'Value': 'Gludo'
                    },
                    {
                        'Key': 'Purpose',
                        'Value': 'EMR'
                    },
                    {
                        'Key': 'Environment',
                        'Value': 'prod'
                    },
                    {
                        'Key': 'Component',
                        'Value': 'cd'
                    },
                    {
                        'Key': 'Subcomponent',
                        'Value': 'emr'
                    },
                    {
                        'Key': 'Servicetype',
                        'Value': 'EC2'
                    }
                ],
                EbsRootVolumeSize=100,
                BootstrapActions=[
                    {
                        'Name': 'Bootstrap downloads',
                        'ScriptBootstrapAction': {
                            'Path': 's3://%s/%s' % (env_details['bucket_name'], Constants.BOOTSTRAP_FILE)
                        }
                    }
                ]
            )
            print(response)
        except:
            print(traceback.format_exc())

    def add_emr_steps(cluster_id, steps):
        response = EMRUtils.get_emr_client().add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps = steps
        )
