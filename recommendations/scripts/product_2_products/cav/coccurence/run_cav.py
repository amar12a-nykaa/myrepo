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
            'Name': 'CAV',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit', '/home/hadoop/%s' % Constants.GEN_CAV_SCRIPT
                ] + command_line_arguments
            }
        }
    ]
    instance_groups = {
        'InstanceFleets': [
            {
              "Name": "Task",
              "InstanceFleetType": "TASK",
              "TargetSpotCapacity": 120,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "c5.4xlarge",
                  "WeightedCapacity": 32,
                  "BidPriceAsPercentageOfOnDemandPrice": 50,
                  "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                      {
                        "VolumeSpecification": {
                          "VolumeType": "gp2",
                          "SizeInGB": 32
                        },
                        "VolumesPerInstance": 8
                      }
                    ]
                  }
                },
                {
                  "InstanceType": "i2.4xlarge",
                  "WeightedCapacity": 122,
                  "BidPriceAsPercentageOfOnDemandPrice": 50,
                  "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                      {
                        "VolumeSpecification": {
                          "VolumeType": "gp2",
                          "SizeInGB": 32
                        },
                        "VolumesPerInstance": 8
                      }
                    ]
                  }
                },
                {
                  "InstanceType": "m4.4xlarge",
                  "WeightedCapacity": 64,
                  "BidPriceAsPercentageOfOnDemandPrice": 50,
                  "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                      {
                        "VolumeSpecification": {
                          "VolumeType": "gp2",
                          "SizeInGB": 32
                        },
                        "VolumesPerInstance": 8
                      }
                    ]
                  }
                }
              ]
            },
            {
              "Name": "Master",
              "InstanceFleetType": "MASTER",
              "TargetOnDemandCapacity": 1,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "m5.xlarge",
                  "WeightedCapacity": 1,
                  "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                      {
                        "VolumeSpecification": {
                          "VolumeType": "gp2",
                          "SizeInGB": 32
                        },
                        "VolumesPerInstance": 8
                      }
                    ]
                  }
                },
              ]
            },
            {
              "Name": "Core",
              "InstanceFleetType": "CORE",
              "TargetOnDemandCapacity": 32,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "m5.2xlarge",
                  "WeightedCapacity": 32,
                  "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                      {
                        "VolumeSpecification": {
                          "VolumeType": "gp2",
                          "SizeInGB": 32
                        },
                        "VolumesPerInstance": 8
                      }
                    ]
                  }
                },
              ]
            }
        ]
    }
    EMRUtils.launch_spark_emr('CAV', config, [], steps, dict(Constants.BIG_INSTANCE_SAMPLE, **instance_groups))

