# Usage
#  python gen_top_categories.py --gen-top-cats --prepare-model --gen-user-categories --platform nykaa --start-datetime "2018-11-01 00:00:00" -n 20
import boto3
import json
import argparse
import traceback
import sys
sys.path.append('/home/ubuntu/nykaa_scripts/utils')
import constants as Constants
from s3utils import S3Utils
from recoutils import RecoUtils
from emrutils import EMRUtils


if __name__ == '__main__':
    command_line_arguments = sys.argv[1:]
    views = False
    if '--views' in command_line_arguments:
        views = True
    env_details = RecoUtils.get_env_details()
    steps = [
        {
            'Name': 'Generating Top categories',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit', '/home/hadoop/%s' % Constants.GEN_TOP_CATEGORIES_SCRIPT 
                ] + command_line_arguments
            }
        }
    ]
    instance_groups = {
        'InstanceFleets': [
            {
              "Name": "Task",
              "InstanceFleetType": "TASK",
              "TargetSpotCapacity": 700,
              "InstanceTypeConfigs": [
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
                },
                {
                  "InstanceType": "m5d.24xlarge",
                  "WeightedCapacity": 384,
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
                  "InstanceType": "m5.12xlarge",
                  "WeightedCapacity": 192,
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
                  "InstanceType": "m5d.2xlarge",
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
                }
              ]
            },
            {
              "Name": "Master",
              "InstanceFleetType": "MASTER",
              "TargetOnDemandCapacity": 1,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "m5.12xlarge",
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
              "TargetSpotCapacity": 200,
              "InstanceTypeConfigs": [
                {
                  "InstanceType": "m5.12xlarge",
                  "WeightedCapacity": 192,
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
              ]
            }
        ]
    }
    if views:
        config = '%s/%s' % (Constants.HOME_DIR, Constants.BIG_EMR_CONFIG)
        EMRUtils.launch_spark_emr('Gen Top Categories: Views', config, [], steps, dict(Constants.BIG_INSTANCE_SAMPLE, **instance_groups))
    else:
        config = '%s/%s' % (Constants.HOME_DIR, Constants.SMALL_EMR_CONFIG)
        EMRUtils.launch_spark_emr('Gen top Categories', config, [], steps, dict(Constants.SMALL_INSTANCE, **{'MasterInstanceType': 'm5.12xlarge'}))
