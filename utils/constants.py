import os
from recoutils import RecoUtils

env_details = RecoUtils.get_env_details()

HOME_DIR = os.path.realpath(__file__)[:os.path.realpath(__file__).rfind("/") + 1] + '..'

SMALL_EMR_CONFIG = 'recommendations/scripts/common/small_config.json'
BIG_EMR_CONFIG = 'recommendations/scripts/common/big_config.json'

GEN_TOP_CATEGORIES_SCRIPT = 'nykaa_scripts/recommendations/scripts/categories/gen_top_categories.py'

GEN_U2P_SCRIPT = 'nykaa_scripts/recommendations/scripts/user_2_products/bought_recommendations/generate_combined_coccurence_direct.py'

GEN_CAV_SCRIPT = 'nykaa_scripts/recommendations/scripts/product_2_products/cav/coccurence/cav_coccurence.py'

BOOTSTRAP_FILE = 'nykaa_scripts/recommendations/scripts/common/bootstrap.sh'

SMALL_INSTANCE = {
    'MasterInstanceType': 'm5.4xlarge', \
    'InstanceCount': 1, \
    'TerminationProtected': False, \
    'Ec2KeyName': env_details['key_name'], \
    'Ec2SubnetId': env_details['subnet_id'], \
    'KeepJobFlowAliveWhenNoSteps': False \
}

BIG_INSTANCE_SAMPLE = {
    'TerminationProtected': False, \
    'Ec2KeyName': env_details['key_name'], \
    'Ec2SubnetId': env_details['subnet_id'], \
    'KeepJobFlowAliveWhenNoSteps': False \
}

BIG_MASTER_INSTANCE = {
    'Name': 'Master',
    'InstanceType': 'm5.12xlarge',
    'InstanceCount': 1,
    'InstanceRole': 'MASTER',
    'EbsConfiguration': {
        'EbsBlockDeviceConfigs': [
            {
                'VolumeSpecification': {
                    'VolumeType': 'gp2',
                    'SizeInGB': 32
                },
                'VolumesPerInstance': 8
            }
        ]
    }
}

BIG_CORE_INSTANCE = {
    'Name': 'Core',
    'InstanceType': 'm5.12xlarge',
    'InstanceCount': 1,
    'InstanceRole': 'CORE',
    'EbsConfiguration': {
        'EbsBlockDeviceConfigs': [
            {
                'VolumeSpecification': {
                    'VolumeType': 'gp2',
                    'SizeInGB': 32
                },
                'VolumesPerInstance': 8
            }
        ]
    }
}

BIG_INSTANCE = {
    'InstanceGroups': [BIG_MASTER_INSTANCE, BIG_CORE_INSTANCE]
}


ORDER_SOURCE_NYKAA = ['Nykaa', 'Nykaa(Old)', 'NYKAA', 'CS-Manual']
ORDER_SOURCE_NYKAAMEN = ['NykaaMen']
