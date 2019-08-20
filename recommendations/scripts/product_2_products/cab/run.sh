#!/bin/bash
# Usage
# sudo sh run.sh -d "2018-01-01 00:00:00" -e "non_prod" -p "nykaa" -a "coccurence_direct"
#subnet ids for prod env are subnet-81b994f7, subnet-7c467d18

START_DATETIME=`date --date='-6 month' '+%Y-%m-%d'`
START_DATETIME="$START_DATETIME 00:00:00"
ENV='non_prod'
CAB_ALGO=''

while getopts d:e:p:a: option
do
    case "${option}"
        in
        d) START_DATETIME=${OPTARG} ;;
        e) ENV=${OPTARG} ;;
        p) PLATFORM=${OPTARG} ;;
        a) CAB_ALGO="--cab-algo=${OPTARG}" ;;
    esac
done

if [ "$ENV" = 'prod' ];
then
    BUCKET_NAME='nykaa-recommendations-mumbai'
    KEY_NAME='nka-prod-emr-mumbai'
    SUBNET_ID='subnet-0d32caca4d4ddc0fd'
else
    BUCKET_NAME='nykaa-dev-recommendations-mumbai'
    KEY_NAME='nka-preprod-emr-mumbai'
    SUBNET_ID='subnet-0ca77a0c5544c4b9d'
fi

echo "env=$ENV"
echo "start_datetime=$START_DATETIME"
echo "key_name=$KEY_NAME"
echo "subnet_id=$SUBNET_ID"

DIR='/home/ubuntu/nykaa_scripts/recommendations/scripts/product_2_products/cab/'
RECO_FILE='generate_cab_fbt_recommendations.py'
BOOTSTRAP_FILE='cab_fbt_download.sh'
CONFIG_FILE='config.json'
S3_PREFIX='cab_fbt'

aws s3 cp "${DIR}${RECO_FILE}" s3://${BUCKET_NAME}/${S3_PREFIX}/${RECO_FILE}
aws s3 cp "${DIR}${BOOTSTRAP_FILE}" s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}

aws emr create-cluster --name "Computing CAB-FBT" --release-label emr-5.14.0 --instance-type m5.4xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},InstanceProfile=DataPipelineDefaultResourceRole,SubnetId=${SUBNET_ID} --tags Category=Gludo Purpose=EMR --ebs-root-volume-size 100 --bootstrap-actions Path="s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}" --log-uri "s3://${BUCKET_NAME}/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://${BUCKET_NAME}/${S3_PREFIX}/${RECO_FILE},"--start-datetime","${START_DATETIME}","--env","${ENV}","--platform","${PLATFORM}","${CAB_ALGO}"] --service-role DataPipelineDefaultRole --auto-terminate --configurations file://${DIR}${CONFIG_FILE}

#aws emr create-cluster --name "CAB" --release-label emr-5.14.0 --instance-type m5.4xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=nka-qa-emr,SubnetId=subnet-2b4c085c --ebs-root-volume-size 100 --bootstrap-actions Path="s3://nykaa-dev-recommendations/cab_download.sh" --log-uri "s3://nykaa-dev-recommendations/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://nykaa-dev-recommendations/generate_coccurence_direct_recommendations.py,"--limit","20000"] --use-default-roles --auto-terminate #--configurations file://config.json
