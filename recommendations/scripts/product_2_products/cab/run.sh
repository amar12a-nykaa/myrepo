#!/bin/bash
#subnet ids for prod env are subnet-81b994f7, subnet-7c467d18

START_DATETIME=`date --date='-6 month' '+%Y-%m-%d'`
START_DATETIME="$START_DATETIME 00:00:00"
ENV='non_prod'

while getopts d:e: option
do
    case "${option}"
        in
        d) START_DATETIME=${OPTARG} ;;
        e) ENV=${OPTARG} ;;
    esac
done

if [ "$ENV" = 'prod' ];
then
    BUCKET_NAME='nykaa-recommendations'
    KEY_NAME='nka-prod-emr'
    SUBNET_ID='subnet-7c467d18'
else
    BUCKET_NAME='nykaa-dev-recommendations'
    KEY_NAME='nka-qa-emr'
    SUBNET_ID='subnet-6608c22f'
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

aws emr create-cluster --name "Computing CAB-FBT" --release-label emr-5.14.0 --instance-type m5.4xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},SubnetId=${SUBNET_ID} --tags Category=Gludo Purpose=EMR --ebs-root-volume-size 100 --bootstrap-actions Path="s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}" --log-uri "s3://${BUCKET_NAME}/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://${BUCKET_NAME}/${S3_PREFIX}/${RECO_FILE},"--start-datetime","${START_DATETIME}","--env","${ENV}"] --use-default-roles --auto-terminate --configurations file://${DIR}${CONFIG_FILE}

#aws emr create-cluster --name "CAB" --release-label emr-5.14.0 --instance-type m5.4xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=nka-qa-emr,SubnetId=subnet-2b4c085c --ebs-root-volume-size 100 --bootstrap-actions Path="s3://nykaa-dev-recommendations/cab_download.sh" --log-uri "s3://nykaa-dev-recommendations/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://nykaa-dev-recommendations/generate_coccurence_direct_recommendations.py,"--limit","20000"] --use-default-roles --auto-terminate #--configurations file://config.json
