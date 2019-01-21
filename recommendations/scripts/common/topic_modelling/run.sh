#!/bin/bash
# Usage
# sudo sh run.sh -t 100 -e non_prod -o "gensim_models/jan_14" -d "2018-01-01 00:00:00" -l 200000
ENV='non_prod'
NUM_TOPICS='300'
START_DATETIME=''
LIMIT=''
WITH_CHILDREN=''

while getopts t:e:o:d:l:c option
do
    case "${option}"
        in
        t) NUM_TOPICS=${OPTARG} ;;
        e) ENV=${OPTARG} ;;
        o) OUTPUT_DIR=${OPTARG} ;;
        d) START_DATETIME="--start-datetime=${OPTARG}" ;;
        l) LIMIT="--limit=${OPTARG}" ;;
        c) WITH_CHILDREN="--with-children" ;;
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

DIR='/home/ubuntu/nykaa_scripts/recommendations/scripts/common/topic_modelling/'
SCRIPT_FILE='generate_topic_models.py'
BOOTSTRAP_FILE='topic_modelling_download.sh'
CONFIG_FILE='config.json'
S3_PREFIX='topic_modelling'

aws s3 cp "${DIR}${BOOTSTRAP_FILE}" s3://${BUCKET_NAME}/${S3_PREFIX}/
aws s3 cp "${DIR}${SCRIPT_FILE}" s3://${BUCKET_NAME}/${S3_PREFIX}/

aws emr create-cluster --name "Topic Modelling customer_id:product_id: ${NUM_TOPICS}" --tags Category=Gludo Purpose=EMR --release-label emr-5.14.0 --instance-type m5.24xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},SubnetId=${SUBNET_ID} --ebs-root-volume-size 100 --bootstrap-actions Path="s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}" --log-uri "s3://${BUCKET_NAME}/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://${BUCKET_NAME}/${S3_PREFIX}/${SCRIPT_FILE},"--bucket-name","${BUCKET_NAME}","--output-dir","${OUTPUT_DIR}","--metric","customer_id","--similarity-metric","product_id","--discarded-metric","order_id","--num-topics","${NUM_TOPICS}","-m","lsi","--env","${ENV}",${LIMIT},"${START_DATETIME}","--verbose","${WITH_CHILDREN}"] --use-default-roles --auto-terminate --configurations file://${DIR}${CONFIG_FILE}

