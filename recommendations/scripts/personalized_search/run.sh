#!/bin/bash
ENV='non_prod'
NUM_TOPICS='300'
LIMIT='-1'
ONLY_PRODUCTS=''

while getopts t:e:i:l:p option
do
    case "${option}"
        in
        i) INPUT_DIR=${OPTARG} ;;
        t) NUM_TOPICS=${OPTARG} ;;
        e) ENV=${OPTARG} ;;
        l) LIMIT=${OPTARG} ;;
        p) ONLY_PRODUCTS='--add-only-products' ;;
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

DIR='/home/ubuntu/nykaa_scripts/recommendations/scripts/personalized_search/'
PERSONALIZED_SEARCH_FILE='generate_user_product_vectors.py'
BOOTSTRAP_FILE='personalized_search_download.sh'
CONFIG_FILE='config.json'

S3_PREFIX='personalized_search'

aws s3 cp "${DIR}${BOOTSTRAP_FILE}" s3://${BUCKET_NAME}/${S3_PREFIX}/
aws s3 cp "${DIR}${PERSONALIZED_SEARCH_FILE}" s3://${BUCKET_NAME}/${S3_PREFIX}/

aws emr create-cluster --name "Personalized search: ${NUM_TOPICS}" --tags Category=Gludo Purpose=EMR --release-label emr-5.14.0 --instance-type m5.4xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},SubnetId=${SUBNET_ID} --ebs-root-volume-size 100 --bootstrap-actions Path="s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}" --log-uri "s3://${BUCKET_NAME}/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://${BUCKET_NAME}/${S3_PREFIX}/${PERSONALIZED_SEARCH_FILE},"--bucket-name","${BUCKET_NAME}","--algo","lsi_${NUM_TOPICS}","--input-dir","${INPUT_DIR}","--vector-len","${NUM_TOPICS}","--store-in-db","${ONLY_PRODUCTS}","--verbose","--env","${ENV}","--limit","${LIMIT}"] --use-default-roles --auto-terminate --configurations file://${DIR}${CONFIG_FILE}

