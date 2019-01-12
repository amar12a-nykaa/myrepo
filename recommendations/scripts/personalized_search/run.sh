#!/bin/bash
ENV='non_prod'
NUM_TOPICS='300'
LIMIT='-1'

while getopts t:e:i:l: option
do
    case "${option}"
        in
        i) INPUT_DIR=${OPTARG} ;;
        t) NUM_TOPICS=${OPTARG} ;;
        e) ENV=${OPTARG} ;;
        l) LIMIT=${OPTARG} ;;
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

aws emr create-cluster --name "Personalized search: ${NUM_TOPICS}" --tags Category=Gludo Purpose=EMR --release-label emr-5.14.0 --instance-type m5.4xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},SubnetId=${SUBNET_ID} --ebs-root-volume-size 100 --bootstrap-actions Path="s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}" --log-uri "s3://${BUCKET_NAME}/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://${BUCKET_NAME}/${S3_PREFIX}/${PERSONALIZED_SEARCH_FILE},"--bucket-name","${BUCKET_NAME}","--algo","lsi_${NUM_TOPICS}","--input-dir","${INPUT_DIR}","--vector-len","${NUM_TOPICS}","--store-in-db","--add-product-children","--verbose","--env","${ENV}","--limit","${LIMIT}"] --use-default-roles --auto-terminate --configurations file://${DIR}${CONFIG_FILE}

#aws emr create-cluster --name "Personalized search: topics ${NUM_TOPICS}" --tags Category=Gludo Purpose=EMR --release-label emr-5.14.0 --instance-type m5.24xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},SubnetId=${SUBNET_ID} --ebs-root-volume-size 100 --bootstrap-actions Path="s3://${BUCKET_NAME}/topic_modelling_download.sh" --log-uri "s3://nykaa-dev-recommendations/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://nykaa-dev-recommendations/generate_topic_models_2.py,"--input-csv","s3://nykaa-dev-recommendations/raw_cab_2018_10_sept.csv","--bucket-name","nykaa-dev-recommendations","--output-dir","gensim_models/raw_cab_2018_10_sept","--metric","customer_id","--similarity-metric","product_id","--discarded-metric","order_id","--num-topics","500","-m","tfidf","lsi"] --use-default-roles --auto-terminate --configurations file://config.json
#aws emr create-cluster --name "Personalized search: topics ${NUM_TOPICS}" --tags Category=Gludo Purpose=EMR --release-label emr-5.14.0 --instance-type m5.24xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},SubnetId=${SUBNET_ID} --ebs-root-volume-size 100 --bootstrap-actions Path="s3://${BUCKET_NAME}/topic_modelling_download.sh" --log-uri "s3://nykaa-dev-recommendations/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://nykaa-dev-recommendations/generate_topic_models_2.py,"--input-csv","s3://nykaa-dev-recommendations/raw_cab_2018_10_sept.csv","--bucket-name","nykaa-dev-recommendations","--output-dir","gensim_models/raw_cab_2018_10_sept","--metric","customer_id","--similarity-metric","product_id","--discarded-metric","order_id","--num-topics","500","-m","tfidf","lsi"] --use-default-roles --auto-terminate --configurations file://config.json

