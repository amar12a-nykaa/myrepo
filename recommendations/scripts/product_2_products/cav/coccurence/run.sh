#!/bin/bash
ENV='non_prod'
DESKTOP=''
PLATFORM='nykaa'
DIR='/home/ubuntu/nykaa_scripts/recommendations/scripts/product_2_products/cav/coccurence/'

RECO_FILE='cav_coccurence.py'
BOOTSTRAP_FILE='cav_download.sh'
CONFIG_FILE='config.json'
S3_PREFIX='cav_coccurence'
S3_PREFIX_DATA="${S3_PREFIX}/data/"


while getopts f:p:e:d option
do
    case "${option}"
        in
        f) FILE_PATH=${OPTARG} ;;
        e) ENV=${OPTARG} ;;
        d) DESKTOP='--desktop' ;;
        p) PLATFORM=${OPTARG} ;;
    esac
done

if [ -z "$FILE_PATH" ];
then
    echo "No file path given. Exiting...."
    exit 1
fi

if [ "$ENV" = 'prod' ];
then
    BUCKET_NAME='nykaa-recommendations'
    KEY_NAME='nka-prod-emr'
    SUBNET_ID='subnet-7c467d18'
    FTP_USERNAME='omniture'
    FTP_PASSWORD='C9PEy2H8TEC2'
    FTP_SERVER='52.220.4.21'
else
    BUCKET_NAME='nykaa-dev-recommendations'
    KEY_NAME='nka-qa-emr'
    SUBNET_ID='subnet-6608c22f'
    FTP_USERNAME='ashwinpal'
    FTP_PASSWORD='8GJ3wCruW'
    FTP_SERVER='13.251.90.232'
fi

FILE_NAME=$(basename -- "$FILE_PATH")
echo "FILE_NAME=$FILE_NAME"
BASE_FILE_NAME=$(basename $FILE_NAME .zip)
echo "BASE_FILE_NAME=$BASE_FILE_NAME"

echo "Downloading the file from ftp server..."
wget --user="${FTP_USERNAME}" --password="${FTP_PASSWORD}" ftp://${FTP_SERVER}/${FILE_PATH} -O /tmp/${FILE_NAME}
echo "Unzipping..."
unzip -o "/tmp/${FILE_NAME}" -d "/tmp"
aws s3 cp "/tmp/${BASE_FILE_NAME}.csv" s3://${BUCKET_NAME}/${S3_PREFIX_DATA}
rm "/tmp/${FILE_NAME}"
rm "/tmp/${BASE_FILE_NAME}.csv"

echo "env=$ENV"
echo "filename=$BASE_FILE_NAME.csv"
echo "bucket_name=$BUCKET_NAME"

aws s3 cp "${DIR}${RECO_FILE}" s3://${BUCKET_NAME}/${S3_PREFIX}/${RECO_FILE}
aws s3 cp "${DIR}${BOOTSTRAP_FILE}" s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}

#aws emr create-cluster --name "Computing CAV" --release-label emr-5.14.0 --instance-type m5.12xlarge --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},SubnetId=${SUBNET_ID} --ebs-root-volume-size 100 --bootstrap-actions Path="s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}" --log-uri "s3://${BUCKET_NAME}/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://${BUCKET_NAME}/${S3_PREFIX}/${RECO_FILE},"--files","${FILE_PATH}","--env","${ENV}"] --use-default-roles --configurations file://${DIR}${CONFIG_FILE}

aws emr create-cluster --name "Computing CAV" --release-label emr-5.14.0 --applications Name=Spark --ec2-attributes KeyName=${KEY_NAME},SubnetId=${SUBNET_ID} --tags Category=Gludo Purpose=EMR --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":8}]},"InstanceGroupType":"MASTER","InstanceType":"m5.12xlarge","Name":"MASTER"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":8}]},"InstanceGroupType":"CORE","InstanceType":"m5.12xlarge","Name":"CORE"}]' --bootstrap-actions Path="s3://${BUCKET_NAME}/${S3_PREFIX}/${BOOTSTRAP_FILE}" --log-uri "s3://${BUCKET_NAME}/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://${BUCKET_NAME}/${S3_PREFIX}/${RECO_FILE},"--files","s3://${BUCKET_NAME}/${S3_PREFIX_DATA}/${BASE_FILE_NAME}.csv","--env","${ENV}","${DESKTOP}","--platform","${PLATFORM}"] --use-default-roles --auto-terminate --configurations file://${DIR}${CONFIG_FILE}
