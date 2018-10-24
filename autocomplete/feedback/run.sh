#!/bin/bash
aws emr create-cluster --name "feedback_autocomplete" --release-label emr-5.14.0 --instance-type m4.large --instance-count 1 --applications Name=Spark --ec2-attributes KeyName=nka-qa-emr,SubnetId=subnet-2b4c085c --ebs-root-volume-size 50 --bootstrap-actions Path="s3://nykaa-nonprod-feedback-autocomplete/download.sh" --log-uri "s3://nykaa-nonprod-feedback-autocomplete/logs" --steps Type=Spark,Name="Spark Program",ActionOnFailure=CONTINUE,Args=[s3://nykaa-nonprod-feedback-autocomplete/feedback_pipeline.py,"-o","feedback_autocomplete_result.json","--verbose"] --use-default-roles --auto-terminate --configurations file://config.json --region ap-southeast-1