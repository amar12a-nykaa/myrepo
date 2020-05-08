import argparse
import boto3
import csv
import sys
import time
import json
import smtplib, ssl
import os

sys.path.append("/var/www/pds_api/")
from nykaa.settings import DISCOVERY_SQS_ENDPOINT
from nykaa.settings import DISCOVERY_SQS_REGION

GLUDO_FILE_NAME = 'gludo_catalog_upload.csv'

valid_fields = {
	"sku": "str",
	"catalog_tag": "array_of_str",
	"quantity": "integer",
	"disabled": "bool"
}


def download_gludo_file(env):
	print('download_gludo_file')
	if env == 'prod':
		BUCKET_NAME = 'prod-gludo'
	elif env == 'preprod':
		BUCKET_NAME = 'nonprod-gludo'
	else:
		raise Exception('invalid environment')

	key = GLUDO_FILE_NAME
	s3 = boto3.resource('s3')
	s3.Bucket(BUCKET_NAME).download_file(key, key)


def validate_data():
	print("Data validation start")
	print("Valid fields : {}".format(valid_fields))

	docs = []
	print(GLUDO_FILE_NAME)
	with open(GLUDO_FILE_NAME, newline='') as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			if not row.get('sku'):
				return False, "No sku value found in row : {} ".format(row)

			doc = {}
			for key, value in row.items():
				if key not in valid_fields:
					return False, "CSV contains invalid key : {} in row : {}".format(key, row)

				field_type = valid_fields.get(key)

				if key == 'sku':
					doc['sku'] = value.upper()
				elif field_type == 'str':
					doc[key] = value
				elif field_type == 'array_of_str':
					values = [x.strip() for x in value.split(',')]
					for value in values:
						if not value:
							return False, "Invalid array_of_string in row : {}".format(row)
					doc[key] = values
				elif field_type == 'integer':
					try:
						doc[key] =int(value)
					except:
						return False, "Invalid integer in row : {}".format(row)
				elif field_type == 'bool':
					if value in ['1', 'true']:
						doc[key] = True
					else:
						doc[key] = False

			doc['forced_push'] = True
			print('doc : {}'.format(doc))
			docs.append(doc)
	print("Data validation completed for {} products ".format(len(docs)))
	return True, docs


def delete_gludo_file():
	print('delete_gludo_file')
	os.remove(GLUDO_FILE_NAME)


def send_email(valid, msg):
	if not valid:
		subject = "Failure: Gludo forced push"
		body = msg
		print('gludo forced push failed, msg : {}'.format(msg))
	else:
		subject = "Success: Gludo forced push"
		body = msg
		print('gludo forced push successfully, msg : {}'.format(msg))

	email = 'review.report.nykaa@gmail.com'
	password = 'nykaa@123'
	port = 465

	fromx = email
	to = ['kedar.pagdhare@nykaa.com', 'pushkar.narayan@nykaa.com', 'pratyush.tripathi@nykaa.com',
		  'shashwat.gupta@nykaa.com', 'rupali.jain@nykaa.com']

	to = ['kedar.pagdhare@nykaa.com']

	msg = 'Subject:{}\n\n{}'.format(subject, body)
	server = smtplib.SMTP('smtp.gmail.com:587')
	server.starttls()
	server.ehlo()
	server.login(email, password)
	server.sendmail(fromx, to, msg)
	server.quit()


def send_to_discovery_service(docs, env):

	number_of_docs = len(docs)
	processed_docs = 0
	update_docs = []
	print(DISCOVERY_SQS_ENDPOINT)
	for update_doc in docs:
		update_docs.append(update_doc)
		processed_docs += 1
		sqs = boto3.client("sqs", region_name=DISCOVERY_SQS_REGION)

		queue_url = DISCOVERY_SQS_ENDPOINT
		if len(update_docs) == 100 or processed_docs == number_of_docs:
			response = sqs.send_message(
				QueueUrl=queue_url,
				DelaySeconds=0,
				MessageAttributes={},
				MessageBody=(json.dumps(update_docs, default=str)),
				MessageGroupId="1",
				MessageDeduplicationId=str(time.time()),
			)
			print(response)
			print("Total %s products sent to SQS" % processed_docs)
			update_docs.clear()

	return True, "{} Products Updated".format(processed_docs)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--env", type=str, help="environment")

	argv = vars(parser.parse_args())
	env = argv['env']
	download_gludo_file(env)
	valid, msg_docs = validate_data()
	if valid:
		success, msg = send_to_discovery_service(msg_docs, env)
		# send_email(success, msg)
	else:
		exit(1)
		# send_email(valid, msg_docs)

	delete_gludo_file()