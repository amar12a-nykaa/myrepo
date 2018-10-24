import boto3
import os
import sys
import argparse
import datetime
import zipfile

sys.path.append("/nykaa/scripts/feed_pipeline")
from health_check import enumerate_dates

parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, default=15)
parser.add_argument("--bucket", '-b', type=str, default='nykaa-nonprod-feedback-autocomplete')
argv = vars(parser.parse_args())
days = -1 * argv['days']

dates_to_process = enumerate_dates(days, -1)
s3 = boto3.client('s3')
bucket_name = argv['bucket']

def unzip_file(path_to_zip_file):
    zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
    zip_ref.extractall(os.path.dirname(path_to_zip_file))
    zip_ref.close()

def uploadToS3(filepath, filename):
    s3.upload_file(filepath, bucket_name, filename)
    print('file uploaded successfully')

for date in dates_to_process:
    print("=== READING CSV FOR : %s ====" % date)
    filename = 'autocompleteFeedback%s.csv' % date.strftime("%Y%m%d")
    filepath = '/nykaa/adminftp/autocompleteFeedback%s.csv' % date.strftime("%Y%m%d")
    if not os.path.exists(filepath):
        filename_zip = '/nykaa/adminftp/autocompleteFeedback%s.zip' % date.strftime("%Y%m%d")
        if not os.path.isfile(filename_zip):
            print("%s not found" %filename_zip)
            continue
        unzip_file(filename_zip)
    os.system('sed -i "s/, 201/ 201/g" %s' % filepath)
    os.system('sed -i "s/\\"//g" %s' % filepath)
    os.system('sed -i "s/,Typed Search Term (evar77),/,typed_term,/g" %s' % filepath)
    os.system('sed -i "s/,Internal Search Term (Conversion) (evar6),/,search_term,/g" %s' % filepath)
    os.system('sed -i "s/,Internal Search Term (Conversion) Instance (Instance of evar6)/,click_count/g" %s' % filepath)

    uploadToS3(filepath, filename)






