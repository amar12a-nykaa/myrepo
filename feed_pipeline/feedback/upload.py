import boto3
import sys
import argparse
import os
import zipfile
import pandas as pd

sys.path.append("/nykaa/scripts/sharedutils")
from dateutils import enumerate_dates
sys.path.append("/nykaa/scripts/feed_pipeline")
from pipelineUtils import PipelineUtils

pipeline = boto3.session.Session(profile_name='datapipeline')
bucket_name = PipelineUtils.getBucketNameForFeedback()
COUNT_THRESHOLD = 3

def process_chunk(df):
    df["valid"] = 0
    df.dropna(subset=['search_term'], inplace=True)

    def process_queryterm(row):
        if str(row["search_term"]).startswith("App:Search:"):
            terms = row["search_term"].split(":")
            if len(terms) >= 3 and str(row["product_id"]).isdigit():
                term = terms[2]
                term = term.lower()
                row["search_term"] = term
                row["valid"] = 1
        return row
    df = df.apply(process_queryterm, axis=1)
    df = df[df.valid == 1]

    #remove spelling mistakes
    search_count = df.groupby("search_term").product_id.count().reset_index()
    search_count.rename(columns={'product_id': 'instance_count'}, inplace=True)
    search_count = search_count[search_count.instance_count >= COUNT_THRESHOLD]
    df = pd.merge(df, search_count, on='search_term')

    df.drop(['valid', 'instance_count'], axis=1, inplace=True)
    df = df.fillna(0)
    df = df.astype({'search_term': str, 'product_id': 'int32', 'views': 'int32', 'cart_adds': 'int32', 'revenue': 'float32', 'order': 'int32'})

    return df


def process_data(filename):
    data_iterator = pd.read_csv(filename, chunksize=10000)
    chunk_list = []

    for data_chunk in data_iterator:
        data_chunk.rename(columns={"Site Sub navigation (evar42)": "search_term",
                                   "Products": "product_id",
                                   "Product Views": "views",
                                   "Cart Additions": "cart_adds",
                                   "Revenue": "revenue",
                                   "Orders": "order"}, inplace=True)
        filtered_chunk = process_chunk(data_chunk)
        chunk_list.append(filtered_chunk)
    filtered_data = pd.concat(chunk_list)
    filtered_data = filtered_data.groupby(['search_term','product_id']).agg(
                        {'views': 'sum', 'cart_adds': 'sum', 'order': 'sum', 'revenue': 'sum'}).reset_index()
    return filtered_data


def uploadFile(days=-1):
    dates_to_process = enumerate_dates(days, -1)

    for date in dates_to_process:
        dateStr = date.strftime("%Y%m%d")

        #process file
        filepath = '/nykaa/adminftp/Site_sub_pid_funn%s.csv' % dateStr
        if not os.path.exists(filepath):
            filepath = '/nykaa/adminftp/Site_sub_pid_funn%s.zip' % dateStr
        if not os.path.isfile(filepath):
            print("[ERROR] File does not exist: %s" % filepath)
            continue

        extention = os.path.splitext(filepath)[1]
        if extention == '.zip':
            csvfilepath = os.path.splitext(filepath)[0] + '.csv'
            try:
                os.remove(csvfilepath)
            except OSError:
                pass
            zip_ref = zipfile.ZipFile(filepath, 'r')
            zip_ref.extractall(os.path.dirname(filepath))
            zip_ref.close()
            assert os.path.isfile(csvfilepath), 'Failed to extract CSV from %s' % filepath
            filepath = csvfilepath

        data = process_data(filepath)
        filename = "search_metrics.csv"
        data.to_csv(filename, index=False)

        #upload file to s3
        s3_file_location = "dt=%s/%s" %(dateStr, filename)
        s3 = pipeline.client('s3')
        try:
            s3.upload_file(filename, bucket_name, s3_file_location)
            print("File uploaded successfully")
            os.remove(filepath)
            os.remove(filename)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=1)
    argv = vars(parser.parse_args())
    days = -1 * argv['days']
    uploadFile(days)