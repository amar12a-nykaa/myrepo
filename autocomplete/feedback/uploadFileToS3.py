import boto3
import sys
import argparse
import re
import time

sys.path.append("/nykaa/scripts/sharedutils")
from dateutils import enumerate_dates
sys.path.append("/nykaa/scripts/feed_pipeline")
from pipelineUtils import PipelineUtils

parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, default=15)
argv = vars(parser.parse_args())
days = -1 * argv['days']

dates_to_process = enumerate_dates(days, -1)
pipeline = boto3.session.Session(profile_name='datapipeline')
bucket_name = PipelineUtils.getBucketNameForFeedback()

params = {
    'region': 'ap-southeast-1',
    'database': 'datapipeline',
    'bucket': bucket_name
}
client = pipeline.client('athena', region_name=params["region"])

def query_athena(params):
    response = client.start_query_execution(
        QueryString=params["query"],
        QueryExecutionContext={
            'Database': params['database']
        },
        ResultConfiguration={
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )
    return response

for date in dates_to_process:
    s3_file_location = 'dt=%s' % date.strftime("%Y%m%d")
    dateStr = date.strftime("%Y-%m-%d")
    query = """SELECT b.*,
                      a.click_freq
                FROM
                    (SELECT DISTINCT lower(typed_term) AS typed_term,
                         lower(clicked_term) AS clicked_term,
                         count(*) AS click_freq
                    FROM _events
                    WHERE visible_sugg!=''
                            AND CAST(dt AS DATE) = DATE('%s')
                    GROUP BY  1,2 ) a
                RIGHT JOIN
                    (SELECT DISTINCT lower(typed_term) AS typed_term,
                         lower(visible_term) AS visible_term,
                         count(*) AS visible_freq
                    FROM _events
                    CROSS JOIN UNNEST(SPLIT(visible_sugg,'|')) AS t (visible_term)
                    WHERE visible_sugg!=''
                            AND CAST(dt AS DATE) = DATE('%s')
                    GROUP BY  1,2 ) b
                    ON a.typed_term=b.typed_term
                        AND a.clicked_term=b.visible_term
                ORDER BY  4 desc, 3 desc
    """%(dateStr, dateStr)
    params['path'] = s3_file_location
    params['query'] = query

    execution = query_athena(params)
    execution_id = execution['QueryExecutionId']
    state = 'RUNNING'

    max_execution = 20
    terminated = False
    outputFile = ""
    while (max_execution > 0 and state in ['RUNNING']):
        max_execution = max_execution - 1
        response = client.get_query_execution(QueryExecutionId=execution_id)
    
        if 'QueryExecution' in response and \
            'Status' in response['QueryExecution'] and \
            'State' in response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                print("query execution failed")
                terminated = True
                break
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                outputFile = re.findall('.*\/(.*)', s3_path)[0]
                print("query execution successfull. File %s created"%outputFile)
                terminated = True
                break
        time.sleep(6)
    if outputFile:
        s3 = pipeline.client('s3')
        source = s3_file_location + '/' + outputFile
        destination = s3_file_location + '/autocompleteFeedbackV2.csv'
        sourcePath = bucket_name + "/" + source
        s3.copy_object(Bucket=bucket_name, CopySource=sourcePath, Key=destination)
        s3.delete_object(Bucket=bucket_name, Key=source)
        print('file uploaded successfully')