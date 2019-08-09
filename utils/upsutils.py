import json
import traceback
import psycopg2
import argparse
import time
import boto3
import os
import sys
import glob
import psycopg2
import sys
from collections import defaultdict
from gensim import corpora, models, similarities
from contextlib import closing
import mysql.connector
from elasticsearch import helpers, Elasticsearch
from IPython import embed
import tempfile
from datetime import datetime, timedelta, date

sys.path.append("/home/ubuntu/nykaa_scripts/utils")
sys.path.append("/home/hadoop/nykaa_scripts/utils")

from recoutils import RecoUtils

env_details = RecoUtils.get_env_details()

class UPSUtils:

    dynamodb = boto3.resource('dynamodb')

    def get_table_name():
        if env_details['env'] == 'prod':
            return 'user_profile_service'
        else:
            return 'user_profile_service_mumbai'

    def _add_recommendations_in_ups(rows):
        keys = [{'user_id': '%s' % customer_id} for customer_id in rows.keys()]
        table_name = UPSUtils.get_table_name()
        get_response = UPSUtils.dynamodb.batch_get_item(RequestItems={table_name: {'Keys': keys}})
        put_items = []
        for user_obj in get_response['Responses'][table_name]:
            if user_obj.get('recommendations'):
                user_obj['recommendations'].update(rows[user_obj['user_id']])
            else:
                user_obj['recommendations'] = rows[user_obj['user_id']]
            put_items.append({'PutRequest': {'Item': user_obj}})
        for i in range(0, len(put_items), 25):
            UPSUtils.dynamodb.batch_write_item(RequestItems={table_name: put_items[i:i+25]})

    def add_recommendations_in_ups(rows):
        rows_chunks = [rows[i:i+100] for i in range(0, len(rows), 100)]
        for chunk in rows_chunks:
            UPSUtils._add_recommendations_in_ups({str(row['customer_id']): row['value'] for row in chunk})

