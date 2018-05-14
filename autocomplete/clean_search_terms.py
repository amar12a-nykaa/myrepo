import argparse
import datetime
import json
import os
import os.path
import pprint
import re
import sys
import traceback
from collections import OrderedDict
from contextlib import closing
import html
#from loopcounter import LoopCounter

import arrow
import mysql.connector
import numpy
import omniture
import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from stemming.porter2 import stem

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

client = Utils.mongoClient()
search_terms = client['search']['search_terms_daily']
search_terms_formatted = client['search']['search_terms_daily_formatted']

offset = 0
limit = 1000000

def format_term(term):
    term = html.unescape(term).lower()
    term = re.sub('[^A-Za-z0-9 ]', "", term)
    term = re.sub("colour", "color", term)
    term = re.sub(" +", " ", term)
    return term.strip()

def normalize_array(query):
    index = set()
    for row in Utils.mysql_read(query): 
        row = row['term']
        for term in row.split(" "):
            term = format_term(term)
            index.add(term)
    return index

brand_index = normalize_array("select brand as term from nykaa.brands")
category_index = normalize_array("select name as term from nykaa.l3_categories")

def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

while True:
    queries = search_terms.find()[offset:(offset+limit)]
    if queries:
        break
    formatted_queries = []
    for query in queries:
        formatted_term = storable_term = format_term(query['term']).strip()
        splitted_terms = storable_term.split()
        for each_term in splitted_terms:
            if each_term in brand_index or each_term in category_index:
                formatted_term = formatted_term.replace(each_term, "")

        #print(formatted_term)
        # TODO Look below, want to uncomment?
#        if len(formatted_term) <= 3:
#            continue
        formatted_term = format_term(formatted_term)
        try:
            formatted_term = float(formatted_term)
        except:
            if formatted_term:
                query['formatted_term'] = storable_term
                query['stripped_term'] = formatted_term
                query.pop('_id', None)
                formatted_queries.append(query)

    #print(json.dumps(formatted_queries, indent=4, default = myconverter))
    if formatted_queries:
        try:
            search_terms_formatted.insert_many(formatted_queries)
        except BulkWriteError as exc:
            print(exc.details)
    offset += limit

