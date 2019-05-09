import argparse
import arrow
import csv
import datetime
import html
import json
import os
import os.path
import pprint
import pymongo
import re
import subprocess
import sys
import time 
import traceback
#from loopcounter import LoopCounter

from IPython import embed
from collections import OrderedDict
from contextlib import closing
from datetime import date, timedelta

import arrow
import mysql.connector
import numpy
import omniture
import pandas as pd
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from stemming.porter2 import stem

from ensure_mongo_indexes import ensure_mongo_indices_now

sys.path.append("/home/apis/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils


sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter
from cliutils import CliUtils
from mongoutils import MongoUtils


DAILY_COUNT_THRESHOLD = 2 

client = MongoUtils.getClient()
search_terms_daily = client['search']['search_terms_daily']
search_terms_formatted = client['search']['search_terms_daily_formatted']
ensure_mongo_indices_now()

def format_term(term):
    term = html.unescape(term).lower()
    term = re.sub('[^A-Za-z0-9 ]', "", term)
    term = re.sub("colour", "color", term)
    term = re.sub(" +", " ", term)
    return term.strip()

def normalize_array(query):
    index = set()
    for row in PasUtils.mysql_read(query): 
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


def create_missing_indices():
  indices = search_terms_daily.list_indexes()
  if 'date_1_platform_1_term_1' not in [x['name'] for x in indices]:
    search_terms_daily.create_index([("date", pymongo.ASCENDING), ("platform", pymongo.ASCENDING), ("term", pymongo.ASCENDING)])

create_missing_indices()

def read_file_by_dates(startdate, enddate, platform, dryrun=False, limit=0, product_id=None, debug=False):

  startdate = datetime.datetime.strptime(startdate,  "%Y-%m-%d") if isinstance(startdate, str) else startdate
  enddate = datetime.datetime.strptime(enddate,  "%Y-%m-%d") if isinstance(enddate, str) else enddate

  startdate = startdate.date() if isinstance(startdate, datetime.datetime) else startdate
  enddate = enddate.date() if isinstance(enddate, datetime.datetime) else enddate

  assert isinstance(startdate, datetime.date)
  assert isinstance(enddate, datetime.date)
  def daterange(start_date, end_date):
      for n in range(int ((end_date - start_date).days) + 1):
          yield start_date + timedelta(n)

  for single_date in daterange(startdate, enddate):
    print(single_date.strftime("%Y-%m-%d"))
    read_file_by_date(single_date, platform, dryrun=dryrun, limit=limit, product_id=product_id, debug=debug)

def read_file_by_date(date, platform, dryrun=False, limit=0, product_id=None, debug=False):
  date = datetime.datetime.strptime(date,  "%Y-%m-%d") if isinstance(date, str) else date
  assert isinstance(date, datetime.datetime) or  isinstance(date, datetime.date), "Bad date format"

  if platform == 'web':
    filename = '/nykaa/adminftp/search_terms_website_%s.csv' %  date.strftime("%Y%m%d")
    if not os.path.exists(filename): 
      filename = '/nykaa/adminftp/search_terms_website_%s.zip' %  date.strftime("%Y%m%d") 

  elif platform == 'app':
    filename = '/nykaa/adminftp/search_terms_app_%s.csv' %  date.strftime("%Y%m%d")
    if not os.path.exists(filename): 
      filename = '/nykaa/adminftp/search_terms_app_%s.zip' %  date.strftime("%Y%m%d")

  print(filename)
  return read_file(filename, platform, dryrun, limit=limit, product_id=product_id, debug=debug)

def read_file(filepath, platform, dryrun, limit=0, product_id=None, debug=False):
  product_id_arg = product_id
  assert platform in ['app', 'web']

  client = MongoUtils.getClient()
  search_terms_daily = client['search']['search_terms_daily']


  def unzip_file(path_to_zip_file):
    import zipfile
    import os
    zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
    zip_ref.extractall(os.path.dirname(path_to_zip_file))
    zip_ref.close()


  #for filepath in files:
  if not os.path.isfile(filepath):
    print("[ERROR] File does not exist: %s" % filepath)
    return
  
  extention = os.path.splitext(filepath)[1]
  if extention == '.zip':
    csvfilepath = os.path.splitext(filepath)[0] + '.csv'
    os.system("rm %s")
    try:
      os.remove(csvfilepath)
    except OSError:
      pass
    unzip_file(filepath)
    assert  os.path.isfile(csvfilepath), 'Failed to extract CSV from %s' % filepath 
    
    filepath = csvfilepath 

  print(filepath)
  os.system('sed -i "s/, 201/ 201/g" %s' % filepath)
  os.system('sed -i "s/\\"//g" %s' % filepath)

  nrows = int(subprocess.check_output('wc -l ' + filepath, shell=True).decode().split()[0])
  ctr = LoopCounter("Reading CSV: ", total=nrows)
  first_row = True
  headers = None
  with open(filepath, newline='') as csvfile:
    for r in csvfile:
      r=r.strip().split(",")
      row = [r[0] , ",".join(r[1:-2]) ]  + r[-2:]
      if first_row:
        first_row = False
        headers = row
        continue
      row = dict(list(zip(headers, row)))
      if limit and ctr.count > limit:
        break
      ctr += 1
      if ctr.should_print():
        if not product_id:
          print(ctr.summary)
      try:
        d = dict(row)
        date = None

        if '\ufeffDate' in d:
          d['date'] = d.pop('\ufeffDate')
        if 'Date' in d:
          d['date'] = d.pop('Date')
        elif 'datetime' in d:
          d['date'] = d.pop('datetime')

        for _format in ['MMM D YYYY', 'MMMM D YYYY', 'YYYY-MM-DD']:
          try:
            date = arrow.get(d['date'], _format).datetime
          except:
            pass
          else:
            break

        d['date'] = date

      except KeyError:
        print("KeyError", d)
        raise
  
      replace_keys = [
        ('event5', 'views'),
        ('Product Views', 'views'),
        ('Products', 'product_id'),
        ('name', 'product_id'),
        ('Cart Additions', 'cart_additions'),
        ('cartadditions', 'cart_additions'),
        ('Orders', 'orders'),
        ('Revenue', 'revenue'),
        ('Units', 'units'),
        ]

      for k,v in replace_keys:
        if k in d:
          d[v] = d.pop(k)

      #required_keys = set(['views', 'product_id', 'cart_additions', 'orders'])
      #print("available:keys: %s" % d.keys())
      required_keys = set(['date', 'Internal Search Term (Conversion) (evar6)', 'Internal Search Term (Conversion) Instance (Instance of evar6)'])
      missing_keys = required_keys - set(list(d.keys()))
      if missing_keys:
        print("Missing Keys: %s" % missing_keys)
        raise Exception("Missing Keys in CSV")

      d['internal_search_term_conversion'] = d.pop("Internal Search Term (Conversion) (evar6)") 
      d['internal_search_term_conversion_instance'] = d.pop("Internal Search Term (Conversion) Instance (Instance of evar6)") 

      is_data_good = True
      for k in ['internal_search_term_conversion_instance', 'cart_additions']:
        try:
          if not d[k]:
            d[k] = 0
          d[k] = int(d[k])
        except:
          print("Error in processing: %s" % d)
          is_data_good = False
      if not is_data_good:
        continue
          

      #print("d: %s" % d)
      terms  = d['internal_search_term_conversion'].split("|")
      #if d['internal_search_term_conversion'] == "blackheads removal mask|blackheads removal mask":
      #  embed()
      #  exit()
      try:
        if len(terms) == 2:
          d['term'] = d['internal_search_term_conversion'].split("|")[1]
        else:
          d['term'] = d['internal_search_term_conversion'].split("|")[0]
        assert d['term']
      except:
        print("Error in processing: %s" % d)
        continue
      filt = {"date": date, "term": d['term'], "platform": platform}
      update = {k:v for k,v in d.items() if k in ['cart_additions', 'internal_search_term_conversion', 'internal_search_term_conversion_instance', 'date', 'term']}

      if update['internal_search_term_conversion_instance'] < DAILY_COUNT_THRESHOLD :
        continue

      formatted_term = storable_term = format_term(update['term']).strip()
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
              update['formatted_term'] = storable_term
              update['stripped_term'] = formatted_term
              update.pop('_id', None)

      if debug:
        print("d: %s" % d)
        print("filt: %s" % filt)
        print("update: %s" % update)

      if not dryrun:
        try:
          ret = search_terms_daily.update_one(filt, {"$set": update}, upsert=True) 
        except:
          print("filt: %s" % filt)
          raise
        if debug:
          print("Mongo response: %s" % ret.raw_result)
if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("--platform", '-p', required=True, help="app or web")
  parser.add_argument("--filepath", '-f')
  parser.add_argument("--dryrun",  action='store_true')
  parser.add_argument("--debug",  action='store_true')
  parser.add_argument("--limit", type=int, default=0)
  parser.add_argument("--id", default=0)
  parser.add_argument("--startdate", help="startdate in YYYYMMDD format or number of days to add from today i.e -4", type=CliUtils.valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
  parser.add_argument("--enddate", help="enddate in YYYYMMDD format or number of days to add from today i.e -4", type=CliUtils.valid_date, default=arrow.now().replace().format('YYYY-MM-DD'))
  argv = vars(parser.parse_args())
  if argv['filepath']:
    read_file(filepath=argv['filepath'], platform=argv['platform'], dryrun=argv['dryrun'], limit=argv['limit'], product_id=argv['id'], debug=argv['debug'])
  else:
    print(argv['startdate'])
    read_file_by_dates(startdate=argv['startdate'], enddate=argv['enddate'], platform=argv['platform'], dryrun=argv['dryrun'], limit=argv['limit'], product_id=argv['id'], debug=argv['debug'])

  exit()
  offset = 0
  limit = 1000000

  while True:
      queries = search_terms_daily.find()[offset:(offset+limit)]
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

