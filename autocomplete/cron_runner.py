"""Script to trigger feed pipeline. Fixes anything missing for last 5 days """

import os
import sys
import argparse
from read_past_searches_data import read_file_by_date
from pipeline import run_pipeline

sys.path.append('/nykaa/scripts/sharedutils/')
from dateutils import enumerate_dates
from mongoutils import MongoUtils

sys.path.append("/nykaa/scripts/feed_pipeline")
#from health_check import get_missing_dates, enumerate_dates

parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, default=5)
parser.add_argument('--bucket', '-b', type=str, default='nykaa-nonprod-feedback-autocomplete')
argv = vars(parser.parse_args())
days = -1 * argv['days']
bucket = argv['bucket']

all_store = ['nykaa', 'men']

def get_missing_dates(collname, filt=None):
  client = MongoUtils.getClient()
  coll = client['search'][collname]

  pipe = []
  if filt:
    assert isinstance(filt, dict)
    pipe.append({"$match": filt})
  pipe += [{"$group": {"_id": "$date", "count": {"$sum": 1}}}, {"$sort": {"_id":1}}]
  res = list(coll.aggregate(pipe))

  dates_with_data = {x['_id'] for x in res}

  all_dates = enumerate_dates(-30*6, 0)
  missing_dates = all_dates - dates_with_data
  return missing_dates

def find_recent_missing_dates(store):
  if store == 'nykaa':
    missing_dates = get_missing_dates('search_terms_daily')
  else:
    missing_dates = get_missing_dates('search_terms_daily_men')
  print("data missing_dates in last 6 months of %s: %s" % (store, missing_dates))
  last_5_dates = enumerate_dates(days, 0)
  recent_missing_dates = missing_dates & last_5_dates
  print("data recent_missing_dates of %s: %s" % (store, recent_missing_dates))
  return recent_missing_dates

for store in all_store:
  recent_missing_dates = find_recent_missing_dates(store)
  #recent_missing_dates=['2020-08-15', '2020-08-16','2020-08-17','2020-08-18','2020-08-19','2020-08-20','2020-08-21','2020-08-22','2020-08-23','2020-08-24','2020-08-25','2020-08-26','2020-08-27','2020-08-28']
  for date in recent_missing_dates:
    print("\n\n\n")
    print("=== CRONRUNNER  =====")
    print("=== READING CSV FOR : %s ====" % date)
    read_file_by_date(date, 'web', store)
    read_file_by_date(date, 'app', store)
    print("THE END")
if recent_missing_dates:
  print("===> Running pipeline")
  run_pipeline()
else:
  print("Everything is up to date. Doing nothing.")

