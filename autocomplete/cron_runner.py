"""Script to trigger feed pipeline. Fixes anything missing for last 5 days """

import os
import sys
import argparse
from read_past_searches_data import read_file_by_date

sys.path.append('/nykaa/scripts/sharedutils/')
from dateutils import enumerate_dates
from mongoutils import MongoUtils

#sys.path.append("/nykaa/scripts/feed_pipeline")
#from health_check import get_missing_dates, enumerate_dates

parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, default=5)
parser.add_argument('--bucket', '-b', type=str, default='nykaa-nonprod-feedback-autocomplete')
argv = vars(parser.parse_args())
days = -1 * argv['days']
bucket = argv['bucket']

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


missing_dates = get_missing_dates('search_terms_daily')
print("data missing_dates in last 6 months: %s" % missing_dates)
last_5_dates = enumerate_dates(days, 0)

recent_missing_dates = missing_dates & last_5_dates
print("data recent_missing_dates: %s" % recent_missing_dates)

for date in recent_missing_dates:
  print("\n\n\n")
  print("=== CRONRUNNER  =====")
  print("=== READING CSV FOR : %s ====" % date)
  read_file_by_date(date, 'web')
  read_file_by_date(date, 'app')


if recent_missing_dates:
  cmd="/usr/bin/python /nykaa/scripts/autocomplete/pipeline.py"
  print(cmd)
  os.system(cmd)
else:
  print("Everything is up to date. Doing nothing.")

