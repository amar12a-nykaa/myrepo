"""Script to trigger feed pipeline. Fixes anything missing for last 5 days """

import os
import sys
import argparse
import datetime

sys.path.append("/nykaa/scripts/feed_pipeline")
from health_check import get_missing_dates, enumerate_dates
from read_csv_popularity_data import read_file_by_date

parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, default=5)
parser.add_argument("--force-run", action='store_true')
argv = vars(parser.parse_args())
days = -1 * argv['days']

missing_dates = get_missing_dates('raw_data')
print("raw_data missing_dates in last 6 months: %s" % missing_dates)
last_5_dates = enumerate_dates(days, 0)

recent_missing_dates_raw = missing_dates & last_5_dates
print("raw_data recent_missing_dates_raw: %s" % recent_missing_dates_raw)

for date in recent_missing_dates_raw:
  print("\n\n\n")
  print("=== CRONRUNNER  =====")
  print("=== READING CSV FOR : %s ====" % date)
  read_file_by_date(date, 'web')
  read_file_by_date(date, 'app')


missing_dates = get_missing_dates('processed_data')
print("preprocessing missing_dates in last 6 months: %s" % missing_dates)
last_5_dates = enumerate_dates(days, 0)

recent_missing_dates_preprocess = (missing_dates & last_5_dates) | recent_missing_dates_raw
print("preprocessing missing_dates: %s" % recent_missing_dates_preprocess)

for date in recent_missing_dates_preprocess:
  print("\n\n\n")
  print("=== PREPROCESSING  FOR : %s ====" % date)
  d = date.strftime("%Y-%m-%d")
  cmd="/usr/bin/python /nykaa/scripts/feed_pipeline/popularity.py --startdate %s --enddate %s --preprocess -n0 -y " % (d, d)
  print(cmd)
  os.system(cmd)

#insert order and revenue data in mongo
missing_dates = get_missing_dates('order_data')
print("order_data missing_dates in last 6 months: %s" % missing_dates)
last_5_dates = enumerate_dates(days, 0)

recent_missing_dates_orderdata = (missing_dates & last_5_dates)
print("processing missing_dates: %s" % recent_missing_dates_orderdata)

for date in recent_missing_dates_orderdata:
  print("\n\n\n")
  print("=== PROCESSING ORDER FOR : %s ====" % date)
  end = date + datetime.timedelta(days=1)
  d = date.strftime("%Y-%m-%d")
  e = end.strftime("%Y-%m-%d")
  cmd = "/usr/bin/python /nykaa/scripts/feed_pipeline/read_order_data_dwh.py --startdate %s --enddate %s" % (d, e)
  print(cmd)
  os.system(cmd)

if recent_missing_dates_raw or recent_missing_dates_preprocess or recent_missing_dates_orderdata or argv['force_run']:
  if argv['force_run']:
    print("Force running popularity calculation .. ")
  cmd="/usr/bin/python /nykaa/scripts/feed_pipeline/popularity.py --popularity -n0 -y " 
  print(cmd)
  os.system(cmd)
else:
  print("Everything is up to date. Doing nothing.")

