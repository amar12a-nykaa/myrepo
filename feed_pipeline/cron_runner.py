"""Script to trigger feed pipeline. Fixes anything missing for last 5 days """

import os
import sys
import argparse
sys.path.append("/nykaa/scripts/feed_pipeline")
from health_check import get_missing_dates, enumerate_dates
from read_csv_popularity_data import read_file_by_date

parser = argparse.ArgumentParser()
parser.add_argument("--days", type=int, default=5)
argv = vars(parser.parse_args())
days = -1 * argv['days']

missing_dates = get_missing_dates('raw_data')
print("raw_data missing_dates in last 6 months: %s" % missing_dates)
last_5_dates = enumerate_dates(days, -1)

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
last_5_dates = enumerate_dates(days, -1)

recent_missing_dates_preprocess = (missing_dates & last_5_dates) | recent_missing_dates_raw
print("preprocessing missing_dates: %s" % recent_missing_dates_preprocess)

for date in recent_missing_dates_preprocess:
  print("\n\n\n")
  print("=== PREPROCESSING  FOR : %s ====" % date)
  d = date.strftime("%Y-%m-%d")
  cmd="/usr/bin/python /nykaa/scripts/feed_pipeline/popularity.py --startdate %s --enddate %s --preprocess -n0 -y " % (d, d)
  print(cmd)
  os.system(cmd)
if recent_missing_dates_raw or recent_missing_dates_preprocess:
  cmd="/usr/bin/python /nykaa/scripts/feed_pipeline/popularity.py --popularity -n0 -y " 
  print(cmd)
  os.system(cmd)
else:
  print("Everything is up to date. Doing nothing.")

