"""Script to trigger feed pipeline. Fixes anything missing for last 5 days """

import os
import sys
sys.path.append("/nykaa/scripts/feed_pipeline")
from health_check import get_missing_dates, enumerate_dates
from read_csv_popularity_data import read_file_by_date

missing_dates = get_missing_dates('raw_data')
print("missing_dates: %s" % missing_dates)
last_5_dates = enumerate_dates(-5, -1)

recent_missing_dates_raw = missing_dates & last_5_dates
print(recent_missing_dates_raw)

for date in recent_missing_dates_raw:
  print("\n\n\n")
  print("=== CRONRUNNER  =====")
  print("=== READING CSV FOR : %s ====" % date)
  read_file_by_date(date, 'web')
  read_file_by_date(date, 'app')


missing_dates = get_missing_dates('processed_data')
print("preprocessing missing_dates: %s" % missing_dates)
last_5_dates = enumerate_dates(-5, -1)

recent_missing_dates_preprocess = (missing_dates & last_5_dates) | recent_missing_dates_raw
print(recent_missing_dates_preprocess)

for date in recent_missing_dates_preprocess:
  print("\n\n\n")
  print("=== PREPROCESSING  FOR : %s ====" % date)
  d = date.strftime("%Y-%m-%d")
  cmd="/usr/bin/python /nykaa/scripts/feed_pipeline/popularity.py --startdate %s --enddate %s --preprocess --popularity -n0 -y " % (d, d)
  print(cmd)
  os.system(cmd)

