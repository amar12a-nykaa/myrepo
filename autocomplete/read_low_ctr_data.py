import argparse
import datetime
import os
import os.path
import pymongo
import subprocess
import sys
from datetime import timedelta

import arrow


sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter
from cliutils import CliUtils
from mongoutils import MongoUtils


DAILY_COUNT_THRESHOLD = 2 

client = MongoUtils.getClient()
search_click_data = client['search']['search_click_data']


def create_missing_indices():
  indices = search_click_data.list_indexes()
  if 'date_1_query_1' not in [x['name'] for x in indices]:
      print('Creating missing index for low_ctr...')
      search_click_data.create_index([("date", pymongo.ASCENDING), ("query", pymongo.ASCENDING)])

create_missing_indices()

def read_file_by_dates(startdate, enddate):
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
    read_file_by_date(single_date)


def read_file_by_date(date):
  date = datetime.datetime.strptime(date,  "%Y-%m-%d") if isinstance(date, str) else date
  assert isinstance(date, datetime.datetime) or  isinstance(date, datetime.date), "Bad date format"
  filename = '/nykaa/adminftp/auto_low_per%s.csv' %  date.strftime("%Y%m%d")
  if not os.path.exists(filename):
    filename = '/nykaa/adminftp/auto_low_per%s.zip' %  date.strftime("%Y%m%d")
  print(filename)
  return read_file(filename, date)


def read_file(filepath, date):
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
  os.system('sed -i "s/\\"//g" %s' % filepath)
  nrows = int(subprocess.check_output('wc -l ' + filepath, shell=True).decode().split()[0])
  ctr = LoopCounter("Reading CSV: ", total=nrows)
  first_row = True
  headers = None
  date = datetime.datetime.combine(date, datetime.datetime.min.time())
  with open(filepath, newline='', encoding='utf-8-sig') as csvfile:
    for r in csvfile:
      r=r.strip().split(",")
      row = [",".join(r[0:-2])] + r[-2:]
      if first_row:
        first_row = False
        headers = row
        required_keys = set(['Internal Search Term (Conversion) (evar6)',
                             'Internal Search Term (Conversion) Instance (Instance of evar6)',
                             'Click Interaction Instance (Instance of evar78)'])
        missing_keys = required_keys - set(headers)
        if missing_keys:
          print("Missing Keys: %s" % missing_keys)
          raise Exception("Missing Keys in CSV")
        continue

      row = dict(list(zip(headers, row)))
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)

      d = dict(row)
      d['date'] = date
      d['query'] = d.pop("Internal Search Term (Conversion) (evar6)")
      d['search_instance'] = d.pop("Internal Search Term (Conversion) Instance (Instance of evar6)")
      d['click_instance'] = d.pop("Click Interaction Instance (Instance of evar78)")

      is_data_good = True
      for k in ['search_instance', 'click_instance']:
          try:
              if not d[k]:
                  d[k] = 0
              d[k] = int(d[k])
          except:
              print("Error in processing: %s" % d)
              is_data_good = False
      if not is_data_good:
          continue

      terms  = d['query'].split("|")
      try:
        if len(terms) == 2:
          d['query'] = d['query'].split("|")[1]
        else:
          d['query'] = d['query'].split("|")[0]
        assert d['query']
      except:
        print("Error in processing: %s" % d)
        continue

      filt = {"date": date, "query": d['query']}
      update = {k:v for k,v in d.items() if k in ['search_instance', 'click_instance', 'date', 'query']}

      if update['search_instance'] < DAILY_COUNT_THRESHOLD :
        continue
      try:
        ret = search_click_data.update_one(filt, {"$set": update}, upsert=True)
      except:
        print("filt: %s" % filt)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("--startdate", help="startdate in YYYYMMDD format or number of days to add from today i.e -4", type=CliUtils.valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
  parser.add_argument("--enddate", help="enddate in YYYYMMDD format or number of days to add from today i.e -4", type=CliUtils.valid_date, default=arrow.now().replace().format('YYYY-MM-DD'))
  argv = vars(parser.parse_args())

  read_file_by_dates(startdate=argv['startdate'], enddate=argv['enddate'])

