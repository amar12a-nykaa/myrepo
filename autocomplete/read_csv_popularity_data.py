import time 
import datetime
import subprocess
import os
import argparse
import sys
import arrow
import csv
from datetime import date, timedelta
from pymongo import MongoClient
from IPython import embed

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter
from cliutils import CliUtils

sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils

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

  client = MongoClient()
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
  with open(filepath, newline='') as csvfile:
    spamreader = csv.DictReader(csvfile,)
    for row in spamreader:
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

