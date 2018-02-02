import re
from datetime import timedelta, date
import time 
import datetime
import subprocess
import os
import argparse
import sys
import arrow
import csv
from pymongo import MongoClient
from IPython import embed

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter

sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils, MemcacheUtils

def valid_date(s):
  try:
    if re.search("^-?[0-9]+$", s):
      adddays = int(s)
      assert abs(adddays) < 500, "Reports can be fetched only 500 days in past."
      now = arrow.utcnow()
      return now.replace(days=adddays).format('YYYY-MM-DD')
    else:
      return arrow.get(s, 'YYYY-MM-DD').format('YYYY-MM-DD')
  except ValueError:
    msg = "Not a valid date: '{0}'.".format(s)
    raise argparse.ArgumentTypeError(msg)


def create_product_id_index():
  start = time.time()
  product_id_index = {}

  nykaa_mysql_conn = Utils.nykaaMysqlConnection()
  query = """
    SELECT a.entity_id as product_id, a.key_id as parent_id
    FROM(
      SELECT cpe.entity_id,cpe.sku, CASE WHEN cpsl.parent_id IS NOT NULL THEN cpsl.parent_id ELSE cpe.entity_id END AS 'key_id'
      FROM nykaalive1.catalog_product_entity cpe
      LEFT JOIN nykaalive1.catalog_product_super_link cpsl 
      ON cpsl.product_id = cpe.entity_id
      WHERE cpe.sku IS NOT NULL
    )a
    JOIN 
    nykaalive1.catalog_product_entity cpe 
    ON cpe.entity_id=a.key_id
    JOIN 
    nykaalive1.`catalog_product_entity_varchar` c 
    ON c.`entity_id`=a.key_id AND c.`attribute_id`=56
    """
  
  delta = time.time() - start 
  for p in Utils.mysql_read(query, connection=nykaa_mysql_conn):
    p = {k:str(v) for k,v in p.items()}
    product_id_index[p['product_id']] = p

  print("Time taken to create product_id index: %s seconds" % round(delta))
  if len(product_id_index.keys()) < 50000:
    print("Failed to create product_id_index. Master feed might be missing.")
  return product_id_index

product_id_index = create_product_id_index()


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
    filename = '/nykaa/adminftp/website_data_all_metrics_%s.csv' %  date.strftime("%Y%m%d")
    if not os.path.exists(filename):
      filename = '/nykaa/adminftp/website_data_all_metrics_%s.zip' %  date.strftime("%Y%m%d")

  elif platform == 'app':
    filename = '/nykaa/adminftp/App_data_all_metrics_%s.csv' %  date.strftime("%Y%m%d")
    if not os.path.exists(filename):
      filename = '/nykaa/adminftp/App_data_all_metrics_%s.zip' %  date.strftime("%Y%m%d")

  print(filename)
  return read_file(filename, platform, dryrun, limit=limit, product_id=product_id, debug=debug)

def read_file(filepath, platform, dryrun, limit=0, product_id=None, debug=False):
  if not limit:
    limit = 0
  product_id_arg = product_id

  assert platform in ['app', 'web']

  client = MongoClient()
  raw_data = client['search']['raw_data']


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
        elif 'datetime' in d:
          d['date'] = d.pop('datetime')

        for _format in ['MMM D YYYY', 'MMMM D YYYY', 'YYYY-MM-DD']:
          try:
            date = arrow.get(d['date'], _format).datetime
          except:
            pass
          else:
            break

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

      required_keys = set(['views', 'product_id', 'cart_additions', 'orders'])
      missing_keys = required_keys - set(list(d.keys()))
      if missing_keys:
        print("Missing Keys: %s" % missing_keys)
        raise Exception("Missing Keys in CSV")

      if not d['product_id']:
        continue

      if product_id_arg:
        #embed()
        #exit()
        if d['product_id'] != product_id_arg:
          continue
        
      for k in ['cart_additions', 'views', 'orders', 'revenue', 'units']:
        if k == 'revenue':
          d[k] = float(d[k])
        else:
          d[k] = int(d[k])

      if not d['product_id']:
        print("Skipping empty product_id.")
        continue
      
      if d['product_id'] in product_id_index:
        d.update(product_id_index[d['product_id']])

      filt = {"date": date, "product_id": d['product_id'], "platform": platform}
      update = {k:v for k,v in d.items() if k in ['cart_additions', 'views', 'orders', 'revenue', 'units', 'parent_id']}
      if debug:
        print("d: %s" % d)
        print("filt: %s" % filt)
        print("update: %s" % update)

      if not dryrun:
        ret = raw_data.update_one(filt, {"$set": update}, upsert=True) 
        if debug:
          print("Mongo response: %s" % ret.raw_result)
if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("--platform", '-p', required=True, help="app or web")
  parser.add_argument("--filepath", '-f',)
  parser.add_argument("--dryrun",  action='store_true')
  parser.add_argument("--debug",  action='store_true')
  parser.add_argument("--limit", type=int, default=0)
  parser.add_argument("--id", default=0)

  parser.add_argument("--startdate", help="startdate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
  parser.add_argument("--enddate", help="enddate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace().format('YYYY-MM-DD'))

  argv = vars(parser.parse_args())
  if argv['filepath']:
    read_file(filepath=argv['filepath'], platform=argv['platform'], dryrun=argv['dryrun'], limit=argv['limit'], product_id=argv['id'], debug=argv['debug'])
  else:
    read_file_by_dates(startdate=argv['startdate'], enddate=argv['enddate'], platform=argv['platform'], dryrun=argv['dryrun'], limit=argv['limit'], product_id=argv['id'], debug=argv['debug'])
      

