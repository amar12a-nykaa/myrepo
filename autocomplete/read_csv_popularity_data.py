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
from pas.v1.utils import Utils


def read_file_by_date(date, platform, dryrun=False):
  assert isinstance(date, datetime.datetime)
  if platform == 'web':
    filename = '/nykaa/adminftp/search_terms_website_%s.zip' %  date.strftime("%Y%m%d")
  elif platform == 'app':
    filename = '/nykaa/adminftp/search_terms_app_%s.zip' %  date.strftime("%Y%m%d")

  print(filename)
  return read_file(filename, platform, dryrun)

def read_file(filepath, platform, dryrun, limit=0, product_id=None, debug=False):
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

      #required_keys = set(['views', 'product_id', 'cart_additions', 'orders'])
      required_keys = set(['Date', 'Internal Search Term (Conversion) (evar6)', 'Internal Search Term (Conversion) Instance (Instance of evar6)'])
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
  parser.add_argument("--filepath", '-f', required=True,)
  parser.add_argument("--dryrun",  action='store_true')
  parser.add_argument("--debug",  action='store_true')
  parser.add_argument("--limit", type=int, default=0)
  parser.add_argument("--id", default=0)
  argv = vars(parser.parse_args())
  read_file(filepath=argv['filepath'], platform=argv['platform'], dryrun=argv['dryrun'], limit=argv['limit'], product_id=argv['id'], debug=argv['debug'])

