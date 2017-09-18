import datetime
import subprocess
import os
import argparse
import sys
import arrow
import csv
from pymongo import MongoClient

sys.path.append("/nykaa/scripts/utils")
from loopcounter import LoopCounter

def read_file_by_date(date, platform, dryrun=False):
  assert isinstance(date, datetime.datetime)
  if platform == 'web':
    filename = '/nykaa/adminftp/website_data_all_metrics_%s.csv' %  date.strftime("%Y%m%d")
  elif platform == 'app':
    filename = '/nykaa/adminftp/App_data_all_metrics_%s.zip' %  date.strftime("%Y%m%d")

  print(filename)
  return read_file(filename, platform, dryrun)

def read_file(filepath, platform, dryrun):
  assert platform in ['app', 'web']

  client = MongoClient()
  raw_data = client['search']['raw_data']

  #files = reversed([
#  '/home/ubuntu/Product_feed_day_wise/product_data_201605.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201606.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201607.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201608.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201609.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201610.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201611.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201612.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201701.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201702.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201703.csv',
#  '/home/ubuntu/Product_feed_day_wise/product_data_201704.csv',
#  '/tmp/gludo_app_20170501-20170823.csv'
   # ])

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
      ctr += 1
      if ctr.should_print():
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
        ('Products', 'productid'),
        ('name', 'productid'),
        ('Cart Additions', 'cart_additions'),
        ('cartadditions', 'cart_additions'),
        ('Orders', 'orders'),
        ]

      for k,v in replace_keys:
        if k in d:
          d[v] = d.pop(k)

      for k in ['cart_additions', 'views', 'orders']:
        d[k] = int(d[k])

      if not d['productid']:
        print("Skipping empty productid.")
        continue

      filt = {"date": date, "productid": d['productid'], "platform": platform}
      update = {k:v for k,v in d.items() if k in ['cart_additions', 'views', 'orders']}
      if dryrun:
        print("d: %s" % d)
        print("filt: %s" % filt)
        print("update: %s" % update)
        sys.exit()

      if not dryrun:
        ret = raw_data.update_one(filt, {"$set": update}, upsert=True) 

if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument("--platform", '-p', required=True, help="app or web")
  parser.add_argument("--filepath", '-f', required=True,)
  parser.add_argument("--dryrun",  action='store_true')
  argv = vars(parser.parse_args())
  read_file(filepath=argv['filepath'], platform=argv['platform'], dryrun=argv['dryrun'])

