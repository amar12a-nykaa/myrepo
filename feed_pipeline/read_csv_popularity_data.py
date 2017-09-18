import subprocess
import os
import argparse
import sys
import arrow
import csv
from pymongo import MongoClient

sys.path.append("/nykaa/scripts/utils")
from loopcounter import LoopCounter

parser = argparse.ArgumentParser()
parser.add_argument("--platform", '-p', required=True, help="app or web")
parser.add_argument("--file", '-f', required=True,)
parser.add_argument("--print-sample-row", action='store_true')
argv = vars(parser.parse_args())

assert argv['platform'] in ['app', 'web']

client = MongoClient()
raw_data = client['search']['raw_data']

files = reversed([
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
  ])

files = [argv['file']]

for filename in files:
  print(filename)
  os.system('sed -i "s/, 201/ 201/g" %s' % filename)
  os.system('sed -i "s/\\"//g" %s' % filename)

  nrows = int(subprocess.check_output('wc -l ' + filename, shell=True).decode().split()[0])
  ctr = LoopCounter("Reading CSV: ", total =nrows)
  with open(filename, newline='') as csvfile:

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

      required_keys = set(['views', 'productid', 'cart_additions', 'orders'])
      missing_keys = required_keys - set(list(d.keys()))
      if missing_keys:
        print("Missing Keys: %s" % missing_keys)
        raise Exception("Missing Keys in CSV")

      if not d['productid']:
        continue
      for k in ['cart_additions', 'views', 'orders']:
        d[k] = int(d[k])

      filt = {"date": date, "productid": d['productid'], "platform": argv['platform']}
      update = {k:v for k,v in d.items() if k in ['cart_additions', 'views', 'orders']}
      #print(filt)
      #print(update)
      if argv['print_sample_row']:
        print("row: %s" % d)
        print("filt: %s" % filt)
        print("update: %s" % update)
        sys.exit()
      ret = raw_data.update_one(filt, {"$set": update}, upsert=True) 
