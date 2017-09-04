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
argv = vars(parser.parse_args())

assert argv['platform'] in ['app', 'web']

client = MongoClient()
raw_data = client['search']['raw_data']

files = reversed([
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
  '/tmp/gludo_app_20170501-20170823.csv'
  ])

files = [argv['file']]

for filename in files:
  print(filename)
  os.system('sed -i "s/, 201/ 201/g" %s' % filename)
  os.system('sed -i "s/\\"//g" %s' % filename)

  ctr = LoopCounter("Reading CSV: ")
  with open(filename, newline='') as csvfile:

    spamreader = csv.DictReader(csvfile,)
    for row in spamreader:
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)
      try:
        d = dict(row)
        date = None

        try:
          date = arrow.get(d['\ufeffDate'], 'MMM D YYYY').datetime
        except:
          pass

        if not date:
          date = arrow.get(d['\ufeffDate'], 'MMMM D YYYY').datetime
      except KeyError:
        print(d)
        raise

      filt = {"date": date, "productid": d['Products'], "platform": argv['platform']}
      update = {"views": int(d["Product Views"]), "cart_additions": int(d['Cart Additions']), "orders": int(d['Orders'])}
      #print(filt)
      #print(update)
      ret = raw_data.update_one(filt, {"$set": update}, upsert=True) 
