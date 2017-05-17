import arrow
from pymongo import MongoClient
client = MongoClient()
raw_data = client['search']['raw_data']

print(raw_data.find_one())

import csv
for filename in reversed([
  '/home/ubuntu/Product_feed_day_wise/product_data_201606.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201607.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201608.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201609.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201610.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201611.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201612.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201701.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201702.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201703.csv',
  '/home/ubuntu/Product_feed_day_wise/product_data_201704.csv',
  ]):
  print(filename)
  cnt = 0 
  with open(filename, newline='') as csvfile:
    spamreader = csv.DictReader(csvfile,)
    for row in spamreader:
      cnt +=1 
      if cnt %1000 == 0 :
        print("%s rows processed"  % cnt)
      d = dict(row)
      date = arrow.get(d['datetime']).datetime
      filt = {"date": date, "productid": d['name']}
      update = {"views": int(d["event5"]), "cart_additions": int(d['cartadditions']), "orders": int(d['orders'])}
      raw_data.update_one(filt, {"$set": update}, upsert=True) 
