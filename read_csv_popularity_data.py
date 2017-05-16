import arrow
from pymongo import MongoClient
client = MongoClient()
raw_data = client['search']['raw_data']

print(raw_data.find_one())

import csv
cnt = 0 
with open('/Users/mayank/Downloads/Product_feed_day_wise/product_data_201606.csv', newline='') as csvfile:
  spamreader = csv.DictReader(csvfile,)
  for row in spamreader:
    cnt +=1 
    if cnt == 10:
      break
    d = dict(row)
    print(d)
    date = arrow.get(d['datetime']).datetime
    filt = {"date": date, "productid": d['name']}
    update = {"views": int(d["event5"]), "cart_additions": int(d['cartadditions']), "orders": int(d['orders'])}
    print("filt: %r" % filt)
    print(update)
    raw_data.update_one(filt, {"$set": update}, upsert=True) 
