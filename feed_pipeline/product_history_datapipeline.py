from datetime import datetime, timedelta
import dateutil.relativedelta
import datetime as dt
import sys

sys.path.append('/home/apis/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils

sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils


client = MongoUtils.getClient()
raw_data = client['search']['raw_data']
product_history_table = client['search']['product_history']
product_history_table_inactive = client['search']['product_history_inactive']
end_date = datetime.now()
start_date = datetime.now() - dateutil.relativedelta.relativedelta(months=1)

db_result = raw_data.aggregate([{"$match":{ "date": {"$gte": dt.datetime(start_date.year,start_date.month,start_date.day), "$lte": dt.datetime(end_date.year,end_date.month,end_date.day)}}},{"$group":{"_id":"$product_id","cart_additions":{"$sum":"$cart_additions"},"orders": {"$sum": "$orders"},"revenue": {"$sum": "$revenue"},"units": {"$sum": "$units"},"views": {"$sum": "$views"}}}])

result = list(db_result)
for row in result:
  product_history_table_inactive.insert_one({"_id":row['_id'],"units_sold_in_last_month":row['units'], "cart_additions_in_last_month":row['cart_additions'],"views_in_last_month":row['views'],"revenue_in_last_month": row['revenue'],"orders_in_last_month":row['orders']})

if product_history_table_inactive.count():
  try:
    difference = (abs(product_history_table_inactive.count() - product_history_table.count()) / product_history_table.count()) * 100
    if difference<5:
      product_history_table.drop()
      product_history_table_inactive.rename("product_history")
      print("product_history table updated successfully.")
    else:
      print("Error!! Please verify the data..")
  except ZeroDivisionError:
    print("Older table is Empty, creating new table")
    product_history_table_inactive.rename("product_history")
