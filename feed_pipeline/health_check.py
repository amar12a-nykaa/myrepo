from pymongo import MongoClient 
import arrow
import pprint

client = MongoClient()

raw_data = client['search']['raw_data']
processed_data = client['search']['processed_data']
popularity = client['search']['popularity']

res = list(raw_data.aggregate([{"$group": {"_id": "$date", "count": {"$sum": 1}}}, {"$sort": {"_id":1}}]))

dates = {x['_id'] for x in res}
#print(dates[0:10])

today = arrow.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
date = today.replace(months=-6)
all_dates = set()
while date < today:
  all_dates.add(date.datetime.replace(tzinfo=None) )
  date = date.replace(days=1)

missing_dates = all_dates - dates 
print(dates.pop())
print(all_dates.pop())
if len(missing_dates) >= 1:
  print("Data missing for following dates:")
  pprint.pprint(missing_dates)
