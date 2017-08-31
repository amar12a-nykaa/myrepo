import argparse
import pprint
import socket 

import arrow
from pymongo import MongoClient


parser = argparse.ArgumentParser()
parser.add_argument("-m", "--mail", help='Mail this report', action='store_true')
argv = vars(parser.parse_args())


host = socket.gethostname()

client = MongoClient()
raw_data = client['search']['raw_data']
processed_data = client['search']['processed_data']
popularity = client['search']['popularity']

res = list(raw_data.aggregate([{"$group": {"_id": "$date", "count": {"$sum": 1}}}, {"$sort": {"_id":1}}]))

dates = {x['_id'] for x in res}
#print(dates[0:10])

yesterday = arrow.now().replace(days=-1, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
date = yesterday.replace(months=-6)
all_dates = set()
while date < yesterday:
  all_dates.add(date.datetime.replace(tzinfo=None) )
  date = date.replace(days=1)
message = ""

missing_dates = all_dates - dates 
if len(missing_dates) >= 1:
  msg = "Data missing for following dates:"
  msg += pprint.pformat(missing_dates, indent=4)


if popularity.count() < 60000:
  msg += "\n"
  msg += "[ERROR] Number of products in popularity is less than 60K.\n"

print(msg)

if argv['mail']:
  if not msg:
    print("Not sending a mail - Message empty")
  # Email the report 
  from marrow.mailer import Mailer, Message

  mailer = Mailer(dict( transport = dict( use = 'smtp', host = 'localhost')))
  mailer.start()

  message = Message(author="no-reply@nykaa.com", to="mayank@gludo.com")
  message.subject = "Feed Pipeline Report @ %s" % host
  message.plain = msg
  mailer.send(message)

  mailer.stop()
  print("Sent a mail")

