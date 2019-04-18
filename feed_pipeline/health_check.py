import sys
import argparse
import csv
import pprint
import socket

import arrow
from IPython import embed
from pymongo import MongoClient
import sys

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

host = socket.gethostname()

client = Utils.mongoClient()
raw_data = client['search']['raw_data']
processed_data = client['search']['processed_data']
popularity = client['search']['popularity']



def enumerate_dates(startdate, enddate):
  lastdate = arrow.now().replace(days=enddate, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  date =  arrow.now().replace(days=startdate, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  all_dates = set()
  while date <= lastdate:
    all_dates.add(date.datetime.replace(tzinfo=None) )
    date = date.replace(days=1)
  return all_dates


def get_missing_dates(collname, filt=None):
  
  coll = client['search'][collname]
  
  pipe = []
  if filt:
    assert isinstance(filt, dict)
    pipe.append({"$match": filt})
  pipe += [{"$group": {"_id": "$date", "count": {"$sum": 1}}}, {"$sort": {"_id":1}}]
  res = list(coll.aggregate(pipe))

  dates_with_data = {x['_id'] for x in res}

  all_dates = enumerate_dates(-30*6, 0)
  missing_dates = all_dates - dates_with_data 
  return missing_dates


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("-m", "--mail", help='Mail this report', action='store_true')
  argv = vars(parser.parse_args())

  msg = ""
  for platform in ['web', 'app']:
    message = ""
    missing_dates = get_missing_dates('raw_data', filt={"platform": platform})
    if len(missing_dates) >= 1:
      msg += "Data missing for following dates in raw_data for %s:\n" % platform
      msg += pprint.pformat(missing_dates, indent=4)

  missing_dates = get_missing_dates('processed_data')
  if len(missing_dates) >= 1:
    msg += "\nData missing for following dates in processed_data for %s:\n" % platform
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
