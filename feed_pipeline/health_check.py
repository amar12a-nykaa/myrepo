import argparse
import pprint
import socket 
from IPython import embed
import arrow
from pymongo import MongoClient

host = socket.gethostname()

client = MongoClient()
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


def get_missing_dates(collname):
  if collname == 'raw_data':
    coll = raw_data 
  elif collname == 'processed_data':
    coll = processed_data 
  else:
    print("unknown collection")
    sys.exit()

  res = list(coll.aggregate([{"$group": {"_id": "$date", "count": {"$sum": 1}}}, {"$sort": {"_id":1}}]))

  dates_with_data = {x['_id'] for x in res}
  #embed()

#  yesterday = arrow.now().replace(days=-1, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
#  date = yesterday.replace(months=-6)
#  all_dates = set()
#  while date < yesterday:
#    all_dates.add(date.datetime.replace(tzinfo=None) )
#    date = date.replace(days=1)

  all_dates = enumerate_dates(-30*6, -1)
  missing_dates = all_dates - dates_with_data 
  return missing_dates


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("-m", "--mail", help='Mail this report', action='store_true')
  argv = vars(parser.parse_args())

  message = ""
  missing_dates = get_missing_dates('raw_data')
  if len(missing_dates) >= 1:
    msg = "Data missing for following dates in raw_data:\n"
    msg += pprint.pformat(missing_dates, indent=4)

  missing_dates = get_missing_dates('processed_data')
  if len(missing_dates) >= 1:
    msg += "\nData missing for following dates in processed_data:\n"
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

