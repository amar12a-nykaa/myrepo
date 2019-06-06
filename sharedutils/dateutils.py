import re
import arrow

def valid_date(s):
  try:
    if re.search("^-?[0-9]+$", s):
      adddays = int(s)
      assert abs(adddays) < 500, "Reports can be fetched only 500 days in past."
      now = arrow.utcnow()
      return now.replace(days=adddays).format('YYYY-MM-DD')
    else:
      return arrow.get(s, 'YYYY-MM-DD').format('YYYY-MM-DD')
  except ValueError:
    msg = "Not a valid date: '{0}'.".format(s)
    raise argparse.ArgumentTypeError(msg)

def enumerate_dates(startdate, enddate):
  lastdate = arrow.now().replace(days=enddate, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  date =  arrow.now().replace(days=startdate, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
  all_dates = set()
  while date <= lastdate:
    all_dates.add(date.datetime.replace(tzinfo=None) )
    date = date.replace(days=1)
  return all_dates



if __name__ == "__main__":

  print(enumerate_dates(-3, +2))

