import arrow
import re
import datetime 
import argparse
import os
import os.path
import sys

import arrow
import IPython

import omniture

embed = IPython.embed

def valid_date(s):
  try:
    if re.search("^-?[0-9]+$", s):
      adddays = int(s)
      assert abs(adddays) < 500, "Reports can be fetched only 500 days in past." 
      now = arrow.utcnow()
      return now.replace(days=adddays).format('YYYY-MM-DD')
    else:
      return arrow.get(s, 'YYYY-MM-DD').format('YYYY-MM-DD')
      #return datetime.datetime.strptime(s, "%Y-%m-%d")
  except ValueError:
    msg = "Not a valid date: '{0}'.".format(s)
    raise argparse.ArgumentTypeError(msg)


parser = argparse.ArgumentParser()
parser.add_argument("--startdate", help="startdate in YYYYMMDD format", type=valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
parser.add_argument("--enddate", help="enddate in YYYYMMDD format", type=valid_date, default=arrow.now().replace(days=-1).format('YYYY-MM-DD'))
parser.add_argument("--dump-metrics", help="Dump metrics into a file", action='store_true')
argv = vars(parser.parse_args())

#print(argv)
#sys.exit()

analytics = omniture.authenticate('soumen.seth:FSN E-Commerce', '770f388b78d019017d5e8bd7a63883fb')
dir_path = os.path.dirname(os.path.realpath(__file__))

suite = analytics.suites['fsnecommerceprod']

if argv['dump_metrics']:
  print("Dumping metrics in files:" )
  filename = os.path.join(dir_path, "analytics.suites.txt")
  with open(filename, 'w') as f:
    f.write("%s" % suite)
  f1 = filename

  filename = os.path.join(dir_path, "metrics.txt")
  with open(filename, 'w') as f:
    f.write("%s" % suite.metrics)
  f2 = filename

  filename = os.path.join(dir_path, "elements.txt")
  with open(filename, 'w') as f:
    f.write("%s" % suite.elements)
  f3 = filename

  print('{f1}\n{f2}\n{f3}'.format(f1=f1,f2=f2, f3=f3))

  sys.exit()

#"event5", "cartadditions", "orders"

report = suite.report \
    .metric('cartadditions') \
    .metric('event5') \
    .metric('orders') \
    .range(argv['startdate'], argv['enddate'])\
    .run()
    #.element('evar3', top=50000, startingWith=0) \
    #.element('evar1')\
    #.metric('orders') \
    #.granularity('day')\
    #.filter(element='evar91', selected=['Product Detail Page'])\
print(" --- ")
print(report)

#print(report.keys())
