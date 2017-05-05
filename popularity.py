import arrow
import re
import datetime 
import argparse
import os
import os.path
import sys
import mysql.connector

from collections import OrderedDict
from contextlib import closing

import arrow
import IPython

import omniture

embed = IPython.embed

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils 

TABLE = 'popularity'

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
parser.add_argument("--num-prods", '-n', required=True, help="Number of products to be fetched from omniture. Pass 0 for fetching all.", default=0, type=int)
parser.add_argument("--yes", '-y', help="Pass this to avoid prompt and run full report", action='store_true')
parser.add_argument("--startdate", help="startdate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace(days=-30).format('YYYY-MM-DD'))
parser.add_argument("--enddate", help="enddate in YYYYMMDD format or number of days to add from today i.e -4", type=valid_date, default=arrow.now().replace().format('YYYY-MM-DD'))
parser.add_argument("--dump-metrics", help="Dump metrics into a file", action='store_true')
parser.add_argument("--dont-run-report", action='store_true', help="Used for development. Uses sample report data.")
parser.add_argument("--dont-write-to-db", help="Only runs the report and prints.", action='store_true')
parser.add_argument("--dont-calculate-popularity", help="", action='store_true')
parser.add_argument("--debug", action='store_true')
argv = vars(parser.parse_args())

debug = argv['debug']

if argv['num_prods'] == 0 and not argv['yes'] and not argv['dont_run_report']:
  response = input("Are you sure you want to run full report? [Y/n]")
  if response == 'Y':
    print("Running full report .. ")
  else:
    print("Exiting")
    sys.exit()

if argv['num_prods'] == 0:
  print("=== Running full report. All data will be flushed. === ")

if not argv['dont_run_report'] or argv['dump_metrics']:
  analytics = omniture.authenticate('soumen.seth:FSN E-Commerce', '770f388b78d019017d5e8bd7a63883fb')
  suite = analytics.suites['fsnecommerceprod']

if argv['dump_metrics']:
  dir_path = os.path.dirname(os.path.realpath(__file__))
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


def fetch_data():
  if argv['dont_run_report']:
    for product in [{'product': '110406', 'category': '::unspecified::', 'event5': 88768, 'cartadditions': 3783, 'orders': 607}, {'product': '71406', 'category': '::unspecified::', 'event5': 86358, 'cartadditions': 13185, 'orders': 2012}, {'product': '121646', 'category': '::unspecified::', 'event5': 82681, 'cartadditions': 167, 'orders': 5}, {'product': '114009', 'category': '::unspecified::', 'event5': 61102, 'cartadditions': 7524, 'orders': 1021}, {'product': '61421', 'category': '::unspecified::', 'event5': 46153, 'cartadditions': 17883, 'orders': 2383}, {'product': '129627', 'category': '::unspecified::', 'event5': 43787, 'cartadditions': 13829, 'orders': 2317}, {'product': '112443', 'category': '::unspecified::', 'event5': 42340, 'cartadditions': 6336, 'orders': 1222}, {'product': '121634', 'category': '::unspecified::', 'event5': 40456, 'cartadditions': 89, 'orders': 0}, {'product': '6478', 'category': '::unspecified::', 'event5': 33264, 'cartadditions': 3522, 'orders': 622}, {'product': '92042', 'category': '::unspecified::', 'event5': 32633, 'cartadditions': 628, 'orders': 185}]:
      yield product
    return

  print("Running report .. ")
  total_rows = argv['num_prods'] or 0
  if not total_rows:
    top = 50000
  else:
    top = min(total_rows, 50000)

  #print("total_rows: %s top=%s" %(total_rows, top))
  startingWith = 0 
  report_cnt = 1
  while(True):
    print("== report  %d ==" % report_cnt)
    report_cnt += 1
    report = suite.report \
        .metric('event5') \
        .metric('cartadditions') \
        .metric('orders') \
        .element('product', top=top, startingWith=startingWith)\
        .element('category')\
        .range(argv['startdate'], argv['enddate'])\
        .run()
        #.element('evar3', top=50000, startingWith=0) \
        #.element('evar1')\
        #.metric('orders') \
        #.granularity('day')\
        #.filter(element='evar91', selected=['Product Detail Page'])\
    print(" --- ")
    #print(report)
    data = report.data
    if argv['dont_write_to_db']:
      print(data)
    for product in data:
      yield product

    if len(data) < top or (total_rows and top * report_cnt > total_rows):
      break
    startingWith += top



def write_report_data_to_db():

  with closing(conn.cursor()) as cursor:
    query = "delete from %s" % TABLE
    #print(query)
    cursor.execute(query)
    conn.commit()


  with closing(conn.cursor()) as cursor:
    for product in fetch_data():
      #print(product['product'])
      d = OrderedDict({
        "views": product['event5'],
        "cart_additions": product['cartadditions'],
        "orders": product['orders'],
        "productid": product['product'],
      })

      d['cart_additions'] = max(d['orders'], d['cart_additions'])
      d['views'] = max(d['orders'], d['views'])

      types =  {
        "views": int,
        "cart_additions": int,
        "orders": int,
        "productid": str
      }
      map_type = {
        str: "'%s'",
        int: "%d"
      }

      fields_list = ", ".join(d.keys())
      values_format_list = ", ".join( [ map_type[types[x]] for x in d.keys()  ])
      values_list = list(d.values())

      query = "replace into "+TABLE+" (" + fields_list + ") VALUES(" +values_format_list + ") "
      query = query % tuple(values_list)

      if debug: print(query)
      cursor.execute(query)
    conn.commit()


conn = Utils.mysqlConnection()
if not argv['dont_write_to_db']:
  write_report_data_to_db()
else:
  print("Skipped writing into DB")


total_prods = Utils.fetchResults(conn, "select count(*) as cnt from %s" % TABLE)[0]['cnt']
print("total_prods: %s" % total_prods)

num_steps = 100
step_size = total_prods / num_steps
step_boundaries = []
step_boundaries.append(0)
for i in range(0, num_steps -1 ):
  skip = (i + 1) * int(step_size )
  query =  "select views from {TABLE} order by views limit {skip}, 1".format(TABLE=TABLE, skip=skip)
  print(query)
  val = Utils.fetchResults(conn, query )[0]['views']
  print(val)
  step_boundaries.append(val)

print("step_boundaries: %s" % step_boundaries)
for i in range(0, num_steps):
  print("--")
  minm = step_boundaries[i]
  max_clause = ""
  if i < num_steps -1:
    maxm = step_boundaries[i+1]
    max_clause = " and views <= %d" % maxm
  views_points = 1 * ((i+1)*1.0/num_steps)
  print("min:%d max:%d" % (minm, maxm))
  query = "select * from {TABLE} where views > {minm} {max_clause}".format(TABLE=TABLE, minm=minm, max_clause=max_clause)
  print(query)
  results = Utils.fetchResults(conn, query)
  print(results)
  for res in results:
    orders = res['orders']
    cart_additions = max(orders, res['cart_additions'], )
    views = max(cart_additions, res['views'])

    print(orders, cart_additions, views)
    if cart_additions and views:
      add_to_cart_by_views = cart_additions/views
      orders_by_add_to_cart = orders/cart_additions
    else:
      add_to_cart_by_views = 0 
      orders_by_add_to_cart = 0 

    f1 = 1 
    f2 = 1
    popularity = (f1 * add_to_cart_by_views + f2 * orders_by_add_to_cart ) *   views_points
    popularity = round(popularity /(f1 + f2 ) * 100, 2)

    print("popularity: %s" % popularity)
    with closing(conn.cursor()) as cursor:
      query = "update {TABLE} set popularity = {popularity}, orders_by_add_to_cart = {orders_by_add_to_cart}, add_to_cart_by_views={add_to_cart_by_views}, views_points = {views_points} where productid = '{productid}'".format(TABLE=TABLE, popularity=popularity, orders_by_add_to_cart=orders_by_add_to_cart, add_to_cart_by_views=add_to_cart_by_views, views_points=views_points, productid=res['productid'])
      print(query)
      if popularity>100:
        embed()
        sys.exit()
      cursor.execute(query)
      conn.commit()

