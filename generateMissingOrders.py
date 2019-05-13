#!/usr/bin/python
import argparse
import csv
import datetime
import os
import re
import subprocess
import sys
import traceback
from datetime import datetime, timedelta

import arrow
import IPython
from pytz import timezone


sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils


parser = argparse.ArgumentParser()
parser.add_argument('--sku')
argv = vars(parser.parse_args())
#print(argv)


# Time Calculations
now = arrow.utcnow()
end = now.replace( minute=0, second=0, microsecond=0)
start = end.replace(hours=-1)
time_from = start.format('YYYY-MM-DD HH:mm:ss')
time_to = end.format('YYYY-MM-DD HH:mm:ss')

def generateMagentoOrders():
  with open('magento_orders.csv', 'w') as csvfile:
    fieldnames = ['sku', 'quantity', 'quote_id']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    nykaa_mysql_conn = PasUtils.nykaaMysqlConnection()
    query = """SELECT sku, qty_ordered AS quantity, quote_id
               FROM sales_flat_order_item sfoi JOIN sales_flat_order sfo 
               ON (sfoi.order_id = sfo.entity_id)
               WHERE sfo.created_at BETWEEN '%s' AND '%s'
                     AND sfoi.product_type = 'simple'
          """ % (time_from, time_to)

    if argv['sku']:
      query = "select * from (%s)A where sku = '%s'" % (query, argv['sku'])
      print(query)
    results = PasUtils.fetchResults(nykaa_mysql_conn, query)
    for row in results:
      writer.writerow({'sku': row['sku'], 'quantity': int(row['quantity']), 'quote_id': int(row['quote_id'])})


def readOrdersData(filename):
  orders = {'skus': {}, 'ids': {}}
  with open(filename, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      sku = row['sku'].strip()
      quantity = int(row['quantity'].strip())
      quote_id = int(row['quote_id'].strip())

      if sku not in orders['skus']:
        orders['skus'][sku] = 0
      if quote_id not in orders['ids']:
        orders['ids'][quote_id] = 0

      orders['skus'][sku] += quantity
      orders['ids'][quote_id] += quantity

  return orders


def getOrderMismatches(magento_orders, gludo_orders):
  for sku, magento_quantity in magento_orders['skus'].items():
    gludo_quantity = gludo_orders['skus'].get(sku, 0)
    if magento_quantity != gludo_quantity:
      diff = abs(magento_quantity - gludo_quantity)
      print("%s quantity deducted is off by %d. Magento quantity deducted: %d, Gludo quantity deducted: %d" % (sku, diff, magento_quantity, gludo_quantity))

  print("==============")
  for quote_id, magento_quantity in magento_orders['ids'].items():
    gludo_quantity = gludo_orders['ids'].get(quote_id)
    if not gludo_quantity:
      print("Gludo quantity not deducted for quote id: %d. Magento quantity deducted: %d" % (quote_id, magento_quantity))
    elif magento_quantity != gludo_quantity:
      diff = abs(magento_quantity - gludo_quantity)
      print("Quote id %d quantity is off by %d. Magento quantity: %d, Gludo quantity: %d" % (quote_id, diff, magento_quantity, gludo_quantity))


def generateGludoOrders():

  DIR = "/tmp/error_logs_api_machines"
  machines = ['52.220.215.78' , '52.221.72.116', '52.221.34.173', '52.77.199.176']
  os.system("mkdir -p %s" % DIR)

  datestrs = [start.format('YYYY-MM-DD')]

  outfile = "/tmp/qty_decs.txt"
  os.system("rm -f %s" % outfile)
  for machine in machines:
    #print(machine)
    dir1 = DIR + "/" + machine
    os.system("mkdir -p %s" % dir1)

    remotefile = "/var/log/apache2/error.log"
    basename = os.path.basename(remotefile)
    localfile = DIR + "/%s/%s" % (machine, basename)
    cmd = "scp -i /root/.ssh/id_rsa ubuntu@%s:%s %s" % (machine, remotefile, localfile)
    print(cmd)
    os.system(cmd)

    cmd = 'grep "Quantity decreased" %s >> %s' % (localfile, outfile)
    if argv['sku']:
      cmd = 'grep "Quantity decreased" %s | grep "%s" >> %s' % (localfile, argv['sku'], outfile)
      print(cmd)
    os.system(cmd)

  os.system("rm -f /nykaa/scripts/gludo_orders.csv")
  with open(outfile, 'r') as f:
    with open("/nykaa/scripts/gludo_orders.csv", 'w') as csv:
      csv.write("sku,quantity,quote_id\n")
      for line in f:
        date_search_str = start.format("MMM DD HH")
        m = re.search(date_search_str + ".*Quantity decreased for sku ([^ ]+) by ([0-9]+)\. quote_id - ([0-9]+)", line) 
        if argv['sku']:
          m = re.search(date_search_str + ".*Quantity decreased for sku (%s) by ([0-9]+)\. quote_id - ([0-9]+)" % argv['sku'], line) 

        if not m:
          continue

        sku = m.group(1)
        qty = m.group(2)
        qid = m.group(3)
        csv.write("%s,%s,%s\n" % (sku, qty, qid))

generateMagentoOrders()
generateGludoOrders()
magento_orders = readOrdersData('magento_orders.csv')
gludo_orders = readOrdersData('gludo_orders.csv')
getOrderMismatches(magento_orders, gludo_orders)

print("\nTime range: %s to %s "% (time_from, time_to))
