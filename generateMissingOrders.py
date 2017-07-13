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


sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils


parser = argparse.ArgumentParser()
parser.add_argument('--sku')
argv = vars(parser.parse_args())
print(argv)


# Time Calculations
now = arrow.utcnow()
end = now.replace( minute=0, second=0, microsecond=0)
start = end.replace(hours=-1)
time_from = start.format('YYYY-MM-DD HH:mm:ss')
time_to = end.format('YYYY-MM-DD HH:mm:ss')

def generateMagentoOrders():
  with open('magento_orders.csv', 'w') as csvfile:
    fieldnames = ['sku', 'quantity']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    #sys.exit()
    nykaa_mysql_conn = Utils.nykaaMysqlConnection()
    query = """SELECT f.sku, ROUND(SUM(f.qty)) AS 'quantity'
             FROM(SELECT a.increment_id, a.sku, a.name, a.parent_item_id,  a.item_id, IFNULL(b.q_c,a.q_s) AS qty
             FROM(SELECT sfo.increment_id, sfoi.`sku`,sfoi.product_type, sfoi.`name`, sfoi.`mrp`, sfo.created_at, sfoi.qty_ordered AS q_s, 
                         sfoi.parent_item_id, sfoi.item_id, sfo.total_qty_ordered
                  FROM nykaalive1.sales_flat_order sfo
                  JOIN nykaalive1.`sales_flat_order_address` sfoa ON sfoa.parent_id=sfo.entity_id AND sfoa.address_type='shipping'
                  JOIN nykaalive1.sales_flat_order_payment sfop ON sfo.entity_id=sfop.parent_id 
                  JOIN nykaalive1.sales_flat_order_item sfoi ON sfoi.`order_id`=sfo.`entity_id`
                  WHERE(((sfo.`status` IN ('processing','complete','confirmed','canceled')) OR (sfo.`status` = 'pending' AND sfop.`method` LIKE '%%cash%%')))
                       AND (sfo.created_at BETWEEN '%s' AND '%s') AND sfoi.product_type='simple')a
                  LEFT JOIN 
                 (SELECT sfo.increment_id, sfoi.`sku`,sfoi.product_type, sfoi.`name`, sfoi.item_id, sfoi.qty_ordered AS q_c
                  FROM nykaalive1.sales_flat_order sfo
                  JOIN nykaalive1.sales_flat_order_payment sfop ON sfo.entity_id=sfop.parent_id 
                  JOIN nykaalive1.sales_flat_order_item sfoi ON sfoi.`order_id`=sfo.`entity_id`
                  WHERE (((sfo.`status` IN ('processing','complete','confirmed','canceled')) OR (sfo.`status` = 'pending' AND sfop.`method` LIKE '%%cash%%')))
                        AND (sfo.created_at BETWEEN '%s' AND '%s') AND sfoi.product_type='configurable'
                 )b ON a.parent_item_id=b.item_id
             )f
             GROUP BY 1
          """ % (time_from, time_to, time_from, time_to)

    if argv['sku']:
      query = "select * from (%s)A where sku = '%s'" % (query, argv['sku'])
      print(query)
    results = Utils.fetchResults(nykaa_mysql_conn, query)
    for row in results:
#      if argv['sku']:
#        print("row..")
#        print(row)
      writer.writerow({'sku': row['sku'], 'quantity': int(row['quantity'])})


def readOrdersData(filename):
  gludo_orders = {}
  with open(filename, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      sku = row['sku'].strip()
      quantity = int(row['quantity'].strip())
      if sku not in gludo_orders:
        gludo_orders[sku] = 0
      gludo_orders[sku] += quantity

  return gludo_orders


def getOrderMismatches(magento_orders, gludo_orders):
  for sku, magento_quantity in magento_orders.items():
    gludo_quantity = gludo_orders.get(sku, 0)
    if magento_quantity != gludo_quantity:
      diff = abs(magento_quantity - gludo_quantity)
      print("%s quantity is off by %d. Magento quantity: %d, Gludo quantity: %d" % (sku, diff, magento_quantity, gludo_quantity))


def generate_gludo_orders():

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
    for datestr in datestrs:
      #print("---")
      try:
        files = subprocess.check_output("ssh -i /root/.ssh/id_rsa ubuntu@%s 'ls /var/log/apache2/error.log'" % (machine,), shell=True)
        files = files.decode().split("\n")
        files = [f for f in files if f]
        #print("files: %r" % files)
        for f in files:
          basename = os.path.basename(f) 
          localpath = DIR + "/%s/%s" % (machine, basename)
          cmd = "scp -i /root/.ssh/id_rsa ubuntu@%s:%s %s" % (machine, f, localpath)
          #print(cmd)
          os.system(cmd)
          if re.search(".gz$",  localpath):
            os.system(localpath)

        #print(cmd)
        os.system(cmd)
      except Exception as e:
        print("Exception .. ")
        if 'No such file or directory' in str(e):
          pass
        else:
          print(traceback.format_exc())
        pass
      localfiles = subprocess.check_output("ls -d -1 %s/%s/*.*" % (DIR, machine), shell=True).decode().split("\n")
      for localfile in localfiles:
        if not localfile:
          continue
        cmd = 'grep "Quantity decreased" %s >> %s' % (localfile, outfile)
        if argv['sku']:
          cmd = 'grep "Quantity decreased" %s | grep "%s" >> %s' % (localfile, argv['sku'], outfile)
          print(cmd)
        os.system(cmd)

  os.system("rm -f /nykaa/scripts/gludo_orders.csv")
  with open(outfile, 'r') as f:
    with open("/nykaa/scripts/gludo_orders.csv", 'w') as csv:
      csv.write("sku,quantity\n")
      for line in f:
        date_search_str = start.format("MMM DD HH")
        m = re.search(date_search_str + ".*Quantity decreased for sku ([^ ]+) by ([0-9]+)", line) 
        if not m:
          continue

        if argv['sku']:
          print(date_search_str + ".*Quantity decreased for sku ([^ ]+) by ([0-9]+)")
          print(line)

        sku = m.group(1)
        qty = m.group(2)
        csv.write("%s,%s\n" % (sku, qty))

generateMagentoOrders()
generate_gludo_orders()
if argv['sku']:
  os.system("grep -r %s /tmp/error_logs_api_machines/" % argv['sku'])
else:
  magento_orders = readOrdersData('magento_orders.csv')
  gludo_orders = readOrdersData('gludo_orders.csv')
  getOrderMismatches(magento_orders, gludo_orders)

  print("\nTime range: %s to %s "% (time_from, time_to))
