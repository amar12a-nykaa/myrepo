import argparse
import sys
import os
import pytz
import datetime
from decimal import ROUND_HALF_UP, Decimal

sys.path.append('/home/apis/nykaa/')

from pas.v2.utils import Utils

from contextlib import closing


def upload_special_price_to_s3(batch_size = 1000):
  tz = pytz.timezone('Asia/Kolkata')
  local_date = datetime.datetime.now(tz=tz).strftime('%d-%m-%Y')
  file_name = 'special_price_{}.csv'.format(local_date)
  f = open(file_name, "w")

  query1 = "SELECT sku, sp, type FROM products where type = 'simple';"
  write_to_result_to_file(query=query1, file=f, batch_size=batchsize)
  print('----Query1-----')

  query2 = "select parent.sku, min(child.sp), 'configurable'  " \
           "from products parent " \
           "join products child " \
           "on parent.psku = child.psku " \
           "where parent.type = 'configurable' " \
           "and child.type = 'simple' " \
           "and parent.price_criteria = 'lowest_price' " \
           "and (child.is_in_stock = 1 or child.backorders = 1) " \
           "group by parent.sku;"

  write_to_result_to_file(query=query2, file=f, batch_size=batchsize)
  print('----Query2-----')

  query3 = "select parent.sku, min(child.sp), 'configurable'  " \
           "from products parent " \
           "join products child " \
           "on parent.psku = child.psku " \
           "where parent.type = 'configurable' " \
           "and child.type = 'simple' " \
           "and parent.price_criteria = 'lowest_price' " \
           "and parent.sku not in (" \
           "select parent.sku from products parent " \
           "join products child " \
           "on parent.psku = child.psku " \
           "where parent.type = 'configurable' " \
           "and child.type = 'simple' " \
           "and parent.price_criteria = 'lowest_price' " \
           "and (child.is_in_stock = 1 or child.backorders = 1))" \
           "group by parent.sku;"

  write_to_result_to_file(query=query3, file=f, batch_size=batchsize)
  print('----Query3-----')

  query4 = "select parent.sku, child.sp, 'configurable' " \
           "from (select parent.sku sku, MAX(child.discount) max_discount " \
           "from products parent " \
           "join products child " \
           "on parent.psku = child.psku " \
           "where parent.type = 'configurable' " \
           "and child.type = 'simple' " \
           "and parent.price_criteria = 'highest_discount' " \
           "and (child.is_in_stock = 1 or child.backorders = 1) " \
           "group by parent.sku ) sub " \
           "join products parent " \
           "on parent.sku = sub.sku " \
           "join products child " \
           "on parent.sku = child.psku " \
           "where child.type = 'simple' " \
           "and (child.is_in_stock = 1 or child.backorders = 1) " \
           "and child.discount = sub.max_discount " \
           "group by parent.sku;"

  write_to_result_to_file(query=query4, file=f, batch_size=batchsize)
  print('----Query4-----')

  query5 = "select parent.sku, child.sp, 'configurable' " \
           "from (select parent.sku sku, MAX(child.discount) max_discount " \
           "from products parent " \
           "join products child " \
           "on parent.psku = child.psku " \
           "where parent.type = 'configurable' " \
           "and child.type = 'simple' " \
           "and parent.price_criteria = 'highest_discount' " \
           "and parent.sku  not in ( " \
           "select distinct(parent.sku) sku " \
           "from products parent  " \
           "join products child  " \
           "on parent.psku = child.psku " \
           "where parent.type = 'configurable' " \
           "and child.type = 'simple' " \
           "and parent.price_criteria = 'highest_discount' " \
           "and (child.is_in_stock = 1 or child.backorders = 1)) " \
           "group by parent.sku ) sub " \
           "join products parent " \
           "on parent.sku = sub.sku " \
           "join products child " \
           "on parent.sku = child.psku " \
           "where child.type = 'simple' " \
           "and child.discount = sub.max_discount " \
           "group by parent.sku"

  write_to_result_to_file(query=query5, file=f, batch_size=batchsize)
  print('----Query5-----')

  query = "SELECT bundles.sku, " \
          "(100-bundles.discount)/100* SUM( products.mrp * mappings.quantity) " \
          "FROM bundles as bundles " \
          "join bundle_products_mappings mappings " \
          "on bundles.sku = mappings.bundle_sku " \
          "join products " \
          "on products.sku = mappings.product_sku " \
          "GROUP by bundles.sku;"

  connection = Utils.mysqlConnection()
  with closing(connection.cursor()) as cursor:
    cursor.execute(query)
    while True:
      results = cursor.fetchmany(batch_size)
      if not results:
        break
      for result in results:
        special_price = Decimal(result[1]).quantize(0, ROUND_HALF_UP)
        line = '"{}", "{}", "bundle"\n'.format(result[0], special_price)
        print(line)
        f.write(line)

  f.close()
  Utils.upload_file_to_s3(file_name)
  os.remove(file_name)


def write_to_result_to_file(query, file, batch_size):
  connection = Utils.mysqlConnection()
  with closing(connection.cursor()) as cursor:
    cursor.execute(query)

    while True:
      results = cursor.fetchmany(batch_size)
      if not results:
        break
      for result in results:
        line = '"{}", "{}", "{}"\n'.format(result[0], result[1], result[2])
        print(line)
        file.write(line)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-b", "--batchsize", default=1000)
  argv = vars(parser.parse_args())
  batchsize = int(argv['batchsize'])
  upload_special_price_to_s3(batchsize)
