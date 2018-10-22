import argparse
import sys
import os
import pytz
import datetime

sys.path.append('/home/apis/nykaa/')

from pas.v2.utils import Utils

from contextlib import closing


def upload_special_price_to_s3(batch_size = 1000):
  tz = pytz.timezone('Asia/Kolkata')
  local_date = datetime.datetime.now(tz=tz).strftime('%d-%m-%Y')
  file_name = 'special_price_{}.csv'.format(local_date)
  f = open(file_name, "w")
  query = "SELECT sku, sp, type FROM products;"
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
        f.write(line)

  query = "SELECT bundles.sku, " \
          "round((100-bundles.discount)/100* SUM( products.mrp * mappings.quantity)) " \
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
        line = '"{}", "{}", "bundle"\n'.format(result[0], result[1])
        print(line)
        f.write(line)

  f.close()
  Utils.upload_file_to_s3(file_name)
  os.remove(file_name)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-b", "--batchsize", default=1000)
  argv = vars(parser.parse_args())
  batchsize = int(argv['batchsize'])
  upload_special_price_to_s3(batchsize)
