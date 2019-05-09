import argparse
import sys
import os
import pytz
import datetime
import requests
import json
import time

sys.path.append('/home/apis/pds_api/')

from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils
from pas.v2.utils import hostname

from contextlib import closing


def get_gludo_url():

    if hostname.startswith('api') or hostname.startswith('admin'):
        gludo_base_url = 'http://priceapi.nyk00-int.network/apis/v2'

    elif hostname.startswith('preprod') or hostname.startswith('dl'):
        gludo_base_url = 'http://preprod-api.nyk00-int.network/apis/v2'

    elif hostname.startswith('qa'):
        gludo_base_url = 'http://qa-api.nyk00-int.network/apis/v2'

    else:
        gludo_base_url = 'http://qa-api.nyk00-int.network/apis/v2'

    return gludo_base_url + '/pas.get'


def upload_special_price_to_s3(batch_size = 1000):
  tz = pytz.timezone('Asia/Kolkata')
  local_date = datetime.datetime.now(tz=tz).strftime('%d-%m-%Y')
  file_name = 'special_price_{}.csv'.format(local_date)
  f = open(file_name, "w")

  query1 = "SELECT sku, type FROM products;"
  write_to_result_to_file(query=query1, file=f, batch_size=batchsize)
  print('----Query1-----')

  query2 = "SELECT sku, 'bundle' FROM nykaa.bundles;"
  write_to_result_to_file(query=query2, file=f, batch_size=batchsize)
  print('----Query2-----')

  f.close()
  Utils.upload_file_to_s3(file_name)
  os.remove(file_name)



def write_to_result_to_file(query, file, batch_size):
  gludo_url = get_gludo_url()
  print('gludo_url', gludo_url)
  connection = Utils.mysqlConnection()
  with closing(connection.cursor()) as cursor:
    cursor.execute(query)
    products_array = []
    while True:
      results = cursor.fetchmany(batch_size)
      if not results:
        break
      products = []
      for result in results:
        products.append({
          "sku": result[0],
          "type": result[1]
        })
      products_array.append(products)

    for products in products_array:
      request_data = {"products": products}

      for attempt in range(1, 4):
        try:
          response = requests.post(url=gludo_url, json=request_data, headers={'Content-Type': 'application/json'})
          if (response.ok):
            response_data = json.loads(response.text)
            skus = response_data['skus']
            for sku in skus:
              line = '"{}", "{}", "{}", "{}", "{}", "{}"\n'.format(sku, skus[sku]['sp'], skus[sku]['type'], skus[sku]['disabled'], skus[sku]['mrp'], skus[sku]['is_in_stock'])
              print(line)
              file.write(line)
            break
        except Exception as e:
          print(e)

        if attempt == 4:
          print('request_data', request_data)
          sys.exit(10)
        time.sleep(5*attempt)


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-b", "--batchsize", default=1000)
  argv = vars(parser.parse_args())
  batchsize = int(argv['batchsize'])
  upload_special_price_to_s3(batchsize)
