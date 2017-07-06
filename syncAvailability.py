#!/usr/bin/python
import sys
import json
import argparse
import traceback
import urllib.parse
import urllib.request
from tomorrow import threads
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils
from pas.v1.csvutils import read_csv_from_file
from feed_pipeline.pipelineUtils import PipelineUtils

MAX_PARALLEL_REQUESTS = 20

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--filepath", help='path to csv file containing skus for which availability needs to be updated')
parser.add_argument("--db", action='store_true', help="Sync availability using Nykaa's DB. This will sync ALL products.")
argv = vars(parser.parse_args())

file_path = argv['filepath']
sync_db = argv['db']

if not (file_path or sync_db):
  msg = "Either of filepath[-p] or --db option needs to specified!"
  print(msg)
  raise Exception(msg)
elif (file_path and sync_db):
  msg = "Please provide only one of filepath[-p] or --db option."
  print(msg)
  raise Exception(msg)

def construct_url(params):
  return "http://" + PipelineUtils.getAPIHost() + "/apis/v1/pas.set?"+urllib.parse.urlencode(params)

@threads(MAX_PARALLEL_REQUESTS, timeout=10)
def load_url(url):
  response = json.loads(urllib.request.urlopen(url).read().decode('utf-8'))  
  return response

results = []

if sync_db:
  # Sync availability info by picking up from Nykaa DB
  nykaa_mysql_conn = Utils.nykaaMysqlConnection()
  limit = "11111111"
  #limit = "100"
  query = """SELECT * FROM
             (SELECT cpep.sku AS parent_sku, cps.parent_id AS parent_id, cpe.sku, cpe.type_id AS type, IF(csi.use_config_backorders=0 AND csi.backorders=1, '1','0') 
             AS backorders, csi.qty AS quantity, csi.is_in_stock
             FROM nykaalive1.catalog_product_entity cpe
             LEFT JOIN nykaalive1.cataloginventory_stock_item csi ON csi.product_id=cpe.entity_id
             LEFT JOIN nykaalive1.catalog_product_super_link cps ON cps.product_id=cpe.entity_id
             LEFT JOIN nykaalive1.catalog_product_entity cpep ON cpep.entity_id=cps.parent_id
             WHERE (cpe.type_id='simple') AND cpe.sku IS NOT NULL GROUP BY 1,3)a
             WHERE (a.parent_id IS NOT NULL AND a.parent_sku IS NOT NULL) OR (a.parent_id IS NULL AND a.parent_sku IS NULL)
             LIMIT %s;"""%limit
  results = Utils.fetchResults(nykaa_mysql_conn, query)
elif file_path:
  results = read_csv_from_file(file_path)
   

count = len(results)
req_urls = {}
for index, row in enumerate(results):
  try:
    sku = row['sku']
    product_type = row['type']
    backorders = int(row.get('backorders') or row.get('magento backorders'))
    assert backorders in [0, 1], "backorders can be 0 or 1 only"
    quantity = row.get('quantity') if row.get('quantity') is not None else row.get('magento quantity')
    quantity = int(quantity)

    if quantity < 0 and not backorders:
      quantity = 0

    params = {'sku': sku, 'type': product_type, 'backorders': backorders, 'quantity': quantity}
    req_urls[sku] = construct_url(params)   

    if len(req_urls.items())==MAX_PARALLEL_REQUESTS or index+1>=count:
      req_skus =  []
      urls = []
      for sku, url in req_urls.items():
        req_skus.append(sku)
        urls.append(url)

      responses = [load_url(url) for url in urls]
      for i, response in enumerate(responses):        
        response_status = response.get('status') 
        if response_status.lower()!='ok':
          print("[UPDATE ERROR] sku: %s; reason: %s"%(req_skus[i], response.get('message'))) 
      req_urls = {}
      req_skus = []
      urls = []

  except Exception as e:
    print("[ERROR] row: %s, error: %s"% (row, str(e)))
    print(traceback.format_exc())

print("Finished syncing availability info!")
