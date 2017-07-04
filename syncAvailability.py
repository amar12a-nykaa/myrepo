#!/usr/bin/python
import sys
import json
import urllib.parse
import urllib.request
from tomorrow import threads

sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils
from feed_pipeline.pipelineUtils import PipelineUtils

MAX_PARALLEL_REQUESTS = 100

def construct_url(params):
  return "http://" + PipelineUtils.getAPIHost() + "/apis/v1/pas.set?"+urllib.parse.urlencode(params)

@threads(MAX_PARALLEL_REQUESTS, timeout=10)
def load_url(url):
  response = json.loads(urllib.request.urlopen(url).read().decode('utf-8'))  
  return response

# DB handlers for nykaa
nykaa_mysql_conn = Utils.nykaaMysqlConnection()
limit = "11111111"
#limit = "10003"

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
count = len(results)
req_urls = {}
for index, row in enumerate(results):
  try:
    sku = row['sku']
    product_type = row['type']
    backorders = int(row['backorders'])
    assert backorders in [0, 1], "backorders can be 0 or 1 only"
    quantity = int(row['quantity'])

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
