#!/usr/bin/python
import sys
import json
import urllib.parse
import urllib.request
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils
from feed_pipeline.pipelineUtils import PipelineUtils

# DB handlers for nykaa
nykaa_mysql_conn = Utils.nykaaMysqlConnection()
limit = "11111111"
#limit = "10000"

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

for row in results:
  try:
    sku = row['sku']
    product_type = row['type']

    backorders = int(row['backorders'])
    assert backorders in [0, 1], "backorders can be 0 or 1 only"

    #is_in_stock = int(row['is_in_stock'])
    #assert is_in_stock in [0, 1], "is_in_stock can be 0 or 1 only"

    quantity = int(row['quantity'])

    params = {'sku': sku, 'type': product_type, 'backorders': backorders, 'quantity': quantity}
    response = json.loads(urllib.request.urlopen("http://" + PipelineUtils.getAPIHost() + "/apis/v1/pas.set?"+urllib.parse.urlencode(params)).read().decode('utf-8'))  
    response_status = response.get('status') 
    if response_status.lower()!='ok':
      print("[UPDATE ERROR] sku: %s; reason: %s"%(sku, response.get('message'))) 
    #else:
    #  print("All okay sku: %s"%sku)

  except Exception as e:
    print("[ERROR] row: %s, error: %s"% (row, str(e)))
