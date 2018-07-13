raise Exception("Assumption is that this file is not being used anymore - Mayank")
import sys
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils

BATCH_SIZE = 20

def querysolr(skus):
  params = {}
  params['q'] = ' OR '.join(['(sku:'+sku.upper()+')' for sku in skus])
  params['fl'] = 'sku'

  response = Utils.makeSolrRequest(params)
  docs = response.get("docs", [])
  skus_from_solr = [doc['sku'] for doc in docs]
  for sku in skus:
    if not sku in skus_from_solr:
      with open("missing_skus.txt", "a") as f:
        f.write("%s\n"%sku)



# DB handler
mysql_conn = Utils.mysqlConnection('r')

query = "SELECT sku FROM products"
results = Utils.fetchResults(mysql_conn, query)
skus_from_db = []
for product in results:
  skus_from_db.append(product['sku']) 
  if len(skus_from_db) == BATCH_SIZE:
    querysolr(skus_from_db)
    skus_from_db = []
if skus_from_db:
  querysolr(skus_from_db)

mysql_conn.close()
mysql_conn = Utils.mysqlConnection('r')

query = "SELECT sku FROM bundles"
results = Utils.fetchResults(mysql_conn, query)
skus_from_db = []
for bundle in results:
  skus_from_db.append(bundle['sku'])
  if len(skus_from_db) == BATCH_SIZE:
    querysolr(skus_from_db)
    skus_from_db = []
if skus_from_db:
  querysolr(skus_from_db)


mysql_conn.close()

   
