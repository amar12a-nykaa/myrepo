import time
import datetime
import subprocess
import os
import argparse
import sys
import arrow
import csv

from IPython import embed

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter

sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

__all__ = ['create_product_id_index']

def create_product_id_index():
  start = time.time()
  product_id_index = {}

  nykaa_mysql_conn = DiscUtils.nykaaMysqlConnection()
  query = """
    SELECT a.entity_id as product_id, a.key_id as parent_id, a.sku as sku
    FROM(
      SELECT cpe.entity_id,cpe.sku, CASE WHEN cpsl.parent_id IS NOT NULL THEN cpsl.parent_id ELSE cpe.entity_id END AS 'key_id'
      FROM nykaalive1.catalog_product_entity cpe
      LEFT JOIN nykaalive1.catalog_product_super_link cpsl 
      ON cpsl.product_id = cpe.entity_id
      WHERE cpe.sku IS NOT NULL
    )a
    JOIN 
    nykaalive1.catalog_product_entity cpe 
    ON cpe.entity_id=a.key_id
    JOIN 
    nykaalive1.`catalog_product_entity_varchar` c 
    ON c.`entity_id`=a.key_id AND c.`attribute_id`=56
    """

  delta = time.time() - start
  for p in DiscUtils.mysql_read(query, connection=nykaa_mysql_conn):
    p = {k:str(v) for k,v in p.items()}
    product_id_index[p['product_id']] = p

  print("Time taken to create product_id index: %s seconds" % round(delta))
  if len(product_id_index.keys()) < 50000:
    print("Failed to create product_id_index. Master feed might be missing.")
  return product_id_index

if __name__ == '__main__':
  product_id_index = create_product_id_index()
  embed()
   
