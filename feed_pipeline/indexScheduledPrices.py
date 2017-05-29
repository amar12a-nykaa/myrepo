#!/usr/bin/python
import sys
from dateutil import tz
from pipelineUtils import PipelineUtils
from datetime import datetime, timedelta
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils

class ScheduledPriceUpdater:

  def update():
    #Current time
    current_datetime = datetime.utcnow()
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Asia/Kolkata')
    current_datetime = current_datetime.replace(tzinfo=from_zone)
    current_datetime = current_datetime.astimezone(to_zone)

    last_datetime = current_datetime - timedelta(hours=1)
    try:
      with open("last_update_time.txt", "r+") as f:    
        content = f.read()
        if content:
          last_datetime = content
        f.seek(0)
        f.write(str(current_datetime))
        f.truncate()
    except FileNotFoundError:
      with open("last_update_time.txt", "w") as f:
        f.write(str(current_datetime))
    except Exception as e:
      print("[ERROR] %s"% str(e))
 
    # DB handler
    mysql_conn = Utils.mysqlConnection('r')
    cursor = mysql_conn.cursor()
    where_clause = " WHERE (schedule_start > %s AND schedule_start <= %s) OR (schedule_end > %s AND schedule_end <= %s)"

    query = "SELECT sku, psku, type FROM products" + where_clause
    results = Utils.fetchResults(mysql_conn, query, (last_datetime, current_datetime, last_datetime, current_datetime))    
    product_count = 0
    for product in results:
      if product['type'] == 'simple':
        product_count += PipelineUtils.updateCatalog(product['sku'], product['psku'], product['type'])


    query = "SELECT sku FROM bundles" + where_clause
    results = Utils.fetchResults(mysql_conn, query, (last_datetime, current_datetime, last_datetime, current_datetime))
    bundle_count = 0
    for bundle in results:
      bundle_count += PipelineUtils.updateCatalog(bundle['sku'], None, 'bundle')

    print("[%s] Total %s simple and %s bundle products updated."%(current_datetime, product_count, bundle_count))
    cursor.close()
    mysql_conn.close()

if __name__ == "__main__":
  ScheduledPriceUpdater.update()
