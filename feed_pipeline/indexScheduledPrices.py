#!/usr/bin/python
import sys
from dateutil import tz
from pipelineUtils import PipelineUtils
from datetime import datetime, timedelta
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils

def getCurrentDateTime():
  current_datetime = datetime.utcnow()
  from_zone = tz.gettz('UTC')
  to_zone = tz.gettz('Asia/Kolkata')
  current_datetime = current_datetime.replace(tzinfo=from_zone)
  current_datetime = current_datetime.astimezone(to_zone) 
  return current_datetime

class ScheduledPriceUpdater:

  def update():
    #Current time
    current_datetime = getCurrentDateTime()
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

    where_clause = " WHERE (schedule_start > %s AND schedule_start <= %s) OR (schedule_end > %s AND schedule_end <= %s)"
    product_updated_count = 0
 
    mysql_conn = Utils.mysqlConnection('r')
    query = "SELECT sku, psku, type FROM products" + where_clause
    results = Utils.fetchResults(mysql_conn, query, (last_datetime, current_datetime, last_datetime, current_datetime))    
    print("[%s] Starting simple product updates" % getCurrentDateTime())
    for product in results:
      if product['type'] == 'simple':
        product_updated_count += PipelineUtils.updateCatalog(product['sku'], product['psku'], product['type'])
        if product_updated_count % 100 == 0:
          print("[%s] Update progress: %s products updated" % (getCurrentDateTime(), product_updated_count))
    mysql_conn.close()

    mysql_conn = Utils.mysqlConnection('r')
    query = "SELECT sku FROM bundles" + where_clause
    results = Utils.fetchResults(mysql_conn, query, (last_datetime, current_datetime, last_datetime, current_datetime))
    print("[%s] Starting bundle product updates" % getCurrentDateTime())
    for bundle in results:
      product_updated_count += PipelineUtils.updateCatalog(bundle['sku'], None, 'bundle')
      if product_updated_count % 100 == 0:
        print("[%s] Update progress: %s products updated" % (getCurrentDateTime(), product_updated_count))
    mysql_conn.close()

    print("\n[%s] Total %s products updated."%(getCurrentDateTime(), product_updated_count))

if __name__ == "__main__":
  ScheduledPriceUpdater.update()
