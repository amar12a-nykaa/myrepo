import argparse
import sys
import arrow
import datetime

sys.path.append("/nykaa/scripts/sharedutils")
from loopcounter import LoopCounter

sys.path.append('/nykaa/api')
from pas.v2.utils import Utils

client = MongoUtils.getClient()

def ensure_mongo_index():
  collection = 'order_data'
  existing_list = client['search'].collection_names()
  if collection not in existing_list:
    print("creating collection: ", collection)
    client['search'].create_collection(collection)
    indexname = "date_1_product_1_parent_1"
    indexdef = [('date', 1), ('product_id', 1), ('parent_id', 1)]
    print("Creating index '%s' in collection '%s' " % (indexname, collection))
    client['search'][collection].create_index(indexdef)

def valid_date(s):
  try:
    return arrow.get(s, 'YYYY-MM-DD').format('YYYY-MM-DD')
  except ValueError:
    msg = "Not a valid date: '{0}'.".format(s)
    raise argparse.ArgumentTypeError(msg)


def read_data(start_date, end_date):
  query = """select trunc(orderdetail_dt_created) as order_date, product_id,
                case when parent_product_id != '' then parent_product_id else cast(product_id as varchar) end as parent_id,
                count(distinct nykaa_orderno) as orders,
                sum(product_unitprice) as revenue,
                sum(orderdetail_qty) as units
              from fact_order_detail_new
              where orderdetail_dt_created between '%s' and '%s' and product_id is not null
              group by 1,2,3 """%(start_date, end_date)
  redshift_conn = Utils.redshiftConnection()
  cur = redshift_conn.cursor()
  cur.execute(query)
  rows = cur.fetchall()
  columns = [column[0] for column in cur.description]
  redshift_conn.close()

  ensure_mongo_index()
  order_data = client['search']['order_data']
  ctr = LoopCounter("Writing Data: ", total=len(rows))
  for row in rows:
    ctr += 1
    if ctr.should_print():
      print(ctr.summary)
    try:
      d = dict(zip(columns, row))
      d['revenue'] = float(d['revenue'])
      d['product_id'] = str(d['product_id'])
      d['orders'] = int(d['orders'])
      d['units'] = int(d['units'])
      date = datetime.datetime.combine(d['order_date'], datetime.time())
      filt = {"date": date, "product_id": d['product_id'], "parent_id": d['parent_id']}
      update = {k: v for k, v in d.items() if k in ['orders', 'revenue', 'units']}
      ret = order_data.update_one(filt, {"$set": update}, upsert=True)
    except:
      print("error occured")
      print(row)

if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  parser.add_argument("--startdate", help="startdate in YYYY-MM-DD format", type=valid_date, default=arrow.now().replace(days=-1).format('YYYY-MM-DD'))
  parser.add_argument("--enddate", help="enddate in YYYY-MM-DD format", type=valid_date, default=arrow.now().replace(days=1).format('YYYY-MM-DD'))

  argv = vars(parser.parse_args())
  print('processing for %s %s'%(argv['startdate'], argv['enddate']))
  read_data(start_date=argv['startdate'], end_date=argv['enddate'])