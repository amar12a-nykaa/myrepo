from pas.v2.utils import Utils as PasUtils
import traceback
import json
import time

class PriceUpdateLogUtils:

    @classmethod
    def logBulkChangeViaProductScheduleUpdate(cls, batch_Id, event_type, schedule_start, schedule_end,  total_rows):
        query_meta = """insert into sku_update_log(batch_id, event_type,schedule_start,schedule_end) values('{batch_id}','{event_type}','{schedule_start}','{schedule_end}')""".format(
            batch_id=batch_Id,
            event_type=event_type,
            schedule_start=schedule_start,
            schedule_end=schedule_end,
        )
        query_bulk = "insert into sku_update_log_bulk(batch_id, sku, data) values(%s,%s,%s)"
        try:
            cls._updateToDB(query_meta)
            cls._updateToDBBulk(query_bulk, total_rows)
        except Exception:
            print("Logging Failed\n")
            print(traceback.format_exc())

    @classmethod
    def _updateToDBBulk(cls, query, totalrows, commit=False):
        if query:
            mysql_conn = PasUtils.priceUpdateLogConnection()
            cursor = mysql_conn.cursor()
            cursor.executemany(query, totalrows)
            cursor.close()
            mysql_conn.commit()
            mysql_conn.close()

    @classmethod
    def _updateToDB(cls, query):
        if query:
            mysql_conn = PasUtils.priceUpdateLogConnection()
            cursor = mysql_conn.cursor()
            cursor.execute(query, )
            cursor.close()
            mysql_conn.commit()
            mysql_conn.close()

    @classmethod
    def logBulkPriceChange(cls,priceChangeData):
      data = ""
      try:
        for sku,priceData in priceChangeData.items():
          if 'new_price' in priceData and priceData['new_price']!=priceData['old_price']:
            logData = {
                    'timestamp': int(time.time()),
                    'event': 'price_changed',
                    'old_price': priceData['old_price'],
                    'new_price': priceData['new_price'],
                    'sku': sku,
                    'type': priceData['type']
                  }
            data+=json.dumps(logData)
            data+="\n"
        PasUtils.logEvent(data)
      except Exception:
        print("Logging Failed\n")
        print(traceback.format_exc())
