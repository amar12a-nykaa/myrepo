from pas.v2.utils import Utils as PasUtils
import traceback

class PriceUpdateLogUtils:

    @classmethod
    def logBulkChangeViaProductScheduleUpdate(cls, total_rows):
        query = "insert into sku_update_log( sku, event_type, source_updated, schedule_start,schedule_end, data) values(%s,%s,%s,%s,%s,%s)"
        try:
            cls._updateToDB(query, total_rows)
        except Exception:
            print("Logging Failed\n")
            print(traceback.format_exc())

    @classmethod
    def _updateToDB(cls, query, totalrows):
        if query:
            mysql_conn = PasUtils.priceUpdateLogConnection()
            cursor = mysql_conn.cursor()
            cursor.executemany(query, totalrows)
            mysql_conn.commit()
            cursor.close()
            mysql_conn.close()
