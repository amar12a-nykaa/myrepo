from pas.v2.utils import Utils as PasUtils
import traceback

class PriceUpdateLogUtils:

    @classmethod
    def logBulkChangeViaProductScheduleUpdate(cls, batch_Id, event_type, schedule_start, schedule_end,  total_rows):
        query_meta = """insert into sku_update_log(batch_id, event_type,schedule_start,schedule_end) values({batch_id},{event_type},{schedule_start},{schedule_end})""".format(
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
            if commit is True:
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
