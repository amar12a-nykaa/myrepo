import sys
import json
import argparse
from datetime import datetime, timedelta

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/home/ubuntu/nykaa_scripts/utils/")
sys.path.append("/home/ubuntu/nykaa_scripts/sharedutils/")


global back_date_90_days
purchase_count_threshold = 100

def populateFrequentProductDetails():
    global back_date_90_days
    redshift_conn = Utils.redshiftConnection()
    mysql_conn = Utils.mysqlConnection('w')
    truncate_table = False
    try:
        query = """select s.product_id, s.l3_id, 
                    case when s.mrp <= 50  then 'BELOW 50'
                         when s.mrp <= 100 then 'BELOW 100'
                         when s.mrp <= 150 then 'BELOW 150'
                         when s.mrp <= 200 then 'BELOW 200'
                    end as bucket,
                    count(*) as purchase_count
                    from fact_order_detail_new fodn join
                    dim_sku s on s.sku = fodn.product_sku
                    where fodn.orderdetail_dt_created > '{0}'
                    and s.mrp > 1 and s.mrp < 251
                    group by s.product_id, l3_id, bucket
                    having purchase_count > {1}
        """.format(back_date_90_days, purchase_count_threshold)
        cursor = redshift_conn.cursor()
        cursor.execute(query)
        print("connection fetched")
        rows = []
        for row in cursor.fetchall():
            rows.append((str(row[0]), row[1], row[2], row[3]))
        print("doing mysql queries")
        Utils.mysql_write("""create table if not exists free_shipping_recommendation(product_id varchar(50),
                                category varchar(255), bucket varchar(50),bought_count int(11))""")
        Utils.mysql_write("""create table free_shipping_recommendation_tmp select * from free_shipping_recommendation""")
        Utils.mysql_write("""truncate table free_shipping_recommendation""")
        truncate_table = True
        add_freeshipping_recommendations_in_mysql(mysql_conn, rows)
        Utils.mysql_write("""drop table free_shipping_recommendation_tmp""")
    except Exception as e:
        print("Not ABLE TO RETRIEVE FREQUENT PRODUCT DATA")
        print(e)
        if truncate_table:
            Utils.mysql_write("""truncate table free_shipping_recommendation""")
            Utils.mysql_write("""insert into free_shipping_recommendation select * from free_shipping_recommendation_tmp""")
            Utils.mysql_write("""drop table free_shipping_recommendation_tmp""")
        return

def _add_recommendations_in_mysql(cursor, rows):
    values_str = ", ".join(["(%s, %s, %s, %s)" for i in range(len(rows))])
    insert_recommendations_query = """ INSERT INTO free_shipping_recommendation(product_id, category, bucket, bought_count)
        VALUES %s
    """ % (values_str)
    values = tuple([str(_i) for row in rows for _i in row])
    cursor.execute(insert_recommendations_query, values)

def add_freeshipping_recommendations_in_mysql(db,rows):
    cursor = db.cursor()
    for i in range(0, len(rows), 100):
        _add_recommendations_in_mysql(cursor, rows[i:i+100])
        db.commit()


if __name__ == '__main__':
    back_date_90_days = str((datetime.now() - timedelta(days=90)).date())
    populateFrequentProductDetails()
