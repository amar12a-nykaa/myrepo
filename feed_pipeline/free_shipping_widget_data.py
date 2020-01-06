import sys
import json
import argparse
from datetime import datetime, timedelta
import pandas as pd

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils

sys.path.append("/home/ubuntu/nykaa_scripts/utils/")
sys.path.append("/home/ubuntu/nykaa_scripts/sharedutils/")

store_threshold_mapping = {'nykaa': 100, 'men': 20}

global back_date_90_days
purchase_count_threshold = 100

def populateFrequentProductDetails():
    global back_date_90_days
    redshift_conn = PasUtils.redshiftConnection()
    mysql_conn = PasUtils.mysqlConnection('w')
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
        """.format(back_date_90_days)
        redshift_conn = PasUtils.redshiftConnection()
        data = pd.read_sql(query, redshift_conn)
        redshift_conn.close()
        
        query = """select product_id, catalog_tag from solr_dump_4"""
        mysql_conn = PasUtils.nykaaMysqlConnection()
        catalog_data = pd.read_sql(query, mysql_conn)
        catalog_data.catalog_tag = catalog_data.catalog_tag.apply(lambda x : x.split('|') if x else [])
        mysql_conn.close()
        
        rows = []
        for store, threshold in store_threshold_mapping.items():
            store_data = catalog_data[
                pd.DataFrame(catalog_data.catalog_tag.tolist()).isin([store]).any(1)].reset_index()
            store_data = pd.merge(store_data, data, on="product_id")
            store_data = store_data[store_data.purchase_count >= threshold]
            for index, row in store_data.iterrows():
                row = dict(row)
                rows.append((str(int(row['product_id'])), row['l3_id'], row['bucket'], row['purchase_count'], store))
        
        print("doing mysql queries")
        PasUtils.mysql_write("""create table if not exists free_shipping_recommendation(product_id varchar(50),
                                category varchar(255), bucket varchar(50),bought_count int(11), store varchar(20))""")
        PasUtils.mysql_write("""create table free_shipping_recommendation_tmp select * from free_shipping_recommendation""")
        PasUtils.mysql_write("""truncate table free_shipping_recommendation""")
        truncate_table = True
        add_freeshipping_recommendations_in_mysql(PasUtils.mysqlConnection("w"), rows)
        PasUtils.mysql_write("""drop table free_shipping_recommendation_tmp""")
    except Exception as e:
        print("Not ABLE TO RETRIEVE FREQUENT PRODUCT DATA")
        print(e)
        if truncate_table:
            PasUtils.mysql_write("""truncate table free_shipping_recommendation""")
            PasUtils.mysql_write("""insert into free_shipping_recommendation select * from free_shipping_recommendation_tmp""")
            PasUtils.mysql_write("""drop table free_shipping_recommendation_tmp""")
        return

def _add_recommendations_in_mysql(cursor, rows):
    values_str = ", ".join(["(%s, %s, %s, %s, %s)" for i in range(len(rows))])
    insert_recommendations_query = """ INSERT INTO free_shipping_recommendation(product_id, category, bucket, bought_count, store)
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
