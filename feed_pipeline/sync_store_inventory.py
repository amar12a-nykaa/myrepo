import os
import socket
import time
import sys
import timeit
import argparse
import traceback
import subprocess
from ftplib import FTP
import csv
sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils 

hostname = socket.gethostname()
my_name = os.path.basename(__file__)
FTP_HOST = "ftp.nyk00-int.network"
FTP_USER = "ftpadmin"
FTP_PASSWD = "Ftp$5&334DgkLm"
FTP_LOCATION = "/store_env"


def get_process_count():
    num = int(subprocess.check_output("ps xao ppid,pid,pgid,sid,comm -o args |  grep python | grep %s| grep -vE 'vim|grep' |  awk '{ print $4 }' | sort -n  | uniq | wc -l " % my_name, shell=True).strip())
    return num


if get_process_count() > 1:
    print("=" * 20)
    print("This script is already running. Exiting without doing anything")
    print("This means that your intended changes might still be in progress!!!")
    raise Exception("Pipeline is already running. Exiting without doing anything")


def _save_data_into_db(store_cursor, product_id_sku_dict, data_chunk):
    for row in data_chunk:
        product_id = product_id_sku_dict.get(row['SKUCODE'])
        store_query = "INSERT INTO nykaa_retail_store_inventory_data (sku, product_id, store_code, inventory, store_update_time) VALUES(%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE inventory = VALUES(inventory), store_update_time=VALUES(store_update_time);"
        store_cursor.execute(store_query, (row['SKUCODE'], product_id, row['LocCode'], row['SOH'], row['LastUpdatedOn']))

def save_data_into_db(data):
    RECORD_GROUP_SIZE = 100

    store_mysql_conn = PasUtils.storeMysqlConnection(mode='w')
    store_cursor = store_mysql_conn.cursor()

    mysql_conn = PasUtils.mysqlConnection(mode='r')
    cursor = mysql_conn.cursor()

    for i in range(0, len(data), RECORD_GROUP_SIZE):
        chunk = data[i:i + RECORD_GROUP_SIZE]
        skus = []
        for row in chunk:
            skus.append(row['SKUCODE'])
        skus = tuple(skus)
        if len(skus) == 1:
            skus = '(' + skus[0] + ')'
        query = "SELECT sku,product_id FROM products WHERE sku in {}".format(skus)
        cursor.execute(query)
        product_id_sku_results = cursor.fetchall()
        product_id_sku_dict = dict(product_id_sku_results)
        _save_data_into_db(store_cursor, product_id_sku_dict, data[i:i + RECORD_GROUP_SIZE])
        store_mysql_conn.commit()

    store_cursor.close()
    store_mysql_conn.commit()
    cursor.close()
    mysql_conn.close()
    store_mysql_conn.close()

def download_file_from_ftp(sync_type):
    status_flag = False
    if sync_type == 'hour':
        filename = 'hourly_store_inventory_' + time.strftime("%Y%m%d%H") + '.csv'
    else:
        filename = 'full_store_inventory_' + time.strftime("%Y%m%d") + '.csv'
    try:
        ftp = FTP()
        ftp.connect(host='ftp.nyk00-int.network')
        ftp.login(user='ftpadmin', passwd='Ftp$5&334DgkLm')
        ftp.cwd('/store_inv')
        file_list = ftp.nlst()
        if filename in file_list:
            with open(filename, 'wb') as f:
                ftp.retrbinary('RETR ' + filename, f.write)
            status_flag = True
    except:
        print("FTP error occured")
        print(traceback.format_exc())
    return status_flag, filename


def read_csv_file(filename):
    rows = []
    with open(filename, 'r', encoding='utf-8-sig') as fcsv:
        reader = csv.DictReader(fcsv)
        for row in reader:
            rows.append(row)
    return rows


if __name__ == "__main__":
    script_start = timeit.default_timer()
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--sync_type", help="type of run. whether it's full sync or hourly sync", default="hour")
    argv = vars(parser.parse_args())
    sync_type = argv['sync_type']
    status, filename = download_file_from_ftp(sync_type)
    if status:
        records = read_csv_file(filename)
        if len(records) > 0:
            save_data_into_db(records)
            print("Total records processed : %s" %(len(records)))
            os.remove(filename)
    else:
        print("File with filename %s not found."%(filename))
    script_stop = timeit.default_timer()
    script_duration = script_stop - script_start
    print("Total time taken for the script to run: %s" % time.strftime("%M min %S seconds", time.gmtime(script_duration)))
