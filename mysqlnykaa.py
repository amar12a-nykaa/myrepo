#!/usr/bin/python
import os
import mysql.connector
import sys
sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
conn_details = {}
print(conn_details)
CD = conn_details;
print("#" * 100)
print("Creating a shell with following parameters: %s" % conn_details)
print("#" * 100)
print("")

host = MAGENTO_MYSQL = "preprod2-cataloging-32st-july18.cjmplqztt198.ap-southeast-1.rds.amazonaws.com"
user = MAGENTO_MYSQL_USER = "nykaalive"
password = MAGENTO_MYSQL_PASSWORD = "oh1ued3phi0uh8ooPh6"
db = MAGENTO_DATABASE = "nykaalive1"

cmd = "mysql -A -h {host} -u{user} -p{password} --database {db}".format(user=user, host=host, password=password, db=db)
print(cmd)
os.system(cmd)
