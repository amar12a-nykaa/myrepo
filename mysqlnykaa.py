#!/usr/bin/python
import os
import mysql.connector
import sys
sys.path.append("/nykaa/api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils
conn_details = {}
conn = PasUtils.nykaaMysqlConnection(connection_details=conn_details)
print(conn_details)
CD = conn_details;
print("#" * 100)
print("Creating a shell with following parameters: %s" % conn_details)
print("#" * 100)
print("")
cmd = "mysql -h {host} -u{user} -p{password}".format(user=CD['user'], host=CD['host'], password=CD['password'])
if conn_details['database']:
  cmd += " --database %s" % conn_details['database']
print(cmd)
os.system(cmd)
