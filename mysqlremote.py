#!/usr/bin/python
import argparse
import os
import mysql.connector
import sys
sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

sys.path.append("/nykaa/scripts")
from machines import machines

DUUMPFILE = "/tmp/sqldump.sql"
conn_details = {}
conn = Utils.mysqlConnection(mode='w', connection_details = conn_details)
parser = argparse.ArgumentParser()
parser.add_argument("unnamed", nargs="*")
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--login", action="store_true")
parser.add_argument("--root", action="store_true", help="When passed with --login, uses root account")
group.add_argument("--sendto")
group.add_argument("--import", action='store_true', help="Imports %s" % DUUMPFILE)
argv = vars(parser.parse_args())
print(conn_details)
CD = conn_details;
print("#" * 100)
print("Creating a shell with following parameters: %s" % conn_details)
print("#" * 100)
print("")
print(argv)
args = " ".join(argv['unnamed'])

if argv['root']:
  CD['user'] = 'root'
  CD['password'] = 'spsdb4nykaa'
if argv['login']:
  cmd = "mysql -h {host} -u{user} -p{password}".format(user=CD['user'], host=CD['host'], password=CD['password'])
  if conn_details['database']:
    cmd += " --database %s" % conn_details['database']
  print(cmd)
  os.system(cmd)
elif argv['sendto']:
  cmd = "mysqldump -h {host} -u{user} -p{password} {args} > {DUUMPFILE}".format(user=CD['user'], host=CD['host'], password=CD['password'], args=args, DUUMPFILE=DUUMPFILE) 
  print(cmd)
  os.system(cmd)
  os.system("chmod 777 %s" % DUUMPFILE)
  os.system(" sed -i  '/INFORMATION_SCHEMA.SESSION_VARIABLES/d' %s" % DUUMPFILE)
  cmd = "/usr/local/bin/scpny {DUUMPFILE} ubuntu@{sendto}:{DUUMPFILE}".format(DUUMPFILE=DUUMPFILE, sendto=argv['sendto'])
  print(cmd)
  os.system(cmd)
elif argv['import']:
  cmd = "mysql -h {host} -u{user} -p{password} {args} < {DUUMPFILE}".format(user=CD['user'], host=CD['host'], password=CD['password'], args=args, DUUMPFILE=DUUMPFILE) 
  print(cmd)
  os.system(cmd)
