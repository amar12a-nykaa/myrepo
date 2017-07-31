import sys
import os
import argparse
import socket

hostname socket.gethostname()



parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--export", "Exports databases from dev machine and ships to qa")
group.add_argument("--import", "Imports databases locally. This must be QA or admin machine.") 
argv = vars(parser.parse_args())

if argv['export']:
  assert 'nykaadev' in hostname, "Export allowed only from dev"
  cmds = [ "/tmp/mongodump_search",
          "mongodump --db search -o /tmp/mongodump_search",
          "zip /tmp/mongodump_search.zip -r /tmp/mongodump_search",
          "scp -i ~/.ssh/gludonykaa /tmp/mongodump_search.zip ubuntu@52.221.205.33:/home/ubuntu/mongodump_search.zip",
        ]

  for cmd in cmds:
    print("cmd")
    os.system(cmd)
    

  tables = ["brands", ""]
  for table in tables:
    os.system("mysqldump -u editor -pk8J@su71$ nykaa %s > /tmp/autocomplete.sql" % " ".join(tables)) 

if argv['import']:
  pass 





