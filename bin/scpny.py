#!/usr/bin/python
import os 
import sys
import re

sys.path.append("/nykaa/scripts/")
from machines import machines

#print(machines)
argv = sys.argv[1:]
#print(argv)
cmd = "scp -i ~/.ssh/id_rsa " + " ".join(argv)
for alias, ip in machines.items():
  cmd = re.sub("@" + alias, "@"+ip, cmd)

print(" === %s ===" % cmd)
os.system(cmd)

