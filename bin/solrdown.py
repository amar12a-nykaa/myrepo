#!/usr/bin/python
from solrargparser import core
import os 
cmd = "/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:9983 -cmd downconfig  -confname {core} -confdir /home/ubuntu/nykaa_solf/{core}".format(core=core)
print(cmd)
os.system(cmd)
