#!/usr/bin/python
from solrargparser import core
import os 
cmd = "/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:9983 -cmd upconfig -confdir /home/ubuntu/nykaa_solrconf/{core} -confname {core}".format(core=core)
print(cmd)
os.system(cmd)
