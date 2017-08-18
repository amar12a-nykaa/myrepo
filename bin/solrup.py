#!/usr/bin/python
from solrargparser import read_config_as_first_arg
import os 


config = read_config_as_first_arg()
cmd = "/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:9983 -cmd upconfig -confdir /home/ubuntu/nykaa_solrconf/{config} -confname {config}".format(config=config)
print(cmd)
os.system(cmd)
