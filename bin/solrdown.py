#!/usr/bin/python
import os 
from solrhost import get_zk_host
from solrargparser import read_config_as_first_arg

config = read_config_as_first_arg()
cmd = "/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost " + get_zk_host() + " -cmd downconfig  -confname {config} -confdir /home/ubuntu/nykaa_solrconf/{config}".format(config=config)
print(cmd)
os.system(cmd)
