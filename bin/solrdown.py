#!/usr/bin/python
from solrargparser import read_config_as_first_arg

import os 
config = read_config_as_first_arg()
cmd = "/opt/solr/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:9983 -cmd downconfig  -confname {config} -confdir /home/ubuntu/nykaa_solf/{config}".format(config=config)
print(cmd)
os.system(cmd)
