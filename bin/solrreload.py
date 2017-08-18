#!/usr/bin/python
from solrargparser import core
import os 
cmd = 'curl "http://localhost:8983/solr/admin/collections?action=RELOAD&name={core}"'.format(core=core)
print(cmd)
os.system(cmd)
