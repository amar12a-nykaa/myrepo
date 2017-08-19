#!/usr/bin/python
import os 
from solrhost import get_solr_host
from solrargparser import read_collection_as_first_arg

collection = read_collection_as_first_arg()
cmd = 'curl "http://' + get_solr_host() + '/solr/admin/collections?action=RELOAD&name={collection}"'.format(collection=collection)
print(cmd)
os.system(cmd)
