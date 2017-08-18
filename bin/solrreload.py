#!/usr/bin/python
from solrargparser import read_collection_as_first_arg

import os 
collection = read_collection_as_first_arg()
cmd = 'curl "http://localhost:8983/solr/admin/collections?action=RELOAD&name={collection}"'.format(collection=collection)
print(cmd)
os.system(cmd)
