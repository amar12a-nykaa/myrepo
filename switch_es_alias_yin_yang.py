import argparse
import sys
from IPython import embed
from pprint import pprint
sys.path.append("/nykaa/scripts/sharedutils")

from esutils import EsUtils


parser = argparse.ArgumentParser()
parser.add_argument("--switch", action="store_true", help="This option will attempt to make an alias switch")
parser.add_argument("--force", action="store_true", help="This option will switch even if inactive index in not healthy.")
argv = vars(parser.parse_args())

indices = EsUtils.get_active_inactive_indexes('livecore')
active_index = indices['active_index']
inactive_index = indices['inactive_index']

conn = EsUtils.get_connection()
ic = EsUtils.get_index_client()    
active_count = ic.stats()['indices'][active_index]['total']['docs']['count']
inactive_count = ic.stats()['indices'][inactive_index]['total']['docs']['count']
#print(active_count, inactive_count)
print("Active Index   - %5s  - %6s documents" % (active_index, active_count))
print("Inactive Index - %5s  - %6s documents" % (inactive_index, inactive_count))

all_good = True
switch_now = False
if inactive_count >= active_count and inactive_count > 200000:
  print("Inactive Index seems to be healthy. It has %s documents." % inactive_count)
  if not argv['switch']:
    print("Pass --switch option switch the Alias")
  else:
    switch_now = True
else:
  print("Inactive Index is NOT  healthy ")
  if not argv['force']:
    print("Pass --force option to switch anyways.")
  else:
    switch_now = True

if switch_now and argv['switch']:
  EsUtils.switch_index_alias('livecore', active_index, inactive_index)
  print("New Active Index: %s"% EsUtils.get_active_inactive_indexes('livecore')['active_index'])

