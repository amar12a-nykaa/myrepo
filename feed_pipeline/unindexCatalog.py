#!/usr/bin/python
import sys
import argparse
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--skus", required=True, help='comma separated skus')
argv = vars(parser.parse_args())
skus = argv['skus']

if not skus:
  raise Exception("Please send valids sku(s)")

try:
  Utils.unindexCatalog(skus.split(','))
except Exception as e:
  print(traceback.format_exc())
  raise Exception(str(e))
