#!/usr/bin/python
import argparse
import subprocess
from importDataFromNykaa import NykaaImporter
from indexCatalog import CatalogIndexer

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filepath", required=True, help='path to csv file')
parser.add_argument("-i", "--importattrs", action='store_true', help='Flag to import attributes first')
argv = vars(parser.parse_args())

file_path = argv['filepath']
import_attrs = argv.get('importattrs', False)

if import_attrs:
  print("Importing attributes from Nykaa DB....")
  NykaaImporter.importAttrs()

print("\n\nIndexing documents from csv file in: %s"%file_path)
CatalogIndexer.index(file_path)


#print("\n\nUpdating Popularity values")
#cmd = ['python3', 'popularity.py', '--post-to-solr', '-n0', '-y']
#p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
#for line in p.stdout:
#  print(line)
#p.wait()

print("\n\nFinished running catalog pipeline")
