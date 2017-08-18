#!/usr/bin/python
from solrargparser import core
import os 
dire = "/home/ubuntu/conf/{core}".format(core=core)
print("cd to " + dire)
os.chdir(dire)
