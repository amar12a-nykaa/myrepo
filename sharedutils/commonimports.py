import argparse
import datetime
import json
import os
import os.path
import pprint
import re
import sys
import traceback
from collections import OrderedDict
from contextlib import closing

import arrow
import IPython
import mysql.connector
import numpy
import omniture
import pandas as pd
from pymongo import MongoClient

sys.path.append("/home/apis/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils
client = MongoUtils.getClient()


