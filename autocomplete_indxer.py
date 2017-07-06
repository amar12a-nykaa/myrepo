import sys
sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

import mysql.connector

res = Utils.fetchResults(Utils.mysqlConnection("r"), "Select * from brand_category_mappings limit 10")

print(res)

Utils.indexCatalog(docs, collection="autocomplete")

