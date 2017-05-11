#!/usr/bin/python
import sys
import traceback
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils

# DB handlers for nykaa and pws DBs
nykaa_mysql_conn = Utils.nykaaMysqlConnection()
pws_mysql_conn = Utils.mysqlConnection('w')
pws_cursor = pws_mysql_conn.cursor()

# Import attributes
query = "SELECT option_id, value FROM eav_attribute_option_value GROUP BY option_id"
results = Utils.fetchResults(nykaa_mysql_conn, query)

count = 0
for option in results:
  option_id = option['option_id']
  name = option['value']

  if name and name.strip():
    color_codes = []
    color_query = "SELECT * FROM colorfamily_codes WHERE color_id=%s"
    color_results = Utils.fetchResults(nykaa_mysql_conn, color_query, (option_id,))
    if color_results:
      color_codes = color_results[0]['color_code'].split(',') if color_results[0]['color_code'].strip() else []

    # Write fetched data to PWS DB
    try:
      query = "INSERT INTO filter_attributes (id, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name=VALUES(name)"
      pws_cursor.execute(query, (option_id, name))
      if color_codes:
        rows = []
        for code in color_codes:
          rows.append((option_id, code.strip()))
        query = "INSERT INTO attribute_values (attribute_id, value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE value=VALUES(value)"
        pws_cursor.executemany(query, rows)
      
      pws_mysql_conn.commit() 
      count += 1
    except Exception as e:
      print("[Attribute Import ERROR]problem with %s: %s"%(option_id, str(e)))
print("==== Inserted %s attributes ===="%count)

#Import Brand-Category level info like app_sorting, featured_products
query = "SELECT cat_id, app_sorting, custom_sort FROM category_information"
results = Utils.fetchResults(nykaa_mysql_conn, query)
count = 0
for item in results:
  try:
    # Write to PWS DB
    query = """INSERT INTO brand_category_info (id, sorting, featured_products) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE 
               sorting=VALUES(sorting), featured_products=VALUES(featured_products)"""    
    pws_cursor.execute(query, (item['cat_id'], item['app_sorting'], item['custom_sort']))
    pws_mysql_conn.commit()
    count += 1
  except Exception as e:
    print("[Brand-Category Info Import ERROR]problem with %s: %s"%(item['cat_id'], str(e)))
print("==== Inserted %s brand-category items ===="%count)

pws_cursor.close()
pws_mysql_conn.close()
