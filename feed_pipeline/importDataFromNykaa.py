#!/usr/bin/python
import sys
import json
import traceback
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils, MemcacheUtils

class NykaaImporter:

  nykaa_mysql_conn = None
  pws_mysql_conn = None
  pws_cursor = None

  def importData():
    # DB handlers for nykaa and pws DBs
    NykaaImporter.nykaa_mysql_conn = Utils.nykaaMysqlConnection()
    NykaaImporter.pws_mysql_conn = Utils.mysqlConnection('w')
    NykaaImporter.pws_cursor = NykaaImporter.pws_mysql_conn.cursor()

    NykaaImporter.importAttributes()
    NykaaImporter.importBrandCategoryAttributes()
    NykaaImporter.importMetaInformation()

    NykaaImporter.pws_cursor.close()
    NykaaImporter.pws_mysql_conn.close()

  def importAttributes():
    # Import attributes
    query = "SELECT option_id, value FROM eav_attribute_option_value GROUP BY option_id"
    results = Utils.fetchResults(NykaaImporter.nykaa_mysql_conn, query)

    count = 0
    for option in results:
      option_id = option['option_id']
      name = option['value']

      if name and name.strip():
        color_codes = []
        color_query = "SELECT * FROM colorfamily_codes WHERE color_id=%s"
        color_results = Utils.fetchResults(NykaaImporter.nykaa_mysql_conn, color_query, (option_id,))
        if color_results:
          color_codes = color_results[0]['color_code'].split(',') if color_results[0]['color_code'].strip() else []

        # Write fetched data to PWS DB
        try:
          option_attrs = {'id': option_id, 'name': name}   

          query = "INSERT INTO filter_attributes (id, name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name=VALUES(name)"
          NykaaImporter.pws_cursor.execute(query, (option_id, name))
          if color_codes:
            option_attrs['color_code'] = color_codes
            rows = []
            for code in color_codes:
              rows.append((option_id, code.strip()))
            query = "INSERT INTO attribute_values (attribute_id, value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE value=VALUES(value)"
            NykaaImporter.pws_cursor.executemany(query, rows)
          
          NykaaImporter.pws_mysql_conn.commit() 
          count += 1

          # Update in memcache
          memcache_key = 'option-%s' % option_id
          jsonstr = json.dumps(option_attrs)
          MemcacheUtils.set(memcache_key, jsonstr)
        except Exception as e:
          print("[Attribute Import ERROR]problem with %s: %s"%(option_id, str(e)))
    print("==== Imported %s attributes ===="%count)

  def importBrandCategoryAttributes():
    #Import Brand-Category level info like app_sorting, featured_products
    default_sorting_map = {'1': 'popularity', '2': 'discount', '3': 'name', '4': 'price_asc', '5': 'price_desc', '6': 'customer_top_rated', '7': 'new_arrival' }
    query = """SELECT DISTINCT(cce.entity_id) AS category_id, ci.app_sorting, ci.custom_sort,
             (cce.level-1) AS level, (CASE WHEN nkb.brand_id > 0 THEN 'brand' ELSE 'category' END) AS type
             FROM `catalog_category_entity` AS cce
             LEFT JOIN `category_information` AS ci ON ci.cat_id = cce.entity_id
             LEFT JOIN nk_brands AS nkb ON nkb.brand_id = cce.entity_id;"""
    results = Utils.fetchResults(NykaaImporter.nykaa_mysql_conn, query)
    count = 0
    for item in results:
      try:
        # Write to PWS DB
        query = """INSERT INTO brand_category_information (id, sorting, featured_products, level, type) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 
                   sorting=VALUES(sorting), featured_products=VALUES(featured_products), level=VALUES(level), type=VALUES(type)"""    
        NykaaImporter.pws_cursor.execute(query, (item['category_id'], item['app_sorting'], item['custom_sort'], item['level'], item['type']))
        NykaaImporter.pws_mysql_conn.commit()
        count += 1

        #Update in memcache
        cat_info = {}
        memcache_key = 'brand-category-%s' % item['category_id']
        app_sorting = item['app_sorting']
        if not (app_sorting and app_sorting in default_sorting_map.keys()):
          app_sorting = '1'
        else:
          app_sorting = str(app_sorting)
        cat_info['sort'] = default_sorting_map[app_sorting]
        cat_info['featured_products'] = item['custom_sort'].split(',') if item['custom_sort'] else []
        cat_info['level'] = item['level']
        cat_info['type'] = item['type']
        MemcacheUtils.set(memcache_key, json.dumps(cat_info))    
      except Exception as e:
        print("[Brand-Category Info Import ERROR]problem with %s: %s"%(item['category_id'], str(e)))
    print("==== Imported %s brand-category items ===="%count)

  def importMetaInformation():
    # Import category meta information
    query = """SELECT e.entity_id AS category_id, CONCAT(ccevt.value, ' | Nykaa') AS meta_title, REPLACE(REPLACE(ccetk.value, '\r', ''), '\n', '') AS meta_keywords, 
               REPLACE(REPLACE(ccetd.value, '\r', ''), '\n', '') AS meta_description FROM catalog_category_entity e
               LEFT JOIN catalog_category_entity_varchar ccevt ON ccevt.entity_id = e.entity_id AND ccevt.attribute_id = 36
               LEFT JOIN catalog_category_entity_text ccetk ON ccetk.entity_id = e.entity_id AND ccetk.attribute_id = 37
               LEFT JOIN catalog_category_entity_text ccetd ON ccetd.entity_id = e.entity_id AND ccetd.attribute_id = 38
               WHERE ccevt.value IS NOT NULL OR ccetk.value IS NOT NULL OR ccetd.value IS NOT NULL;"""    
    results = Utils.fetchResults(NykaaImporter.nykaa_mysql_conn, query)
    count = 0
    for result in results:
      try:
        query = "INSERT INTO categories_meta (category_id, meta_title, meta_description, meta_keywords) VALUES (%s, %s, %s, %s) "
        query += "ON DUPLICATE KEY UPDATE meta_title=%s, meta_description=%s, meta_keywords=%s"

        values = (result['category_id'], result['meta_title'], result['meta_description'], result['meta_keywords'], result['meta_title'], result['meta_description'], result['meta_keywords'])
        NykaaImporter.pws_cursor.execute(query, values)
        NykaaImporter.pws_mysql_conn.commit()
        count += 1
      except Exception as e:
        print("[Category Meta Import ERROR]problem with %s: %s"%(result['category_id'], str(e)))
    print("==== Imported meta information of %s categories ===="%count) 
    

if __name__ == "__main__":
  NykaaImporter.importData()

