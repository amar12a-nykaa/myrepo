#!/usr/bin/python
import sys
import json
import traceback
from IPython import embed
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils, MemcacheUtils

class NykaaImporter:

  pws_mysql_conn = None
  pws_cursor = None

  default_sorting_map = {'1': 'popularity', '2': 'discount', '3': 'name', '4': 'price_asc', '5': 'price_desc', '6': 'customer_top_rated', '7': 'new_arrival' }

  def importData():
    # DB handlers for nykaa and pws DBs
    NykaaImporter.pws_mysql_conn = Utils.mysqlConnection('w')
    NykaaImporter.pws_cursor = NykaaImporter.pws_mysql_conn.cursor()

    NykaaImporter.importAttributes()
    NykaaImporter.importBrandCategoryAttributes()
    NykaaImporter.importOfferAttributes()
    NykaaImporter.importMetaInformation()

    NykaaImporter.pws_cursor.close()
    NykaaImporter.pws_mysql_conn.close()

  def importAttributes():
    # Import attributes
    nykaa_mysql_conn = Utils.nykaaMysqlConnection()
    query = "SELECT option_id, value FROM eav_attribute_option_value GROUP BY option_id"
    results = Utils.fetchResults(nykaa_mysql_conn, query)

    count = 0
    for option in results:
      option_id = str(option['option_id'])
      name = option['value']

      if name and name.strip():
        color_codes = []
        color_query = "SELECT * FROM colorfamily_codes WHERE color_id=%s"
        color_results = Utils.fetchResults(nykaa_mysql_conn, color_query, (option_id,))
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
    nykaa_mysql_conn = Utils.nykaaMysqlConnection()
    query = """SELECT DISTINCT(cce.entity_id) AS category_id, cur.request_path AS category_url, 
            ci.app_sorting, ci.custom_sort, ci.art_banner_image, ci.art_banner_video, ci.art_banner_video_image, 
            ci.font_color, ci.art_title, ci.art_content, ci.art_url, ci.art_link_text, ci.categories, ci.art_position,
            ci.android_landing_url, ci.ios_landing_url, ci.tip_tile, 
            (cce.level-2) AS level, (CASE WHEN nkb.brand_id > 0 THEN 'brand' ELSE 'category' END) AS type 
            FROM `catalog_category_entity` AS cce
            LEFT JOIN `category_information` AS ci ON ci.cat_id = cce.entity_id
            LEFT JOIN `core_url_rewrite` AS cur ON cur.category_id = cce.entity_id
            LEFT JOIN nk_brands AS nkb ON nkb.brand_id = cce.entity_id
            WHERE cur.store_id = 0 AND cur.product_id IS NULL
            GROUP BY category_id;"""
    results = Utils.fetchResults(nykaa_mysql_conn, query)
    count = 0
    for item in results:
      try:
        # Write to PWS DB
        field_list = ['id', 'sorting', 'featured_products', 'level', 'type', 'url', 'banner_image', 'banner_video', 'banner_video_image', 
        'font_color', 'art_title', 'art_content', 'art_url', 'art_link_text', 'child_categories', 'art_pos', 'android_landing_url', 'ios_landing_url', 'tip_tile'
]

        fields = ''
        values = ''
        on_duplicate_values = ''
        for index, field in enumerate(field_list):
           fields += field
           values += '%s'
           on_duplicate_values += "%s=VALUES(%s)" % (field, field)
           if index < len(field_list)-1:
             fields += ', '
             values += ', '
             on_duplicate_values += ', '
          
        query = "INSERT INTO brand_category_information (" + fields + ") VALUES (" + values + ") ON DUPLICATE KEY UPDATE " + on_duplicate_values
        NykaaImporter.pws_cursor.execute(query, (item['category_id'], item['app_sorting'], item['custom_sort'], item['level'], item['type'], item['category_url'],
                                         item['art_banner_image'], item['art_banner_video'], item['art_banner_video_image'], item['font_color'],
                                         item['art_title'], item['art_content'], item['art_url'], item['art_link_text'], item['categories'], item['art_position'],
                                         item['android_landing_url'], item['ios_landing_url'], item['tip_tile']))
        NykaaImporter.pws_mysql_conn.commit()
        count += 1

        #Update in memcache
        cat_info = {}
        memcache_key = 'brand-category-%s' % item['category_id']
        app_sorting = item['app_sorting']
        if not (app_sorting and app_sorting in NykaaImporter.default_sorting_map.keys()):
          app_sorting = '1'
        else:
          app_sorting = str(app_sorting)
        cat_info['sort'] = NykaaImporter.default_sorting_map[app_sorting]
        cat_info['featured_products'] = item['custom_sort'].split(',') if item['custom_sort'] else []
        cat_info['level'] = item['level']
        cat_info['type'] = item['type']
        cat_info['url'] = item['category_url']
        cat_info['banner_image'] = item['art_banner_image']
        cat_info['banner_video'] = item['art_banner_video']
        cat_info['banner_video_image'] = item['art_banner_video_image']
        cat_info['font_color'] = item['font_color']
        cat_info['art_title'] = item['art_title']
        cat_info['art_content'] = item['art_content']
        cat_info['art_url'] = item['art_url']
        cat_info['art_link_text'] = item['art_link_text']
        cat_info['child_categories'] = item['categories']
        cat_info['art_pos'] = item['art_position']
        MemcacheUtils.set(memcache_key, json.dumps(cat_info), update_prod_memcache=True)    
      except Exception as e:
        print("[Brand-Category Info Import ERROR]problem with %s: %s"%(item['category_id'], str(e)))
    print("==== Imported %s brand-category items ===="%count)

  def importOfferAttributes():
    # Import offer attributes: featured_products and app_sorting
    nykaa_mysql_conn = Utils.nykaaMysqlConnection()
    query = "SELECT entity_id AS offer_id, name, app_sorting, custom_sort, filter_params, filter_values FROM `nykaa_offers`"
    results = Utils.fetchResults(nykaa_mysql_conn, query)
    count = 0
    for item in results:
      try:
        # Write to PWS DB
        query = """INSERT INTO offer_information (id, name, sorting, featured_products, filter_params, filter_values) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 
                   name=VALUES(name), sorting=VALUES(sorting), featured_products=VALUES(featured_products), filter_params=VALUES(filter_params), filter_values=VALUES(filter_values)"""
        NykaaImporter.pws_cursor.execute(query, (item['offer_id'], item['name'], item['app_sorting'], item['custom_sort'], item['filter_params'], item['filter_values']))
        NykaaImporter.pws_mysql_conn.commit()
        count += 1

        #Update in memcache
        offer_info = {}
        memcache_key = 'offer-v2-%s' % item['offer_id']
        app_sorting = str(item['app_sorting'])
        if not (app_sorting and app_sorting in [str(x) for x in NykaaImporter.default_sorting_map.keys()]):
          app_sorting = '1'
        else:
          app_sorting = str(app_sorting)
        offer_info['sort'] = NykaaImporter.default_sorting_map[app_sorting]
        offer_info['featured_products'] = item['custom_sort'].split(',') if item['custom_sort'] else []
        MemcacheUtils.set(memcache_key, json.dumps(offer_info), update_prod_memcache=True)
      except Exception as e:
        print("[Offer Info Import ERROR]problem with %s: %s"%(item['offer_id'], str(e)))
    print("==== Imported %s offer items ===="%count)

  def importMetaInformation():
    # Import category meta information
    nykaa_mysql_conn = Utils.nykaaMysqlConnection()
    query = """SELECT e.entity_id AS category_id, CONCAT(ccevt.value, ' | Nykaa') AS meta_title, REPLACE(REPLACE(ccetk.value, '\r', ''), '\n', '') AS meta_keywords, 
               REPLACE(REPLACE(ccetd.value, '\r', ''), '\n', '') AS meta_description FROM catalog_category_entity e
               LEFT JOIN catalog_category_entity_varchar ccevt ON ccevt.entity_id = e.entity_id AND ccevt.attribute_id = 36
               LEFT JOIN catalog_category_entity_text ccetk ON ccetk.entity_id = e.entity_id AND ccetk.attribute_id = 37
               LEFT JOIN catalog_category_entity_text ccetd ON ccetd.entity_id = e.entity_id AND ccetd.attribute_id = 38
               WHERE ccevt.value IS NOT NULL OR ccetk.value IS NOT NULL OR ccetd.value IS NOT NULL;"""    
    results = Utils.fetchResults(nykaa_mysql_conn, query)
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

