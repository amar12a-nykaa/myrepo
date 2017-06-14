#!/usr/bin/python
import sys
import json
import socket
import argparse
import traceback
import dateparser
import urllib.request
import urllib.parse
from datetime import datetime
sys.path.append('/home/apis/nykaa/')
from pipelineUtils import PipelineUtils
from pas.v1.utils import Utils
from pas.v1.csvutils import read_csv_from_file
from pas.v1.exceptions import SolrError
from popularity_api import get_popularity_for_ids


class CatalogIndexer:
  PRODUCT_TYPES = ['simple', 'configurable', 'bundle']
  VISIBILITY_TYPES = ['visible', 'not_visible']

  def validate_catalog_feed_row(row):
    for key, value in row.items():
      value = value.strip()
      value = '' if value.lower() == 'null' else value

      if key=='sku':
        assert value, 'sku cannot be empty'
        value = value.upper()

      elif key=='parent_sku':
        value = value.upper()             

      elif key in ['product_id', 'name']:
        assert value, '%s cannot be empty'%key
  
      elif key=='type_id':
        value = value.lower()
        assert value in CatalogIndexer.PRODUCT_TYPES, "Invalid type: '%s'. Allowed are %s" %(value, CatalogIndexer.PRODUCT_TYPES)

      elif key in ['rating', 'review_count', 'qna_count']:
        if value:
          try:
            value = int(value)
          except Exception as e:
            raise Exception('Bad value - %s' % str(e)) 

      elif key=='rating_num':
        if value:
          try:
            value = float(value)
          except Exception as e:
            raise Exception('Bad value - %s' % str(e))

      elif key=='created_at':
        assert value, 'created_at cannot be empty'
        try:
          datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        except Exception as e:
          raise Exception("Incorrect created_at date format - %s" % str(e))        

      elif key=='visibility':
        assert value, 'visibility cannot be empty'
        value = value.lower()
        assert value in CatalogIndexer.VISIBILITY_TYPES, "Invalid type: '%s'. Allowed are %s" %(value, CatalogIndexer.VISIBILITY_TYPES)

      row[key] = value

  def index(file_path):
    host = 'localhost'
    if socket.gethostname().startswith('admin'):
      host = 'priceapi.nyk00-int.network'

    required_fields_from_csv = ['sku', 'parent_sku', 'product_id', 'type_id', 'name', 'description', 'product_url', 'price', 'special_price', 'discount', 'is_in_stock',
    'pack_size', 'tag', 'rating', 'rating_num', 'review_count', 'qna_count', 'try_it_on', 'image_url', 'main_image', 'shade_name', 'variant_icon', 'size',
    'variant_type', 'offer_name', 'offer_id', 'product_expiry', 'created_at', 'category_id', 'category', 'brand_v1', 'brand', 'shop_the_look_product', 'style_divas',
    'visibility', 'gender_v1', 'gender', 'color_v1', 'color', 'concern_v1', 'concern', 'finish_v1', 'finish', 'formulation_v1', 'formulation', 'try_it_on_type',
    'hair_type_v1', 'hair_type', 'benefits_v1', 'benefits', 'skin_tone_v1', 'skin_tone', 'skin_type_v1', 'skin_type', 'coverage_v1', 'coverage', 'preference_v1',
    'preference', 'spf_v1', 'spf', 'add_to_cart_url', 'parent_id', 'redirect_to_parent', 'eretailer', 'product_ingredients', 'vendor_id', 'vendor_sku']

    all_rows = read_csv_from_file(file_path)
    columns = all_rows[0].keys()
    PipelineUtils.check_required_fields(columns, required_fields_from_csv)

    count = len(all_rows)
    input_docs = []
    for index, row in enumerate(all_rows):
      try:
        CatalogIndexer.validate_catalog_feed_row(row)
        doc = {}
        doc['sku'] = row['sku']
        doc['product_id'] = row['product_id']
        doc['type'] = row['type_id']
        doc['psku'] = row['parent_sku'] if doc['type'] == 'simple' and row['parent_sku'] else row['sku']
        doc['parent_id'] = row['parent_id'] if doc['type'] == 'simple' and row['parent_id'] else row['product_id']
        doc['title'] = row['name']
        doc['title_text_split'] = row['name']
        doc['description'] = row['description']
        doc['tags'] = row['tag'].split(',')
        doc['star_rating_count'] = row['rating']
        if row['rating_num'] and row['rating_percentage']:
          doc['star_rating'] = row['rating_num']
          doc['star_rating_percentage'] = row['rating_percentage']
        doc['review_count'] = row['review_count'] or 0
        doc['qna_count'] = row['qna_count'] or 0
        if row['product_expiry']:
          doc['expiry_date'] = dateparser.parse(row['product_expiry'], ['%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M:%S']).strftime('%Y-%m-%dT%H:%M:%SZ')
        doc['is_service'] = row.get('d_sku') == '1'
        doc['pack_size'] = row['pack_size']
        doc['is_luxe'] = row.get('is_luxe') == '1'
        doc['can_subscribe'] = row.get('is_subscribable') == '1'
        doc['can_try'] = row.get('try_it_on') == '1'
        if doc['can_try']:
          doc['can_try_type'] = row.get('try_it_on_type')
        doc['show_wishlist_button'] = row.get('show_wishlist_button') == '1'
        doc['button_text'] = row.get('button_text')
        doc['how_to_use'] = row['product_use']
        doc['visibility'] = row['visibility']
        doc['product_ingredients'] = row['product_ingredients']
        if row['fbn']:
          doc['fbn'] = row['fbn'].lower() == 'yes'
        doc['add_to_cart_url'] = row['add_to_cart_url']
        if row['eretailer']:
          doc['eretailer'] = row['eretailer'] == '1'
        doc['redirect_to_parent'] = row['redirect_to_parent'] == '1'
        doc['shipping_quote'] = row.get('shipping_quote')
        doc['vendor_id'] = row['vendor_id']
        doc['vendor_sku'] = row['vendor_sku']

        #Popularity
        popularity_obj = get_popularity_for_ids([doc['product_id']]) 
        popularity_obj = popularity_obj.get(doc['product_id'])
        doc['popularity'] = 0 
        if popularity_obj:
          doc['popularity'] = popularity_obj['popularity']
            
        # Product URL and slug
        product_url = row['product_url']
        if product_url:
          doc['product_url'] = product_url
          split_url = product_url.rsplit('/', 1)
          if len(split_url) > 1:
            doc['slug'] = split_url[-1]

        #Category related
        category_ids = row['category_id'].split('|') if row['category_id'] else []
        category_names = row['category'].split('|') if row['category'] else []
        if category_ids and len(category_ids)==len(category_names):
          doc['category_ids'] = category_ids
          doc['category_values'] = category_names
          doc['category_facet'] = []
          cat_info = PipelineUtils.getCategoryFacetAttributes(category_ids)
          for info in cat_info:
            doc['category_facet'].append(info)
        elif len(category_ids)!=len(category_names):
          #with open("/data/inconsistent_cat.txt", "a") as f:
          #  f.write("%s\n"%doc['sku'])
          print('inconsistent category values for %s'%doc['sku'])

        if row.get('seller_name'):
          doc['seller_name'] = row['seller_name']
          if row.get('seller_rating'):
            doc['seller_rating'] = row['seller_rating']

        # media stuff
        doc['media'] = []
        main_image = row['main_image']
        images = row['image_url'].split('|') if row['image_url'] else []
        if main_image and images:
          for image in images:
            image = image.strip()
            if image != main_image:
              doc['media'].append({'type': 'image', 'url': image})
          doc['media'].insert(0, {'type': 'image', 'url': main_image})

        video = row.get('video')
        if video:
          doc['media'].append({'type': 'video', 'url': video})

        # variant stuff
        if doc['type'] == 'configurable':
          variant_type = row['variant_type']
          variant_skus = row['parent_sku'].split('|') if row['parent_sku'] else []
          variant_ids = row['parent_id'].split('|') if row['parent_id'] else []
          variant_name_key = ''
          variant_icon_key = ''
          if variant_type == 'shade':
            variant_name_key = 'shade_name'
            variant_icon_key = 'variant_icon'
          elif variant_type == 'size':
            variant_name_key = 'size'

          variant_names = row[variant_name_key].split('|') if variant_name_key and row[variant_name_key] else []
          variant_icons = row[variant_icon_key].split('|') if variant_icon_key and row[variant_icon_key] else []
          if variant_type and variant_skus and len(variant_skus)==len(variant_names) and len(variant_skus)==len(variant_ids):
            if variant_icons and len(variant_icons) != len(variant_skus):
              variant_icons = []
            variants_arr = []
            if not variant_icons:
              variant_icons = [""]*len(variant_skus)
            for i, sku in enumerate(variant_skus):
              variants_arr.append({'sku': variant_skus[i], 'id': variant_ids[i], 'name': variant_names[i], 'icon': variant_icons[i]})
            doc['variants'] = {variant_type: variants_arr}
            doc['variant_type'] = variant_type
          doc['option_count'] = len(variant_skus)
        elif doc['type'] == 'simple':
          variant_name = row['shade_name'] or row['size']
          variant_icon = row['variant_icon']
          if variant_name:
            doc['variant_name'] = variant_name
            if variant_icon:
              doc['variant_icon'] = variant_icon
        elif doc['type'] == 'bundle':
          doc['product_ids'] = row['parent_id'].split('|') if row['parent_id'] else []
          doc['product_skus'] = row['parent_sku'].split('|') if row['parent_sku'] else []

        #Price and availability
        dummy_product = row.get('shop_the_look_product') == '1' or row.get('style_divas') == '1'
        if dummy_product:
          # shop the look products, ignore PAS info
          doc['mrp'] = 0
          doc['price'] = 0
          doc['discount'] = 0
          doc['in_stock'] = False
        else:
          # get price and availability from PAS
          params = {'sku': doc['sku'], 'type': doc['type']}
          pas_object = json.loads(urllib.request.urlopen("http://" + host + "/apis/v1/pas.get?"+urllib.parse.urlencode(params)).read().decode('utf-8'))['skus'] 
          if pas_object.get(doc['sku']):
            pas = pas_object[doc['sku']]
            missing_fields = []
            swap_keys = {'sp': 'price', 'discount': 'discount', 'mrp': 'mrp', 'is_in_stock': 'in_stock'}
            for key in swap_keys.keys():
              if pas.get(key) is None:
                missing_fields.append(key)
              else:
                doc[swap_keys[key]] = pas[key]

            if pas.get('quantity') is not None:
              doc['quantity'] = pas.get('quantity')

            if pas.get('backorders') is not None:
              doc['backorders'] = pas.get('backorders') == True

            if pas.get('disabled') is not None:
              doc['disabled'] = pas.get('disabled')

            # if bundle, get qty of each product also
            if doc['type']=='bundle':
              bundle_products = pas.get('products', {})
              product_qty_map = {}
              for product_sku in doc.get('product_skus', []):
                prod_obj = bundle_products.get(product_sku) 
                if prod_obj:
                  product_qty_map[product_sku] = prod_obj.get('quantity', 0)
              doc['product_qty_map'] = product_qty_map  

            if missing_fields:
              #if doc['type']=='configurable':
              #  line = doc['sku'] + "  " + ",".join(row['parent_sku'].split("|"))
              #  with open("no_child_configurables.txt", "a") as f:
              #    f.write("%s\n"%line)
              raise Exception("Missing PAS fields: %s"%missing_fields)
          else:
            #r = json.loads(urllib.request.urlopen("http://priceapi.nyk00-int.network/apis/v1/pas.get?"+urllib.parse.urlencode(params)).read().decode('utf-8'))
            #if not r['skus'].get(doc['sku']):
              #with open("/data/missing_skus.txt", "a") as f:
                #f.write("%s\n"%doc['sku'])
            raise Exception("sku not found in Price DB.")
        doc['is_saleable'] = doc['in_stock']

        # offers stuff
        offer_ids = row['offer_id'].split("|") if row['offer_id'] else []
        offer_names = row['offer_name'].split("|") if row['offer_name'] else []
        offer_descriptions = row['offer_description'].split("|") if row['offer_description'] else []
        doc['offers'] = []
        if offer_ids and len(offer_ids) == len(offer_names) and len(offer_ids) == len(offer_descriptions):
          doc['offer_ids'] = offer_ids
          doc['offer_facet'] = []
          for i, offer_id in enumerate(offer_ids):
            doc['offers'].append({'id': offer_ids[i], 'name': offer_names[i], 'description': offer_descriptions[i]})
            doc['offer_facet'].append({'id': offer_ids[i], 'name': offer_names[i]})
        #elif offer_ids:
          #with open("/data/inconsistent_offers.txt", "a") as f:
          #  f.write("%s\n"%doc['sku'])
          #print('inconsistent offer values for %s'%doc['sku'])
        doc['offer_count'] = len(doc['offers'])

        # facets: dynamic fields
        facet_fields = [field for field in required_fields_from_csv if field.endswith("_v1")]
        for field in facet_fields:
          field_prefix = field.rsplit('_', 1)[0]
          facet_ids = row[field].split('|') if row[field] else []
          facet_values = row[field_prefix].split('|') if row[field_prefix] else []
          if facet_ids and len(facet_ids) == len(facet_values):
            doc[field_prefix + '_ids'] = facet_ids
            doc[field_prefix + '_values'] = facet_values
            facets = []
            if field_prefix == 'brand':
              for i, brand_id in enumerate(facet_ids):
                facets.append({'id': brand_id, 'name': facet_values[i]})
            else:
              option_attrs = PipelineUtils.getOptionAttributes(facet_ids)
              for attr_id, attrs in option_attrs.items():
                facets.append(attrs)
            doc[field_prefix + '_facet'] = facets
          #elif len(facet_ids) != len(facet_values):
          #  with open("/data/inconsistent_facet.txt", "a") as f:
          #    f.write("%s  %s\n"%(doc['sku'], field))

        # meta info: dynamic fields
        meta_fields = [field for field in row.keys() if field.startswith("meta_")]
        for field in meta_fields:
          doc[field] = row.get(field, "")

        doc['update_time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        doc['create_time'] = row['created_at']
        doc['object_type'] = "product"
        input_docs.append(doc)

        #index to solr in batches of 300
        if ((index+1) % 300 == 0) or (index+1 >= count):
          Utils.indexCatalog(input_docs)
          input_docs = []
      except SolrError as e:
        raise Exception(str(e))
      except Exception as e:
        print(traceback.format_exc())
        print("Error with %s: %s"% (row['sku'], str(e)))


if __name__ == "__main__": 
  parser = argparse.ArgumentParser()
  parser.add_argument("-f", "--filepath", required=True, help='path to csv file')
  argv = vars(parser.parse_args())
  file_path = argv['filepath']

  CatalogIndexer.index(file_path)
