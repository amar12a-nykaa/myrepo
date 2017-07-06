import sys
import json
import socket
import urllib.parse
import urllib.request
from datetime import datetime
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils, MemcacheUtils


class PipelineUtils:

  def getAPIHost():
    host = 'localhost'
    if socket.gethostname().startswith('admin'):
      host = 'priceapi.nyk00-int.network'
    return host 

  def getOptionAttributes(option_ids):
    option_attrs = {}
    missing_option_ids = []
    for option_id in option_ids:
      key = 'option-%s' % option_id
      jsonstr = MemcacheUtils.get(key)
      if jsonstr:
        option_attrs[option_id] = json.loads(jsonstr)
      else:
        missing_option_ids.append(option_id)

    if missing_option_ids:
      # get details from DB
      mysql_conn = Utils.mysqlConnection('r')

      in_p = ','.join(['%s']*len(missing_option_ids))
      query = "SELECT fa.id, fa.name, av.value FROM filter_attributes fa LEFT JOIN attribute_values av ON (fa.id = av.attribute_id) WHERE fa.id IN (%s)"%in_p
      results = Utils.fetchResults(mysql_conn, query, tuple(missing_option_ids))
      for result in results:
        option_id = result['id']
        option_value = result['value']
        if not option_id in option_attrs.keys():
          option_attrs[option_id] = {'id': option_id, 'name': result['name']}
        if option_value:
          if 'color_code' in option_attrs[option_id].keys():
            option_attrs[option_id]['color_code'].append(option_value)
          else:
            option_attrs[option_id]['color_code'] = [option_value]
        jsonstr = json.dumps(option_attrs[option_id])
        MemcacheUtils.set(key, jsonstr)

    return option_attrs


  def getCategoryFacetAttributes(cat_ids):
    # get details from Nykaa DB
    cat_facet_attrs = []
    mysql_conn = Utils.nykaaMysqlConnection()
    in_p = ','.join(['%s']*len(cat_ids))
    query = "SELECT * FROM nk_categories WHERE category_id IN (%s)"%in_p
    results = Utils.fetchResults(mysql_conn, query, tuple(cat_ids))
    for result in results:
      cat_facet_attrs.append(result)

    return cat_facet_attrs

  def check_required_fields(columns, required_fields):
    missing_fields = set()
    missing_fields_singles = set(required_fields) - set(columns)
    for missing_field in missing_fields_singles:
      missing_fields.add(missing_field)

    if missing_fields:
      raise Exception("Missing Fields: %s" % missing_fields)

  def getProductsToIndex(products):
    params = json.dumps({"products": products}).encode('utf8')
    req = urllib.request.Request("http://" + PipelineUtils.getAPIHost() + "/apis/v1/pas.get", data=params, headers={'content-type': 'application/json'})
    pas_object = json.loads(urllib.request.urlopen(req, params).read().decode('utf-8'))['skus']
   
    update_docs = []
    swap_keys = {'sp': 'price', 'discount': 'discount', 'mrp': 'mrp', 'is_in_stock': 'in_stock', 'quantity': 'quantity', 'backorders': 'backorders', 'disabled': 'disabled'}

    for sku, pas in pas_object.items():
      update_fields = {}
      for key in swap_keys.keys():
        if pas.get(key) is not None:
          update_fields[swap_keys[key]] = {'set': pas[key]}
      update_fields.update({'sku': sku})
      update_fields['update_time'] = {'set': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}
      update_docs.append(update_fields)

    return update_docs

  def updateCatalog(sku, psku, product_type):
    products = []
    products.append({'sku': sku, 'type': product_type})
    if psku and psku!=sku:
      # has a parent product, re-index the parent configurable product also
      products.append({'sku': psku, 'type': 'configurable'})

    # check if product is part of bundle
    if product_type == 'simple':
      mysql_conn = Utils.mysqlConnection('r')
      cursor = mysql_conn.cursor()
      query = "SELECT product_sku, bundle_sku FROM bundle_products_mappings WHERE product_sku=%s"
      results = Utils.fetchResults(mysql_conn, query, (sku,))
      for res in results:
        products.append({'sku': res['bundle_sku'], 'type': 'bundle'})

    update_docs = PipelineUtils.getProductsToIndex(products)
    if update_docs:
      Utils.updateCatalog(update_docs)
    return len(update_docs)
