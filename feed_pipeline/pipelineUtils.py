import sys
import json
sys.path.append('/home/apis/nykaa/')
from pas.v1.utils import Utils, MemcacheUtils


class PipelineUtils:

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
        MemcacheUtils.set('option-%s' % option_id, jsonstr)

    return option_attrs


  def getCategoryFacetAttributes(cat_ids):
    # get details from Nykaa DB
    cat_facet_attrs = []
    mysql_conn = Utils.nykaaMysqlConnection()
    in_p = ','.join(['%s']*len(cat_ids))
    query = "SELECT * FROM nk_categories WHERE category_id IN (%s)"%in_p
    results = Utils.fetchResults(mysql_conn, query, tuple(cat_ids))
    for result in results:
      #print('category_facet: %s'%result)
      cat_facet_attrs.append(result)

    return cat_facet_attrs

  def check_required_fields(columns, required_fields):
    missing_fields = set()
    missing_fields_singles = set(required_fields) - set(columns)
    for missing_field in missing_fields_singles:
      missing_fields.add(missing_field)

    if missing_fields:
      raise Exception("Missing Fields: %s" % missing_fields)

  def validate_catalog_feed_row(row):
    non_empty_fields = ['sku', 'product_id', 'name', 'type_id', 'created_at']
    for key, value in row.items():
      value = value.strip()
      value = '' if value.lower() == 'null' else value
      if key in ['sku', 'parent_sku']:
        value = value.upper()
      row[key] = value

      if key in non_empty_fields and not row[key]:
        raise Exception("'"+key+"' cannot be empty.")
