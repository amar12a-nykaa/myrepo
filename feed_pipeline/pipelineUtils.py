import sys
import json
import socket
import requests
import urllib.parse
import urllib.request
from datetime import datetime
import boto3
sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils
from disc.v2.utils import MemcacheUtils
#from pas.v2.models import Product
from IPython import embed 

class PipelineUtils:

  def chunks(l, n):
    """Yield successive n-sized chunks from list l."""
    for i in range(0, len(l), n):
      yield l[i:i + n]

  def getAPIHost():
    host = 'localhost'
    if socket.gethostname().startswith('admin'):
      host = 'priceapi.nyk00-int.network'
    return host

  def getOffersAPIHost():
    host = 'preprod-api.nykaa.com/offer'
    if socket.gethostname().startswith('admin'):
      host = 'prod-offer.nyk00-int.network/offer'
    return host

  def getSQSDetails():
    SQS_ENDPOINT = 'https://sqs.ap-south-1.amazonaws.com/911609873560/failedOfferUpdate'
    SQS_REGION = 'ap-south-1'
    if socket.gethostname().startswith('admin'):
      SQS_ENDPOINT = 'https://sqs.ap-south-1.amazonaws.com/268361018769/failedOfferUpdate'
    return SQS_ENDPOINT, SQS_REGION

  def getVarnishDetails():
    TARGET_GROUP_ARNS = [{
      'region': 'ap-south-1',
      'tg': 'arn:aws:elasticloadbalancing:ap-south-1:911609873560:targetgroup/preprod-cd-pdp-varnish-tg/b4df51ce8c2ffc49'
    }, {
      'region': 'ap-south-1',
      'tg': 'arn:aws:elasticloadbalancing:ap-south-1:911609873560:targetgroup/preprod-cd-main-varnish-tg/e93af2b86500a118'
    }
    ]
    if socket.gethostname().startswith('admin'):
      TARGET_GROUP_ARNS = [{
        'region': 'ap-south-1',
        'tg': 'arn:aws:elasticloadbalancing:ap-south-1:268361018769:targetgroup/prod-cd-pdp-varnish-tg/90762f691b35b2e6'
      }, {
        'region': 'ap-south-1',
        'tg': 'arn:aws:elasticloadbalancing:ap-south-1:268361018769:targetgroup/prod-cd-main-varnish-tg/8c91dae2f815b6e8'
      }]
    varnish_hosts = {}
    for target_group_arn in TARGET_GROUP_ARNS:
      varnish_hosts_current_tg = {}
      elbv2_client = boto3.client('elbv2', region_name=target_group_arn['region'])
      ec2_client = boto3.client('ec2', region_name=target_group_arn['region'])
      health_response = elbv2_client.describe_target_health(TargetGroupArn=target_group_arn['tg'])
      if health_response.get('TargetHealthDescriptions'):
        for instance in health_response.get('TargetHealthDescriptions', []):
          if instance.get('TargetHealth', {}).get('State') == 'healthy':
            varnish_hosts_current_tg[instance.get('Target', {}).get('Id')] = {
              'id': instance.get('Target', {}).get('Id'), 'port': instance.get('Target', {}).get('Port')}
      if varnish_hosts_current_tg:
        instances_response = ec2_client.describe_instances(InstanceIds=list(varnish_hosts_current_tg.keys()))
        if instances_response:
          reservations = instances_response.get('Reservations')
          for reservation in reservations:
            for instance in reservation.get('Instances', [{}]):
              instance_id = instance.get('InstanceId')
              private_ip = instance.get('PrivateIpAddress')
              if varnish_hosts_current_tg.get(instance_id):
                varnish_hosts_current_tg.get(instance_id).update({'ip': private_ip})
      varnish_hosts.update(varnish_hosts_current_tg)
    hosts = []
    for instance_id in varnish_hosts:
      varnish_host = varnish_hosts[instance_id]
      hosts.append("{}:{}".format(varnish_host.get('ip'), varnish_host.get('port')))
    return hosts

  def getDiscoveryVarnishPurgeSQSDetails():
    SQS_ENDPOINT = 'https://sqs.ap-south-1.amazonaws.com/911609873560/DiscoveryVarnishPurgeQueue'
    SQS_REGION = 'ap-south-1'
    if socket.gethostname().startswith('admin'):
      SQS_ENDPOINT = 'https://sqs.ap-south-1.amazonaws.com/268361018769/DiscoveryVarnishPurgeQueue'
    return SQS_ENDPOINT, SQS_REGION

  def getAdPlatformEndPoint():
    host = 'nykaa-widgets-staging.nykaa.com'
    if socket.gethostname().startswith('admin'):
      host = 'nykaa-widgets.nykaa.com'
    return host

  def getBucketNameForFeedback():
    bucket_name = 'nykaa-nonprod-feedback-autocomplete'
    if socket.gethostname().startswith('admin'):
      bucket_name = 'nykaa-prod-autocomplete-feedback'
    return bucket_name

  def getBucketNameForDailyDiscounts():
      bucket_name = 'nonprod-gludo'
      if socket.gethostname().startswith('admin'):
          bucket_name = 'prod-gludo'
      return bucket_name

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
      mysql_conn = DiscUtils.mysqlConnection('r')

      in_p = ','.join(['%s']*len(missing_option_ids))
      query = "SELECT fa.id, fa.name, av.value FROM filter_attributes fa LEFT JOIN attribute_values av ON (fa.id = av.attribute_id) WHERE fa.id IN (%s)"%in_p
      results = DiscUtils.fetchResults(mysql_conn, query, tuple(missing_option_ids))
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
        key = 'option-%s' % option_id
        MemcacheUtils.set(key, jsonstr)

    return option_attrs


  def getCategoryFacetAttributes(cat_ids):
    # get details from Nykaa DB
    cat_facet_attrs = []
    mysql_conn = DiscUtils.nykaaMysqlConnection()
    in_p = ','.join(['%s']*len(cat_ids))
    query = "SELECT * FROM nk_categories WHERE category_id IN (%s)"%in_p
    results = DiscUtils.fetchResults(mysql_conn, query, tuple(cat_ids))
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

  def getProductsToIndex(products, add_limit = False):
    update_docs = []

    # Check if to-be-updated skus are actually present in ES, ignore skus not present
    final_products_to_update = []
    product_skus = [product['sku'].upper() for product in products]
    querydsl = {}
    if product_skus:
      sku_should_query = []
      for sku in product_skus:
        sku_should_query.append({'term': {'sku.keyword': sku}})
      querydsl['query'] = {'bool': {'should': sku_should_query}}
      querydsl['_source'] = ['sku', 'type']
      if add_limit:
        querydsl['size'] = len(products) + 1

      response = DiscUtils.makeESRequest(querydsl, index='livecore')
      docs = response['hits']['hits']
      for doc in docs:
        final_products_to_update.append({'sku': doc['_source']['sku'], 'type': doc['_source']['type']})

    # pas_object = Product.getPAS(final_products_to_update)

    params = json.dumps({"products": final_products_to_update}).encode('utf8')
    req = urllib.request.Request("http://" + PipelineUtils.getAPIHost() + "/apis/v2/pas.get", data=params,
                                 headers={'content-type': 'application/json'})
    pas_object = json.loads(urllib.request.urlopen(req, params).read().decode('utf-8')).get('skus', {})

    for product in final_products_to_update:
      update_fields = {}
      sku = product['sku']
      product_type = product['type']
      pas = pas_object.get(sku) or {}
      swap_keys = {'sp': 'price', 'discount': 'discount', 'mrp': 'mrp', 'is_in_stock': 'in_stock',
                   'quantity': 'quantity', 'backorders': 'backorders', 'disabled': 'disabled'}
      for key in swap_keys.keys():
        if pas.get(key) is not None:
          update_fields[swap_keys[key]] = pas[key]
        if key == 'is_in_stock':
          update_fields[swap_keys[key]] = bool(pas.get(key, False))
      update_fields.update({'sku': sku})
      update_fields['update_time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
      if product_type == 'bundle':
        bundle_products = pas.get('products', {})
        product_qty_map = {}
        for product_sku, bundle_prod in bundle_products.items():
          product_qty_map[product_sku] = bundle_prod.get('quantity_in_bundle', 0)
        update_fields['product_qty_map'] = json.dumps(product_qty_map)
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
      mysql_conn = DiscUtils.mysqlConnection('r')
      query = "SELECT product_sku, bundle_sku FROM bundle_products_mappings WHERE product_sku=%s"
      results = DiscUtils.fetchResults(mysql_conn, query, (sku,))
      for res in results:
        products.append({'sku': res['bundle_sku'], 'type': 'bundle'})

    update_docs = PipelineUtils.getProductsToIndex(products)
    if update_docs:
      DiscUtils.updateESCatalog(update_docs)
    return len(update_docs)



if __name__ == "__main__":
  #print(ret)
  pass
