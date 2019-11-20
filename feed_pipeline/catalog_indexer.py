#!/usr/bin/python

import argparse
import ast
import csv
import dateparser
import datetime as dt
import dateutil.relativedelta
import json
import numpy
import os
import pprint
import queue
import re
import socket
import subprocess
import sys
import threading
import time
import timeit
import traceback
import boto3

from IPython import embed
from collections import OrderedDict
from collections import defaultdict 
from datetime import datetime, timedelta
from dateutil import tz
from operator import itemgetter
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pipelineUtils import PipelineUtils
from popularity_api import get_popularity_for_id, validate_popularity_data_health, get_bestseller_products

sys.path.append("/nykaa/scripts/sharedutils")
from esutils import EsUtils
from loopcounter import LoopCounter
from mongoutils import MongoUtils
from offerutils import OfferUtils

sys.path.append('/nykaa/scripts/recommendations/scripts/personalized_search/')
from generate_user_product_vectors import get_vectors_from_mysql_for_es

sys.path.append("/var/www/pds_api")
from pas.v2.csvutils import read_csv_from_file

sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

hostname = socket.gethostname()
if not(hostname.startswith('admin') or hostname.startswith('preprod')):
	NUMBER_OF_THREADS = 2
else:
	NUMBER_OF_THREADS = 20

RECORD_GROUP_SIZE = 100
APPLIANCE_MAIN_CATEGORY_ID = "1390"
CM_TO_INCH_FACTOR = 0.3937
INCH_TO_CM_FACTOR = 2.54

conn = DiscUtils.mysqlConnection()

total = 0
def synchronized(func):
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func

@synchronized
def incrementGlobalCounter(increment):
    global total
    curr = total + increment
    total = curr
    return total

def getCurrentDateTime():
    current_datetime = datetime.utcnow()
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('Asia/Kolkata')
    current_datetime = current_datetime.replace(tzinfo=from_zone)
    current_datetime = current_datetime.astimezone(to_zone)
    return current_datetime

class Worker(threading.Thread):
    def __init__(self, q, search_engine, collection, skus, categoryFacetAttributesInfoMap, offersApiConfig, required_fields_from_csv,
                 update_productids, product_2_vector_lsi_100, product_2_vector_lsi_200, product_2_vector_lsi_300,size_filter,offerbatchsize,offerswitch):
        self.q = q
        self.search_engine = search_engine
        self.collection = collection
        self.skus = [x for x in skus if x]
        self.categoryFacetAttributesInfoMap = categoryFacetAttributesInfoMap
        self.offersApiConfig = offersApiConfig
        self.required_fields_from_csv = required_fields_from_csv
        self.update_productids = update_productids
        self.product_2_vector_lsi_100 = product_2_vector_lsi_100 
        self.product_2_vector_lsi_200 = product_2_vector_lsi_200 
        self.product_2_vector_lsi_300 = product_2_vector_lsi_300 
        self.size_filter = size_filter
        self.offerbatchsize = offerbatchsize
        self.offerswitch = offerswitch

        super().__init__()

    def run(self):
        is_first=True
        while True:
            try:
                rows = self.q.get(timeout=3)  # 3s timeout
                client = MongoUtils.getClient()
                product_history_table = client['search']['product_history']
                product_history = {}
                product_ids=[]
                for row in rows:
                  product_ids.append(row['product_id'])
                db_result = product_history_table.find({"_id": {"$in": product_ids}})
                for row in db_result:
                  product_history[row['_id']] = row
                CatalogIndexer.indexRecords(rows, self.search_engine, self.collection, self.skus, self.categoryFacetAttributesInfoMap, self.offersApiConfig, self.required_fields_from_csv, self.update_productids, self.product_2_vector_lsi_100, self.product_2_vector_lsi_200, self.product_2_vector_lsi_300,self.size_filter,is_first,product_history,self.offerbatchsize,self.offerswitch)
                is_first=False
            except queue.Empty:
                return
            # do whatever work you have to do on work
            self.q.task_done()
            total_count = incrementGlobalCounter(len(rows))
            print("[%s] Update progress: %s products updated" % (getCurrentDateTime(), total_count))


class CatalogIndexer:
    PRODUCT_TYPES = ['simple', 'configurable', 'bundle']
    VISIBILITY_TYPES = ['visible', 'not_visible']
    DOCS_BATCH_SIZE = 2000

    field_type_pattens = {
        ".*_i$": int,
    }

    replace_brand_dict = {
        "faces": "faces canada"
    }

    final_replace_dict = {
        "makeup": "makeup make up",
        "make up": "makeup make up",
        "skingenius": "skingenius skin genius",
        "body wash": "bodywash body wash",
        "bodywash": "body wash bodywash",
        "face wash": "facewash face wash",
        "facewash": "facewash face wash",
        "anti acne": "antiacne anti acne",
        "anti oxidants": "antioxidants anti oxidants",
        "antiacne": "anti acne antiacne",
        "antioxidants": "anti oxidants antioxidants",
        "eye liner": "eyeliner eye liner",
        "eye shadow": "eyeshadow eye shadow",
        "eyeliner": "eye liner eyeliner",
        "eyeshadow": "eye shadow eyeshadow",
        "foot care": "footcare foot care",
        "footcare": "foot care footcare",
        "hair care": "haircare hair care",
        "hair fall": "hairfall hair fall",
        "hair spray": "hairspray hair spray",
        "haircare": "hair care haircare",
        "hairfall": "hair fall hairfall",
        "hairspray": "hair spray hairspray",
        "hand wash": "handwash hand wash",
        "handwash": "hand wash handwash",
        "lip balm": "lipbalm lip balm",
        "lip gloss": "lipgloss lip gloss",
        "lip liner": "lipliner lip liner",
        "lip stick": "lipstick lip stick",
        "lipbalm": "lip balm lipbalm",
        "lipgloss": "lip gloss lipgloss",
        "lipliner": "lip liner lipliner",
        "lipstick": "lip stick lipstick",
        "maang tikka": "maangtikka maang tikka",
        "maangtikka": "maang tikka maangtikka",
        "mouth wash": "mouthwash mouth wash",
        "mouthwash": "mouth wash mouthwash",
        "nail polish": "nailpolish nail polish",
        "nailpolish": "nail polish nailpolish",
        "night dress": "nightdress night dress",
        "nightdress": "night dress nightdress",
        "panty liners": "pantyliners panty liners",
        "pantyliners": "panty liners pantyliners",
        "racer back": "racerback racer back",
        "racerback": "racer back racerback",
        "shower caps": "showercaps shower caps",
        "showercaps": "shower caps showercaps",
        "sleep shirt": "sleepshirt sleep shirt",
        "sleepshirt": "sleep shirt sleepshirt",
        "sun screen": "sunscreen sun screen",
        "sunscreen": "sun screen sunscreen",
        "super food": "superfood super food",
        "superfood": "super food superfood",
        "tooth paste": "toothpaste tooth paste",
        "toothpaste": "tooth paste toothpaste",
        "under wired": "underwired under wired",
        "underwired": "under wired underwired",
        "weight loss": "weightloss weight loss",
        "weightloss": "weight loss weightloss",
        "vwash": "v wash vwash",
        "mini": "little mini",
        "little": "little mini",
        "eyelash": "eye lash eyelash",
        "matte": "matt matte",
        "color pop": "colorpop color pop",
        "mamaearth": "mamaearth mama earth",
        "colorbar": "color bar colorbar",
        "glamglow": "glamglow glam glow",
        "deodorants": "deodorants deo",
        "moisturizer": "moisturizer moisturiser",
        "almond": "badam almond",
        "badam": "badam almond",
        "de tan": "de tan d tan",
        "d tan": "de tan d tan",
        "thebalm": "thebalm the balm",
        "bblunt": "bblunt b blunt",
        "boroplus": "boroplus boro plus",
        "sheet mask": "sheet mask face mask",
        "flashmud": "flashmud flash mud",
        "supermud": "supermud super mud",
        "thirstymud": "thirstymud thirsty mud",
        "gravitymud": "gravitymud gravity mud",
        "youthmud": "youthmud youth mud",
        "waxing": "waxing wax",
        "wax": "wax waxing",
        "bags": "bags women women's ladies girls female girl",
        "jewellery": "jewellery women women's ladies girls female girl",
        "lingerie": "lingerie women women's ladies girls female girl",
        "dresses": "dresses women women's ladies girls female girl",
        "kay beauty": "kay beauty katrina kaif by k",
        "twenty dresses": "twenty dresses 20dresses 20"
    }

    folderpath = "/nykaa/product_metadata/"

    filepath = folderpath +"data.csv"

    if os.path.exists(filepath):
      os.remove(filepath)
    if not os.path.exists(folderpath):
      os.makedirs(folderpath)

    def print_errors(errors):
        for err in errors:
            print("[ERROR]: " + err)

    def validate_catalog_feed_row(row):
        for key, value in row.items():
            try:
                if value is None:
                    value = ""
                value = value.strip()
                value = '' if value.lower() == 'null' else value

                if key == 'sku':
                    assert value, 'sku cannot be empty'
                    value = value.upper()

                elif key == 'parent_sku':
                    value = value.upper()

                elif key in ['product_id', 'name']:
                    assert value, '%s cannot be empty' % key

                elif key == 'type_id':
                    value = value.lower()
                    assert value in CatalogIndexer.PRODUCT_TYPES, "Invalid type: '%s'. Allowed are %s" % (
                    value, CatalogIndexer.PRODUCT_TYPES)

                elif key in ['review_count', 'qna_count']:
                    if value:
                        try:
                            value = int(value)
                        except Exception as e:
                            raise Exception('Bad value for key %s- %s' % (key, str(e)))

                elif key in ['rating', 'rating_num']:
                    if value:
                        try:
                            value = float(value)
                        except Exception as e:
                            raise Exception('Bad value for key %s- %s' % (key, str(e)))

                elif key == 'bucket_discount_percent':
                    if value:
                        try:
                            value = float(value)
                        except Exception as e:
                            raise Exception('Bad value - %s' % str(e))

                elif key == 'created_at':
                    assert value, 'created_at cannot be empty'
                    try:
                        datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
                    except Exception as e:
                        raise Exception("Incorrect created_at date format - %s" % str(e))

                elif key == 'visibility':
                    assert value, 'visibility cannot be empty'
                    value = value.lower()
                    assert value in CatalogIndexer.VISIBILITY_TYPES, "Invalid type: '%s'. Allowed are %s" % (
                    value, CatalogIndexer.VISIBILITY_TYPES)

                row[key] = value
            except:
                print("[ERROR] Could not process row: %s" % row)
                raise 

    def getCategoryFacetAttributesMap():
        cat_facet_attrs = {}
        mysql_conn = DiscUtils.nykaaMysqlConnection()
        query = "SELECT * FROM nk_categories"
        results = DiscUtils.fetchResults(mysql_conn, query)
        for result in results:
            cat_facet_attrs[str(result['category_id'])] = result

        return cat_facet_attrs

    def getOffersApiConfig():
        mysql_conn = DiscUtils.nykaaMysqlConnection()
        query = "SELECT plain_value FROM core_variable_value cvv JOIN core_variable cv ON cv.variable_id = cvv.variable_id WHERE code = 'offers-api-config';"
        results = DiscUtils.fetchResults(mysql_conn, query)
        offers_api_config = None
        for key, config_value in enumerate(results):
            if config_value['plain_value']:
                offers_api_config = json.loads(config_value['plain_value'])
                offers_api_config['product_ids'] = offers_api_config['product_ids'].split(',')
        return offers_api_config

    def getSizeFilterConfig():
        mysql_conn = DiscUtils.nykaaMysqlConnection()
        query  = "SELECT plain_value FROM core_variable_value WHERE variable_id = (SELECT variable_id FROM core_variable WHERE CODE = 'enable_disable_flags')"
        result = DiscUtils.fetchResults(mysql_conn, query)
        config = None
        for key, config_value in enumerate(result):
            if config_value['plain_value']:
                config = json.loads(config_value['plain_value'])
                size_filter = config['size_filter']

        return size_filter

    def fetch_price_availability(input_docs, pws_fetch_products):
        request_url = "http://" + PipelineUtils.getAPIHost() + "/apis/v2/pas.get"
        request_data = json.dumps({'products': pws_fetch_products}).encode('utf8')
        req = Request(request_url, data=request_data, headers={'content-type': 'application/json'})
        attempts = 3
        while (attempts):
            try:
                pas_object = json.loads(str(urlopen(req).read().decode('utf-8')))
                break
            except:
                attempts -= 1
                print("WARNING ... Attempts remaining: %s, Failed to fetch data: %s %s " % (
                attempts, request_url, request_data))
                if attempts == 0:
                  embed()
                  raise

        pas_object = pas_object.get('skus')

        pws_input_docs = []
        errors = []
        for doc in input_docs:
            if doc.get('mrp') is None:  # Isn't a dummy product
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

                    if pas.get('jit_eretail') is not None:
                        doc['jit_eretail'] = pas.get('jit_eretail') == True

                    if pas.get('disabled') is not None:
                        doc['disabled'] = pas.get('disabled')

                    if pas.get('mrp_freeze') is not None:
                        doc['mrp_freeze'] = pas.get('mrp_freeze')

                    if pas.get('expdt') is not None:
                        try:
                            if doc['primary_categories'][0]['l1']['id'] == APPLIANCE_MAIN_CATEGORY_ID:
                                doc['expdt'] = None
                            else:
                                doc['expdt'] = pas.get('expdt')
                        except:
                            print("primary_categories key missing for sku: %s" % pas.get('sku'))

                    # if bundle, get qty of each product also
                    if doc['type'] == 'bundle':
                        bundle_products = pas.get('products', {})
                        product_qty_map = {}
                        for product_sku in (doc.get('product_skus', []) or []):
                            prod_obj = bundle_products.get(product_sku)
                            if prod_obj:
                                product_qty_map[product_sku] = prod_obj.get('quantity_in_bundle', 0)
                        doc['product_qty_map'] = product_qty_map

                    if missing_fields:
                        # if doc['type']=='configurable':
                        #  line = doc['sku'] + "  " + ",".join(row['parent_sku'].split("|"))
                        #  with open("no_child_configurables.txt", "a") as f:
                        #    f.write("%s\n"%line)
                        errors.append("%s: Missing PAS fields: %s" % (doc['sku'], missing_fields))
                        continue
                        # raise Exception("Missing PAS fields: %s"%missing_fields)
                else:
                    # r = json.loads(urllib.request.urlopen("http://priceapi.nyk00-int.network/apis/v2/pas.get?"+urllib.parse.urlencode(params)).read().decode('utf-8'))
                    # if not r['skus'].get(doc['sku']):
                    # with open("/data/missing_skus.txt", "a") as f:
                    # f.write("%s\n"%doc['sku'])
                    errors.append("%s: sku not found in Price DB." % doc['sku'])
                    continue
                    # raise Exception("sku not found in Price DB.")
            doc['in_stock'] = bool(doc['in_stock'])
            doc['is_saleable'] = doc['in_stock']
            pws_input_docs.append(doc)
        return pws_input_docs, errors

    def fetch_offers(input_docs, offerbatchsize=1000):

        request_url = "http://" + PipelineUtils.getOffersAPIHost() + "/api/v1/catalog/products/offer"
        SQS_ENDPOINT, SQS_REGION = PipelineUtils.getSQSDetails()
        for i in range(0, len(input_docs), offerbatchsize):
            current_docs_batch = input_docs[i:i+offerbatchsize]
            req_body = []
            product_id_list = []
            for doc in current_docs_batch:
                record = {}
                if doc.get("product_id"):
                    record["productId"] = doc.get('product_id')
                    product_id_list.append(record["productId"])
                else:
                    continue
                record["sku"] = doc.get('sku', "")
                record["name"] = doc.get('title', "")
                record["description"] = doc.get('description', "")
                record["categoryIds"] = ""
                if doc.get('category_ids'):
                    category_ids_list = doc.get('category_ids')
                    record["categoryIds"] = ','.join(str(category_id) for category_id in category_ids_list)
                record["mrp"] = doc.get('mrp', 0)
                record["sp"] = doc.get('price', 0)
                manufacturer_id_list = doc.get('old_brand_ids',[])
                if len(manufacturer_id_list)==0:
                    record["manufacturerId"] = ""
                else:
                    record["manufacturerId"] = manufacturer_id_list[0]
                req_body.append(record)
            req_body = json.dumps(req_body).encode('utf8')

            attempts = 2
            while(attempts):
                try:
                    res = Request(request_url, data=req_body, headers={'content-type': 'application/json'})
                    result = json.loads(str(urlopen(res).read().decode('utf-8')))
                    break
                except:
                    attempts = attempts-1
                    print("Bulk offers api request failed. Attempts remaining %s " % attempts)
                    if attempts==0:
                        print("Skipping offers updation on " + str(product_id_list))
                        print(traceback.format_exc())

            if(attempts==0):
                try:
                    sqs = boto3.client("sqs", region_name=SQS_REGION)
                    queue_url = SQS_ENDPOINT
                    response = sqs.send_message(
                        QueueUrl=queue_url,
                        DelaySeconds=0,
                        MessageAttributes={},
                        MessageBody=(json.dumps(product_id_list, default=str))
                    )
                except:
                    print(traceback.format_exc())
                    print("Insertion in SQS failed")
                continue
            #update in input docs
            response_data = result.get('data',{})
            for doc in current_docs_batch:
                product_id = doc.get('product_id')
                offers_data = response_data.get(product_id,{})
                if offers_data.get('offers'):
                    offers_dict = offers_data.get('offers',{})
                    OfferUtils.merge_offer_data(doc, offers_dict)
                    doc['offerUpdateEnable'] = True
                else:
                    error = offers_data.get('error')
                    print("Product_id {},Error {}".format(product_id,error))

    def indexES(docs, index):
        if not index:
            indexes = EsUtils.get_active_inactive_indexes("livecore")
            index = indexes['active_index']
        EsUtils.indexDocs(docs, index)

    def update_generic_attributes_filters(doc, row):
        generic_attributes_raw = row.get('generic_attributes', '')
        generic_multiselect_attributes_raw = row.get('generic_multiselect_attributes', '')
        generic_filters_raw = row.get('generic_filters', '')
        try:
            generic_attributes_raw = '{' + generic_attributes_raw + '}'
            generic_attributes = json.loads(generic_attributes_raw.replace('\n', ' ').replace('\\n', ' ').replace('\r', '').replace('\\r', '').replace('\\\\"', '\\"'))

            for attribute_key, attribute_data in generic_attributes.items():
                value = attribute_data.get('value')
                type = attribute_data.get('type')

                if type == 'int':
                    value = int(value)
                elif type == 'decimal':
                    value = float(value)

                doc[attribute_key] = value

            generic_filters_raw = '{' + generic_filters_raw + '}'
            generic_filters = json.loads(generic_filters_raw.replace('\n', ' ').replace('\\n', ' ').replace('\r', '').replace('\\r', '').replace('\\\\"', '\\"'))

            for attribute_key, facets_data in generic_filters.items():

                facets = []
                facet_ids = []
                facet_values = []

                for facet_data in facets_data:
                    id = facet_data.get('id')
                    value = facet_data.get('value')
                    facet_ids.append(id)
                    facet_values.append(value)
                    facets.append({"id": id, "name": value})

                doc[attribute_key + '_ids'] = facet_ids
                doc[attribute_key + '_values'] = facet_values
                doc[attribute_key + '_facet'] = facets

            generic_multiselect_attributes_raw = '{' + generic_multiselect_attributes_raw + '}'
            generic_multiselect_attributes = json.loads(generic_multiselect_attributes_raw.replace('\n', ' ').replace('\\n', ' ').replace('\r', '').replace('\\r', '').replace('\\\\"', '\\"'))

            for attribute_key, attribute_values in generic_multiselect_attributes.items():
                doc[attribute_key] = attribute_values

        except Exception as ex:
            print({"msg": ex, "row": row})

    def update_size_chart_attributes(doc):
        size_chart_attributes = ['size_bust',
                                 'size_hip',
                                 'size_length',
                                 'size_overbust',
                                 'size_shoulder',
                                 'size_underbust',
                                 'size_waist']
        if doc.get('size_unit') and doc.get('size_unit').strip().lower() in ['cm', 'inch']:
            size_unit = doc.pop('size_unit')
            inch_size = {}
            cm_size = {}
            for attribute in size_chart_attributes:
                if doc.get(attribute):
                    value = doc.get(attribute).replace(' ', '')
                    #removing size_ from attributes
                    if size_unit == 'inch':
                        inch_size[attribute] = value
                        cm_size[attribute] = CatalogIndexer.unit_conversion(attribute_value=value, factor=INCH_TO_CM_FACTOR)
                    elif size_unit == 'cm':
                        cm_size[attribute] = value
                        inch_size[attribute] = CatalogIndexer.unit_conversion(attribute_value=value, factor=CM_TO_INCH_FACTOR)
            if cm_size:
                doc['inch_size'] = inch_size
                doc['cm_size'] = cm_size

    def unit_conversion(attribute_value, factor):
        value_array = attribute_value.split('-')
        for key, value in enumerate(value_array):
            if value.isdigit():
                value_array[key] = str(round(float(value) * factor))

        val = '-'.join(value_array).replace(' ', '')
        return val

    def update_sku_product_mapping(rows):

        sku_list = []
        try:
            data_map = {}
            for row in rows:
                data_map[row['sku']] = {
                    "product_id": str(row['product_id']).strip(),
                    "parent_id": str(row['parent_id']).strip() if row['type_id'].strip() == 'simple' and row['parent_id'] else str(
                        row['product_id']).strip()
                }
            query = "UPDATE products SET "

            parent_set_query = "parent_id = (case "
            product_set_query = "product_id = (case "

            parent_id_found = False
            product_id_found = False

            for sku in data_map:
                sku_list.append(sku)
                product_id = data_map[sku]['product_id']
                parent_id = data_map[sku]['parent_id']

                if parent_id:
                    parent_id_found = True
                    parent_set_query += " when sku = '" + sku + "' then " + parent_id + " "
                if product_id:
                    product_id_found = True
                    product_set_query += " when sku = '" + sku + "' then " + product_id + " "

            parent_set_query += " end) "
            product_set_query += " end) "

            if parent_id_found and product_id_found:
                query += parent_set_query + ", " + product_set_query
            elif parent_id_found:
                query += parent_set_query
            elif product_id_found:
                query += product_set_query

            query += " WHERE sku in ( " + ','.join("'{}'".format(sku) for sku in sku_list) + ") "

            if parent_id_found or product_id_found:
                DiscUtils.mysql_write(query)
        except:
            print(traceback.format_exc())
            print("[ERROR] Failed to update product_id and parent_id for sku: {}".format(sku_list))

    def formatESDoc(doc):
        for key, value in doc.items():
            if isinstance(value, list) and value == ['']:
                doc[key] = []
    @classmethod
    def indexRecords(cls,records, search_engine, collection, skus, categoryFacetAttributesInfoMap, offersApiConfig, required_fields_from_csv, update_productids, product_2_vector_lsi_100, product_2_vector_lsi_200, product_2_vector_lsi_300,size_filter,is_first,product_history,offerbatchsize,offerswitch):
        start_time = int(round(time.time() * 1000))
        input_docs = []
        pws_fetch_products = []
        size_filter_flag = 0
        csvfile = open(cls.filepath, 'a')
        bestsellers = get_bestseller_products()
        # index_start = timeit.default_timer()
        if update_productids:
            CatalogIndexer.update_sku_product_mapping(records)

        for index, row in enumerate(records):
            try:
                CatalogIndexer.validate_catalog_feed_row(row)
                doc = {}
                CatalogIndexer.update_generic_attributes_filters(doc=doc, row=row)
                doc['size_chart_enabled'] = bool(doc.get('size_chart_enabled'))
                if doc['size_chart_enabled']:
                    doc['size_unit'] = row.get('size_unit')
                    CatalogIndexer.update_size_chart_attributes(doc=doc)
                doc['sku'] = row['sku']
                if skus and doc['sku'] not in skus:
                    continue
                doc['product_id'] = row['product_id']

                product_id = str(doc['product_id'])
                if product_2_vector_lsi_100.get(product_id):
                    doc.update(product_2_vector_lsi_100[product_id])

                if product_2_vector_lsi_200.get(product_id):
                    doc.update(product_2_vector_lsi_200[product_id])

                if product_2_vector_lsi_300.get(product_id):
                    doc.update(product_2_vector_lsi_300[product_id])

                doc['type'] = row['type_id']
                doc['hide_child_description_i'] = row['hide_child_description']
                doc['psku'] = row['parent_sku'] if doc['type'] == 'simple' and row['parent_sku'] else row['sku']
                doc['parent_id'] = row['parent_id'] if doc['type'] == 'simple' and row['parent_id'] else row[
                    'product_id']
                doc['title'] = (re.sub(r'\s+', ' ', row['name'])).strip()
                if 'with' in doc['title'].lower():
                  title = doc['title'].lower()
                  doc['title_prefix'] = title.split('with',1)[0]
                doc['title_text_split'] = row['name']
                doc['description'] = row['description']
                doc['tags'] = (row['tag'] or "").split('|')
                doc['highlights'] = (row['highlights'] or "").split('|')
                doc['featured_in_titles'] = (row['featured_in_titles'] or "").split('|')
                doc['featured_in_urls'] = (row['featured_in_urls'] or "").split('|')
                doc['star_rating_count'] = int(row['rating'] or 0)
                if row['rating_num']:
                    doc['star_rating'] = row['rating_num']
                    doc['star_rating_percentage'] = int(row['rating_num']*20)
                doc['review_count'] = row['review_count'] or 0
                doc['qna_count'] = row['qna_count'] or 0
                if row['product_expiry']:
                    doc['expiry_date'] = dateparser.parse(row['product_expiry'],
                                                          ['%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M:%S']).strftime(
                        '%Y-%m-%dT%H:%M:%SZ')
                doc['is_service'] = row.get('d_sku') == '1'
                if (row['d_sku'] == '2'):
                    doc['d_sku_s'] = 'giftcard'
                elif (row['d_sku'] == '1'):
                    doc['d_sku_s'] = 'beauty_service'
                else:
                    doc['d_sku_s'] = 'none'
                doc['pack_size'] = row['pack_size']
                doc['is_luxe'] = row.get('is_luxe') == '1'
                doc['can_subscribe'] = row.get('is_subscribable') == '1'
                if doc['can_subscribe']:
                    doc['bucket_discount_percent'] = row['bucket_discount_percent']
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
                doc['catalog_tag'] = row['catalog_tag'].split('|')
                try:
                    if False:
                        set_clause_arr = []
                        if doc.get('parent_id'):
                            set_clause_arr.append("parent_id='%s'" % doc.get('parent_id'))
                        if doc.get('product_id'):
                            set_clause_arr.append("product_id='%s'" % doc.get('product_id'))
                        if set_clause_arr:
                            set_clause = " set " + ", ".join(set_clause_arr)
                            query = "update products {set_clause} where sku ='{sku}' ".format(set_clause=set_clause, sku=doc['sku'])
                            DiscUtils.mysql_write(query)
                except:
                    print(traceback.format_exc())
                    print("[ERROR] Failed to update product_id and parent_id for sku: %s" % doc['sku'])
                    pass
                # Popularity
                popularity_obj = get_popularity_for_id(product_id=doc['product_id'], parent_id=doc['parent_id'])
                popularity_obj = popularity_obj.get(doc['product_id'])
                doc['popularity'] = 0
                doc['viewcount_i'] = 0
                doc['popularity_recent'] = 0
                if popularity_obj:
                    doc['popularity'] = popularity_obj['popularity']
                    # View based Popularity
                    doc['viewcount_i'] = popularity_obj.get('views', 0)
                    doc['popularity_recent'] = popularity_obj.get('popularity_recent', 0)

                # Product URL and slug
                product_url = row['product_url']
                if product_url:
                    doc['product_url'] = product_url
                    slug_initial_pos = product_url.find('.com/') + 5
                    slug_end_pos = product_url.find('?') if product_url.find('?') != -1 else len(product_url)
                    doc['slug'] = product_url[slug_initial_pos: slug_end_pos]

                    # Category related
                category_ids = (row['category_id'] or "").split('|') if row['category_id'] else []
                category_names = (row['category'] or "").split('|') if row['category'] else []
                if category_ids and len(category_ids) == len(category_names):
                    doc['category_ids'] = category_ids
                    doc['category_values'] = category_names
                    doc['category_facet'] = []
                    for category_id in category_ids:
                        info = categoryFacetAttributesInfoMap.get(str(category_id))
                        if info:
                            cat_facet = OrderedDict()
                            for key in ['category_id', 'name', 'rgt', 'lft', 'depth', 'include_in_menu', 'parent_id',
                                        'position']:
                                cat_facet[key] = str(info.get(key))
                            doc['category_facet'].append(cat_facet)
                    doc['category_facet_searchable'] = " ".join([x['name'] for x in doc['category_facet'] if 'nykaa' not in x['name'].lower()]) or ""
                    valid_category_value_list = ["parcos", "the men universe", "the women universe", "the art of living"]
                    doc['category_facet_searchable'] += " " + " ".join(
                        [x for x in doc['category_values'] if any(word in x.lower() for word in valid_category_value_list)])

                elif len(category_ids) != len(category_names):
                    # with open("/data/inconsistent_cat.txt", "a") as f:
                    #  f.write("%s\n"%doc['sku'])
                    print('inconsistent category values for %s' % doc['sku'])

                if row.get('seller_name'):
                    doc['seller_name'] = row['seller_name']
                    if row.get('seller_rating'):
                        doc['seller_rating'] = row['seller_rating']

                # media stuff
                doc['media'] = []
                main_image = row['main_image']
                main_image_path = urlparse(main_image).path
                images = (row['image_url'] or "").split('|') if row['image_url'] else []
                if main_image and images:
                    for image in images:
                        image = image.strip()
                        image_path = urlparse(image).path
                        if image_path != main_image_path:
                            doc['media'].append({'type': 'image', 'url': image})
                    doc['media'].insert(0, {'type': 'image', 'url': main_image})

                video = row.get('video')
                if video:
                    doc['media'].append({'type': 'video', 'url': video})

                # Primary Categories
                doc['primary_categories'] = []
                l1 = {}
                l2 = {}
                l3 = {}
                if row['primary_categories'] == "":
                    doc['primary_categories'].append(
                        {"l1": {'id': None, 'name': None}, "l2": {'id': None, 'name': None},
                         "l3": {'id': None, 'name': None}})
                else:
                    primary_categories = row['primary_categories'].split('|')
                    if primary_categories[0] != "":
                        l1['id'] = primary_categories[0]
                        # Do not send expiry date in case of appliance category
                        if l1.get('id') == APPLIANCE_MAIN_CATEGORY_ID:
                            doc['expiry_date'] = None
                        if category_ids and len(category_ids) == len(category_names):
                            if primary_categories[0] in category_ids:
                                cat_index = category_ids.index(primary_categories[0])
                                l1['name'] = category_names[cat_index]
                            else:
                                l1['name'] = None
                        else:
                            l1['name'] = None
                    else:
                        l1['id'] = None
                        l1['name'] = None

                    if primary_categories[1] != "":
                        l2['id'] = primary_categories[1]
                        if category_ids and len(category_ids) == len(category_names):
                            if primary_categories[1] in category_ids:
                                cat_index = category_ids.index(primary_categories[1])
                                l2['name'] = category_names[cat_index]
                            else:
                                l2['name'] = None
                        else:
                            l2['name'] = None
                    else:
                        l2['id'] = None
                        l2['name'] = None

                    if primary_categories[2] != "":
                        l3['id'] = primary_categories[2]
                        if category_ids and len(category_ids) == len(category_names):
                            if primary_categories[2] in category_ids:
                                cat_index = category_ids.index(primary_categories[2])
                                l3['name'] = category_names[cat_index]
                            else:
                                l3['name'] = None
                        else:
                            l3['name'] = None

                    else:
                        l3['id'] = None
                        l3['name'] = None

                    doc['primary_categories'].append({"l1": l1, "l2": l2, "l3": l3})

                size_filter_flag = 0
                if category_ids:
                    size_filter_cat_ids = size_filter['category_ids'].split(',')
                    diff = list(set(category_ids) & set(size_filter_cat_ids))
                    if len(diff)>0 and size_filter['status'] == '1':
                        size_filter_flag = 1

                # variant stuff
                if doc['type'] == 'configurable':
                    variant_type = row['variant_type']
                    variant_skus = (row['parent_sku'] or "").split('|') if row['parent_sku'] else []
                    variant_ids = (row['parent_id'] or "").split('|') if row['parent_id'] else []
                    variant_name_key = ''
                    variant_icon_key = ''
                    variant_attr_id_key = ''
                    if variant_type == 'shade':
                        variant_name_key = 'shade_name'
                        variant_icon_key = 'variant_icon'
                        variant_attr_id_key = 'shade_id'
                    elif variant_type == 'size':
                        variant_name_key = 'size'
                        variant_attr_id_key = 'size_id'

                    variant_names = (row[variant_name_key] or "").split('|') if variant_name_key and row[variant_name_key] else []
                    variant_icons = (row[variant_icon_key] or "").split('|') if variant_icon_key and row[variant_icon_key] else []
                    variant_attr_ids = (row[variant_attr_id_key] or "").split('|') if variant_attr_id_key and row[variant_attr_id_key] else []
                    try:
                        if variant_type and variant_skus and len(variant_skus) == len(variant_names) and len(variant_skus) == len(variant_ids) and len(variant_skus) == len(variant_attr_ids):
                            if variant_icons and len(variant_icons) != len(variant_skus):
                                variant_icons = []
                            variants_arr = []
                            if not variant_icons:
                                variant_icons = [""] * len(variant_skus)
                            for i, sku in enumerate(variant_skus):
                                variants_arr.append({'sku': variant_skus[i], 'id': variant_ids[i], 'name': variant_names[i],'icon': variant_icons[i], 'variant_id': variant_attr_ids[i]})
                            doc['variants'] = {variant_type: variants_arr}
                    except:
                        print("[ERROR] variants field not formed for SKU: %s" % doc['sku'])
                        pass
                    doc['variant_type'] = variant_type
                    doc['option_count'] = len(variant_skus)
                elif doc['type'] == 'simple':
                    variant_type = row['variant_type']
                    variant_name = row['shade_name'] or row['size']
                    variant_attr_id = row['shade_id'] or row['size_id']
                    variant_icon = row['variant_icon']
                    if variant_name:
                        doc['variant_type'] = variant_type
                        doc['variant_name'] = variant_name
                        if variant_icon:
                            doc['variant_icon'] = variant_icon
                        if variant_attr_id:
                            doc['variant_id_i'] = variant_attr_id
                elif doc['type'] == 'bundle':
                    doc['product_ids'] = (row['parent_id'] or "").split('|') if row['parent_id'] else []
                    doc['product_skus'] = (row['parent_sku'] or "").split('|') if row['parent_sku'] else []

                # Price and availability
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
                    pws_fetch_products.append(params)

                # offers stuff
                offer_ids = row['offer_id'].split("|") if row['offer_id'] else []
                offer_names = row['offer_name'].split("|") if row['offer_name'] else []
                offer_descriptions = row['offer_description'].split("|") if row['offer_description'] else []
                doc['offers'] = []
                if offer_ids and len(offer_ids) == len(offer_names) and len(offer_ids) == len(offer_descriptions):
                    doc['offer_ids'] = offer_ids
                    doc['offer_facet'] = []
                    for i, offer_id in enumerate(offer_ids):
                        doc['offers'].append(
                            {'id': offer_ids[i], 'name': offer_names[i], 'description': offer_descriptions[i]})
                        offer_facet = OrderedDict()
                        offer_facet['id'] = offer_ids[i]
                        offer_facet['name'] = offer_names[i]
                        doc['offer_facet'].append(offer_facet)
                # elif offer_ids:
                # with open("/data/inconsistent_offers.txt", "a") as f:
                #  f.write("%s\n"%doc['sku'])
                # print('inconsistent offer values for %s'%doc['sku'])
                doc['offer_count'] = len(doc['offers'])
                # print(doc['offers'])
                new_offer_status = 0
                if offersApiConfig and offersApiConfig['status']:
                    if offersApiConfig['status'] == 1 and offersApiConfig['product_ids'] and len(
                            offersApiConfig['product_ids']) > 0:
                        if doc['product_id'] in offersApiConfig['product_ids']:
                            new_offer_status = 1
                    elif offersApiConfig['status'] == 2:
                        new_offer_status = 1
                    else:
                        new_offer_status = 0
                else:
                    new_offer_status = 0

                if new_offer_status == 1:
                    # New offer Json
                    doc['offers'] = []
                    doc['offer_ids'] = []
                    doc['offer_facet'] = []
                    doc['nykaaman_offers'] = []
                    doc['nykaaman_offer_ids'] = []
                    doc['nykaaman_offer_facet'] = []
                    doc['nykaa_pro_offers'] = []
                    doc['nykaa_pro_offer_ids'] = []
                    doc['nykaa_pro_offer_facet'] = []

                    try:
                        if row['offers']:
                            product_offers = row['offers'].replace("\\\\", "\\")
                            product = {}
                            product['offers'] = ast.literal_eval(product_offers)
                            for i in product['offers']:
                                prefix = i
                                if product['offers'][prefix]:
                                    for i in product['offers'][prefix]:
                                        if prefix == 'nykaa':
                                            key = 'offer_facet'
                                        else:
                                            key = prefix + '_offer_facet'
                                        doc['key'] = []
                                        key = OrderedDict()
                                        key['id'] = i['id']
                                        key['name'] = i['name']
                                        key['offer_start_date'] = i['offer_start_date']
                                        key['offer_end_date'] = i['offer_end_date']
                                        if 'customer_group' in i:
                                          key['customer_group'] = i['customer_group']
                                        doc['key'].append(key)
                                        if prefix == 'nykaa':
                                            doc['offer_ids'].append(i['id'])
                                        else:
                                            doc.setdefault(prefix + '_offer_ids', []).append(i['id'])

                                    product['offers'][prefix] = sorted(product['offers'][prefix], key=itemgetter('priority'), reverse=True)
                                    if prefix == 'nykaa':
                                        doc['offers'] = product['offers'][prefix]
                                    else:
                                        doc[prefix + '_offers'] = product['offers'][prefix]
                    except Exception as e:
                        print(traceback.format_exc())
                    doc['offer_count'] = len(doc['offers'])
                    doc['nykaaman_offer_count'] = len(doc['nykaaman_offers'])
                    doc['nykaa_pro_offer_count'] = len(doc['nykaa_pro_offers'])

                # facets: dynamic fields
                facet_fields = [field for field in required_fields_from_csv if field.endswith("_v1") or (field == 'size_id' and size_filter_flag == 1)]
                for field in facet_fields:
                    field_prefix = field.rsplit('_', 1)[0]
                    facet_ids = (row[field] or "").split('|') if row[field] else []
                    facet_values = (row[field_prefix] or "").split('|') if row[field_prefix] else []
                    if facet_ids and len(facet_ids) == len(facet_values):
                        doc[field_prefix + '_ids'] = facet_ids
                        doc[field_prefix + '_values'] = facet_values
                        facets = []
                        if field_prefix in ['brand', 'old_brand', 'size']:
                            for i, brand_id in enumerate(facet_ids):
                                brand_facet = OrderedDict()
                                brand_facet['id'] = brand_id
                                brand_facet['name'] = facet_values[i]
                                facets.append(brand_facet)
                        else:
                            option_attrs = PipelineUtils.getOptionAttributes(facet_ids)
                            for attr_id, attrs in option_attrs.items():
                                other_facet = OrderedDict()
                                other_facet['id'] = attrs['id']
                                other_facet['name'] = attrs['name']
                                if attrs.get('color_code'):
                                    other_facet['color_code'] = attrs['color_code']
                                facets.append(other_facet)
                        doc[field_prefix + '_facet'] = facets
                    # elif len(facet_ids) != len(facet_values):
                    #  with open("/data/inconsistent_facet.txt", "a") as f:
                    #    f.write("%s  %s\n"%(doc['sku'], field))

                doc['brand_facet_searchable'] = " ".join([x['name'] for x in doc.get('brand_facet', [])]) or ""
                if not doc['brand_facet_searchable']:
                    doc['brand_facet_searchable'] = " ".join([x['name'] for x in doc.get('old_brand_facet', [])]) or ""

                for key, value in CatalogIndexer.replace_brand_dict.items():
                    if key in doc.get("brand_facet_searchable", "").lower():
                        doc['brand_facet_searchable'] = doc['brand_facet_searchable'].lower().replace(key, value)

                # meta info: dynamic fields
                meta_fields = [field for field in row.keys() if field.startswith("meta_")]
                for field in meta_fields:
                    doc[field] = row.get(field, "")

                doc['list_offer_ids'] = (row['list_offer_id'] or "").split('|')
                doc['max_allowed_qty_i'] = row['max_allowed_qty'] or 5
                doc['bulkbuyer_max_allowed_qty_i'] = row['bulkbuyer_max_allowed_qty'] or 0
                doc['is_free_sample_i'] = row['is_free_sample'] or 0
                doc['pro_flag_i'] = row['pro_flag'] or 0
                doc['pro_disclaimer_s'] = row['pro_disclaimer']
                # doc['is_kit_combo_i'] = row['is_kit_combo'] or 0
                if 'pro_disclaimer' in row:
                  doc['pro_disclaimer_s'] = row['pro_disclaimer']
                else:
                  doc['pro_disclaimer_s'] = ''
                if 'is_searchable' in row:
                  doc['is_searchable_i'] = row['is_searchable'] or 0
                else:
                  doc['is_searchable_i'] = 0
                doc['update_time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
                doc['create_time'] = row['created_at']
                doc['object_type'] = "product"
                doc['top_reviews'] = row.get('top_reviews', '')
                doc['review_splitup'] = row.get('review_splitup', '')
                doc['days_to_return'] = row.get('days_to_return')
                doc['message_on_return'] = row.get('message_on_return')
                doc['return_available'] = row.get('return_available')
                doc['country_code'] = row.get('country_code')
                doc['country_name'] = row.get('country_name')
                if doc.get('type') == 'simple' or doc.get('type') == 'configurable':
                    doc['manufacturer_name'] = row.get('manufacturer_name', '')
                    doc['manufacturer_address'] = row.get('manufacturer_address', '')
                    doc['country_of_manufacture'] = row.get('country_of_manufacture')

                for k, v in doc.items():
                    for pattern, _type in CatalogIndexer.field_type_pattens.items():
                        if re.search(pattern, k):
                            doc[k] = _type(v)
                    if v == ['']:
                        doc[k] = None
                    if not v and v != False:
                        doc[k] = None

                try:
                    brand = doc.get("brand_facet_searchable", "") or ""
                    if not brand: 
                      print("ERROR ... Could not extract brand for product_id: %s" % doc['product_id'])
                    title_searchable = row.get('name', "")
                    doc['title_brand_category'] = " ".join([x for x in [title_searchable, doc.get("brand_facet_searchable", ""),doc.get("category_facet_searchable", "")] if x])
                except:
                    print(traceback.format_exc())
                    print("product_id: %s " % doc['product_id'])

                for key, value in CatalogIndexer.final_replace_dict.items():
                    pattern = '\\b' + key + '\\b'
                    doc['title_brand_category'] = re.sub(pattern, value, doc['title_brand_category'].lower())
                    
                for facet in ['color_facet', 'finish_facet', 'formulation_facet', 'benefits_facet', 'skin_tone_facet', 'spf_facet',
                        'concern_facet', 'coverage_facet', 'gender_facet', 'skin_type_facet', 'hair_type_facet', 'preference_facet']:
                    try:
                        doc['title_brand_category'] += " " + " ".join([x['name'] for x in doc[facet]]) 
                    except:
                        pass

                product_id = doc['product_id']
                if product_history and (product_id in product_history) and product_history[product_id]:
                  doc['views_in_last_month'] = product_history[product_id]['views_in_last_month']
                  doc['orders_in_last_month'] = product_history[product_id]['orders_in_last_month']
                  doc['revenue_in_last_month'] = product_history[product_id]['revenue_in_last_month']
                  doc['units_sold_in_last_month'] = product_history[product_id]['units_sold_in_last_month']
                  doc['cart_additions_in_last_month'] = product_history[product_id]['cart_additions_in_last_month']
                
                if doc.get('type', '') == 'bundle':
                    doc['title_brand_category'] += " " + "combo"

                doc['visible_after_color_filter_i'] = 1
                if doc.get('color_facet', '') and doc.get('size_facet', ''):
                    if doc.get('type', '') != 'configurable':
                        doc['visible_after_color_filter_i'] = 0
                elif doc.get('type', '') != 'simple':
                    doc['visible_after_color_filter_i'] = 0
                doc['custom_tags'] = []
                if doc['product_id'] in bestsellers:
                    doc['custom_tags'].append('BESTSELLER')
                golive_time = doc.get('product_enable_time', 0)
                today = dt.datetime.combine(dt.datetime.today(), dt.time.min)
                startdate = today - dt.timedelta(days=30)
                if golive_time and (str(golive_time) >= str(startdate)):
                    doc['custom_tags'].append('NEW')
                
                if search_engine == 'elasticsearch':
                    CatalogIndexer.formatESDoc(doc)

                input_docs.append(doc)
                csv_columns=[]
                for key in doc:
	                csv_columns.append(key)
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                if is_first:
                  writer.writeheader()
                  is_first = False
                writer.writerow(doc)
            except Exception as e:
                print(traceback.format_exc())
                print("Error with %s: %s" % (row['sku'], str(e)))


        # index_stop = timeit.default_timer()
        # index_duration = index_stop - index_start
        # print("for loop time: %s seconds." % (time.strftime("%M min %S seconds", time.gmtime(index_duration))))
        if input_docs:
            # index_start = timeit.default_timer()
            (input_docs, errors) = CatalogIndexer.fetch_price_availability(input_docs, pws_fetch_products)
            # index_stop = timeit.default_timer()
            # index_duration = index_stop - index_start
            # print("fetch_price_availability time: %s seconds." % (
            #     time.strftime("%M min %S seconds", time.gmtime(index_duration))))
            if offerswitch:
                CatalogIndexer.fetch_offers(input_docs, offerbatchsize)
            # index elastic search
            if search_engine == 'elasticsearch':
                # index_start = timeit.default_timer()
                CatalogIndexer.indexES(input_docs, collection)
                # index_stop = timeit.default_timer()
                # index_duration = index_stop - index_start
                # print("indexES time: %s seconds" % time.strftime("%M min %S seconds", time.gmtime(index_duration)))
                
            CatalogIndexer.print_errors(errors)

        end_time = int(round(time.time() * 1000))
        print("index record sync time : {} ms".format(end_time - start_time))

    def index(search_engine, file_path, collection, update_productids=False, limit=0, skus=None, offerbatchsize=1000, offerswitch=False):
        skus = skus or []
        skus = [x for x in skus if x]
        if skus:
            print("Running Index Catalog for selected skus: %s" % skus)
        validate_popularity_data_health()

        required_fields_from_csv = ['sku', 'parent_sku', 'product_id', 'type_id', 'name', 'description', 'product_url', 'price', 'special_price', 'discount', 'is_in_stock',
        'pack_size', 'tag', 'rating', 'rating_num', 'review_count', 'qna_count', 'try_it_on', 'image_url', 'main_image', 'shade_name', 'variant_icon', 'size','size_id',
        'variant_type', 'offer_name', 'offer_id', 'product_expiry', 'created_at', 'category_id', 'category', 'brand_v1', 'brand', 'shop_the_look_product', 'style_divas',
        'visibility', 'gender_v1', 'gender', 'color_v1', 'color', 'concern_v1', 'concern', 'finish_v1', 'finish', 'formulation_v1', 'formulation', 'try_it_on_type',
        'hair_type_v1', 'hair_type', 'benefits_v1', 'benefits', 'skin_tone_v1', 'skin_tone', 'skin_type_v1', 'skin_type', 'coverage_v1', 'coverage', 'preference_v1',
        'preference', 'spf_v1', 'spf', 'add_to_cart_url', 'parent_id', 'redirect_to_parent', 'eretailer', 'product_ingredients', 'vendor_id', 'vendor_sku', 'old_brand_v1',
        'old_brand', 'highlights', 'featured_in_titles', 'featured_in_urls', 'is_subscribable', 'bucket_discount_percent','list_offer_id', 'max_allowed_qty', 'beauty_partner_v1',
        'beauty_partner', 'primary_categories', 'offers', 'filter_size_v1', 'filter_size', 'speciality_search_v1', 'speciality_search', 'filter_product_v1', 'filter_product', 'usage_period_v1', 'usage_period',
        'hide_child_description','country_code','country_name']

        print("Reading CSV .. ")
        all_rows = read_csv_from_file(file_path)
        print("Processing CSV .. ")
        columns = all_rows[0].keys()
        PipelineUtils.check_required_fields(columns, required_fields_from_csv)
        if skus:
          all_rows = [x for x in all_rows if x['sku'] in skus] 
        count = len(all_rows)
        numpy_count = int(count / RECORD_GROUP_SIZE) or 1
        input_docs = []
        pws_fetch_products = []
        categoryFacetAttributesInfoMap = CatalogIndexer.getCategoryFacetAttributesMap()
        offersApiConfig = CatalogIndexer.getOffersApiConfig()

        size_filter = CatalogIndexer.getSizeFilterConfig()

        #product_2_vector_lsi_100 = {}
        #product_2_vector_lsi_200 = {}
        #product_2_vector_lsi_300 = {}
        print("Fetching embedding vectors for products")
        index_check_results = DiscUtils.fetchResults(DiscUtils.mlMysqlConnection('r'), 'SHOW INDEXES FROM embedding_vectors')
        if len(list(filter(lambda x: x['Key_name'] == 'embedding_vector_scroll', index_check_results))) == 0:
            raise Exception('Index embedding_vector_scroll is not present in ml db')
        product_2_vector_lsi_100 = {doc['product_id']: doc for doc in get_vectors_from_mysql_for_es(DiscUtils.mlMysqlConnection('r'), 'lsi_100', False)}
        product_2_vector_lsi_200 = {doc['product_id']: doc for doc in get_vectors_from_mysql_for_es(DiscUtils.mlMysqlConnection('r'), 'lsi_200', False)}
        product_2_vector_lsi_300 = {doc['product_id']: doc for doc in get_vectors_from_mysql_for_es(DiscUtils.mlMysqlConnection('r'), 'lsi_300', False)}

        ctr = LoopCounter(name='Indexing %s' % search_engine, total=len(all_rows))
        q = queue.Queue(maxsize=0)

        searchable_info = defaultdict(list)
        for index, row in enumerate(all_rows):
          psku = row['parent_sku']
          sku = row['sku']
          if psku == sku:
            continue
          name_arr = row['name'].split(" ")
          for word in row['name'].split(" "):
            if word and word not in searchable_info[psku]:
              searchable_info[psku].append(word)

        for index, row in enumerate(all_rows):
          psku = row['parent_sku']
          sku = row['sku']
          title_searchable = row['name'].split(" ")
          if sku in searchable_info:
            for word in searchable_info[sku]:
              if word and word not in searchable_info[psku]:
                title_searchable.append(word)

          all_rows[index]['title_searchable'] = " ".join(title_searchable)

        print("Number of rows to process from csv: %s" % len(all_rows))
        chunks = numpy.array_split(numpy.array(all_rows), numpy_count)
        print("Number of chunks created from csv: %s" % len(chunks))
        for index, row in enumerate(chunks):
            if limit and ctr.count == limit:
                break
            ctr += 1
            # if ctr.should_print():
            #     print(ctr.summary)
            q.put_nowait(row)

        for _ in range(NUMBER_OF_THREADS):
            Worker(q, search_engine, collection, skus, categoryFacetAttributesInfoMap, offersApiConfig, required_fields_from_csv,
                   update_productids, product_2_vector_lsi_100, product_2_vector_lsi_200, product_2_vector_lsi_300,size_filter,offerbatchsize,offerswitch).start()
        q.join()
        print("Index Catalog Finished!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--filepath", required=True, help='path to csv file')
    parser.add_argument("-c", "--collection", help='name of collection to index to')
    parser.add_argument("-s", "--searchengine", default='elasticsearch')
    parser.add_argument("--update_productids", action='store_true', help='Adds product_id and parent_id to products table')
    parser.add_argument("--sku", type=str, default="")
    parser.add_argument("-b", "--offerbatchsize", default=1000, help='size of offer docs batch', type=int)
    parser.add_argument("-w", "--offerswitch", default=False, help='switch for fetch_offers function', type=bool)
    argv = vars(parser.parse_args())
    file_path = argv['filepath']
    collection = argv['collection']
    searchengine = argv['searchengine']
    offerbatchsize = argv['offerbatchsize']
    offerswitch = argv['offerswitch']
    argv['update_productids'] = True
    if argv['sku']:
      NUMBER_OF_THREADS = 1
    CatalogIndexer.index(searchengine, file_path, collection, update_productids=argv['update_productids'],
                         skus=argv['sku'].split(","), offerbatchsize=offerbatchsize, offerswitch=offerswitch)

