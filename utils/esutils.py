import traceback
import psycopg2
from elasticsearch import helpers, Elasticsearch

import sys
sys.path.append('/home/ubuntu/nykaa_scripts/utils')
sys.path.append('/home/hadoop/nykaa_scripts/utils')
from recoutils import RecoUtils

env_details = RecoUtils.get_env_details()

class ESUtils:

    def esConn():
        if env_details['env'] == 'prod':
            ES_ENDPOINT = 'vpc-prod-priceapi-u22xdmuudgqkzoiiufyezkmrkq.ap-south-1.es.amazonaws.com'
        elif env_details['env'] == 'non_prod':
            ES_ENDPOINT = 'vpc-preprod-priceapi-plwkmnbdhm6td5itzwxznsfn44.ap-south-1.es.amazonaws.com'
        else:
            raise Exception('Unknown env')
        print(ES_ENDPOINT)
        es = Elasticsearch(['http://%s:80' % ES_ENDPOINT], timeout=120)
        return es

    def scrollESForResults():
        es_conn = ESUtils.esConn()
        ES_BATCH_SIZE = 10000
        scroll_id = None
        luxe_products = []
        product_2_mrp = {}
        child_2_parent = {}
        primary_categories = {}
        brand_facets = {}
        sku_2_product_id = {}
        product_2_title = {}
        product_2_image = {}
        product_in_stock = {}

        while True:
            if not scroll_id:
                query = { 
                    "size": ES_BATCH_SIZE,
                    "query": { "match_all": {} },
                    "_source": ["product_id", "is_luxe", "mrp", "parent_id", "primary_categories", "brand_facet", "sku", "media", "title_text_split", "in_stock"]
                }
                response = es_conn.search(index='livecore', body=query, scroll='15m')
            else:
                response = es_conn.scroll(scroll_id=scroll_id, scroll='15m')

            if not response['hits']['hits']:
                break

            scroll_id = response['_scroll_id']
            luxe_products += [int(p['_source']['product_id']) for p in response['hits']['hits'] if p["_source"].get("is_luxe") and p["_source"]["is_luxe"]]
            product_2_mrp.update({int(p["_source"]["product_id"]): p["_source"]["mrp"] for p in response["hits"]["hits"] if p["_source"].get("mrp")})
            child_2_parent.update({int(p["_source"]["product_id"]): int(p["_source"]["parent_id"]) for p in response["hits"]["hits"] if p["_source"].get("parent_id")})
            primary_categories.update({int(p["_source"]["product_id"]): p["_source"]["primary_categories"] for p in response["hits"]["hits"] if p["_source"].get("primary_categories")})
            brand_facets.update({int(p["_source"]["product_id"]): p["_source"].get("brand_facet") for p in response["hits"]["hits"] if p["_source"].get("brand_facet")})
            sku_2_product_id.update({p["_source"]["sku"]: int(p["_source"]["product_id"]) for p in response["hits"]["hits"] if p["_source"].get("sku")})
            product_2_image.update({int(p["_source"]["product_id"]): p['_source']['media'] for p in response['hits']['hits'] if p['_source'].get('media')})
            product_2_title.update({int(p["_source"]["product_id"]): p['_source']['title_text_split'] for p in response['hits']['hits']})
            product_in_stock.update({int(p["_source"]["product_id"]): p['_source']['in_stock'] for p in response['hits']['hits']})

        return {'luxe_products': luxe_products, 'product_2_mrp': product_2_mrp, 'child_2_parent': child_2_parent, 'primary_categories': primary_categories, 'brand_facets': brand_facets, 'sku_2_product_id': sku_2_product_id, 'product_2_image': product_2_image, 'product_2_title': product_2_title, 'product_in_stock': product_in_stock}
