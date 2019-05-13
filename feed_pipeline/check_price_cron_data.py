import sys
sys.path.append('/var/www/pds_api/')
import argparse
import json
import requests
import urllib.parse
import urllib.request
from pipelineUtils import PipelineUtils
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

def getDataFromDb(skus):

    final_products_to_update = []
    for key,doc in enumerate(skus):
        final_products_to_update.append({'sku': skus[doc]['sku'], 'type': skus[doc]['type']})

    params = json.dumps({"products": final_products_to_update}).encode('utf8')
    req = urllib.request.Request("http://" + PipelineUtils.getAPIHost() + "/apis/v2/pas.get", data=params,
                                 headers={'content-type': 'application/json'})
    pas_object = json.loads(urllib.request.urlopen(req, params).read().decode('utf-8')).get('skus', {})
    return pas_object

def getDataFromES(skus, size, sort_limit):
    querydsl = {}
    docs = []
    response = {}
    querydsl['sort'] = {'_id': 'asc'}
    if sort_limit is not None:
        querydsl['search_after'] = sort_limit
    if skus:
        product_skus = [product.upper() for product in skus]
        if product_skus:
            sku_should_query = []
            for sku in product_skus:
                sku_should_query.append({'term': {'sku.keyword': sku}})
            querydsl['query'] = {'bool': {'should': sku_should_query}}
            querydsl['_source'] = ['sku', 'type', 'mrp', 'price', 'discount', 'update_time','type']
            querydsl['size'] = len(product_skus) + 1
    else:
        querydsl['_source'] = ['sku', 'mrp', 'price', 'discount','type']
        querydsl['size'] = size
        querydsl['from'] = -1
    esResponse = DiscUtils.makeESRequest(querydsl, index='livecore')
    docs = esResponse['hits']['hits']
    for single_doc in docs:
        sku_unit = single_doc['_source']['sku']
        response[sku_unit] = single_doc['_source']
        sort_values = single_doc['sort']
    if not response:
        print("No data found in ES")
    return (response, sort_values)

def getESTotalCount(indexName):
    body = {"query": {"match_all": {}}, "size": 0}
    return DiscUtils.makeESRequest(body, indexName)['hits']['total']

def compareData(skus, batch_limit, upto):
    count = 0
    if upto:
        totalDocs = int(upto)
    else:
        totalDocs = getESTotalCount('livecore')
    sort_limit = None
    while count < totalDocs:
        esData, sort_limit = getDataFromES(skus, batch_limit, sort_limit)
        if esData:
            db_data = getDataFromDb(esData)
            for singleDbRecord in db_data:
                dbPrice = db_data[singleDbRecord]['sp']
                dbDiscount = db_data[singleDbRecord]['discount']
                esPrice = esData[singleDbRecord]['price']
                esDiscount = esData[singleDbRecord]['discount']
                if (dbPrice != esPrice or dbDiscount != esDiscount) and dbPrice is not None and dbDiscount is not None and esPrice is not None and esDiscount is not None:
#                if dbPrice != esPrice or dbDiscount != esDiscount:
                    if dbDiscount == esDiscount and abs(dbPrice - esPrice) <= 1:
                        continue
                    print(":( Not matching sku:%s ---- db_price:%s ------es_price:%s------db_discount:%s------es_discount:%s" %(singleDbRecord, dbPrice, esPrice, dbDiscount, esDiscount))
#                else:
 #                   print("Yay...matching sku:%s ---- db_price:%s ------es_price:%s------db_discount:%s------es_discount:%s" %(singleDbRecord, dbPrice, esPrice, dbDiscount, esDiscount))
        count = count + int(batch_limit)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skus", help='skus which needs tobe checked')
    parser.add_argument("--batch_limit", help='limit of records in single batch')
    parser.add_argument("--upto", help='restrict number of records instead of whole es count')
    argv = vars(parser.parse_args())
    skus = argv.get('skus', None)
    batch_limit = argv.get('batch_limit', None)
    upto = argv.get('upto', None)
    if skus:
        skus = skus.split(",")
    if not batch_limit:
        batch_limit = 500
    compareData(skus, batch_limit, upto)
