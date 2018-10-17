import sys
sys.path.append('/home/apis/nykaa/')
from pas.v2.utils import Utils, MemcacheUtils, CATALOG_COLLECTION_ALIAS
import argparse


def getDataFromDb(skus):
    mysql_conn = Utils.mysqlConnection('r')
    sku_string = "','".join(skus)

    query = """SELECT sku, type, mrp, 
                CASE 
                    WHEN CURRENT_TIMESTAMP() BETWEEN schedule_start AND schedule_end AND scheduled_discount IS NOT NULL
                        THEN scheduled_discount 
                    ELSE discount 
                END AS 'discount', 
                CASE 
                    WHEN CURRENT_TIMESTAMP() BETWEEN schedule_start AND schedule_end AND scheduled_discount IS NOT NULL 
                        THEN ROUND(mrp - ((scheduled_discount/100)*mrp)) 
                    WHEN CURRENT_TIMESTAMP() NOT BETWEEN schedule_start AND schedule_end AND discount IS NOT NULL 
                        THEN ROUND(mrp - ((discount/100)*mrp)) 
                    ELSE sp 
                END as 'price' 
            FROM products WHERE sku IN('"""+ sku_string +"')"

    results = Utils.fetchResults(mysql_conn, query)
    mysql_conn.close()
    return results

def getDataFromES(skus, offset, size):
    querydsl = {}
    docs = []
    response = {}
    if skus:
        product_skus = [product.upper() for product in skus]
        if product_skus:
            sku_should_query = []
            for sku in product_skus:
                sku_should_query.append({'term': {'sku.keyword': sku}})
            querydsl['query'] = {'bool': {'should': sku_should_query}}
            querydsl['_source'] = ['sku', 'type', 'mrp', 'price', 'discount', 'update_time']
            querydsl['size'] = len(product_skus) + 1
    else:
        querydsl['_source'] = ['sku', 'mrp', 'price', 'discount']
        querydsl['size'] = size
        querydsl['from'] = offset

    esResponse = Utils.makeESRequest(querydsl, index='livecore')
    docs = esResponse['hits']['hits']
    for single_doc in docs:
        sku_unit = single_doc['_source']['sku']
        response[sku_unit] = single_doc['_source']
    if not response:
        print("No data found in ES")
    return response

def getESTotalCount(indexName):
    body = {"query": {"match_all": {}}, "size": 0}
    return Utils.makeESRequest(body, indexName)['hits']['total']

def compareData(skus, batch_limit, limitEsRecords):
    count = 0
    if limitEsRecords:
        totalDocs = int(limitEsRecords)
    else:
        totalDocs = getESTotalCount('livecore')

    while count < totalDocs:
        esData = getDataFromES(skus, count, batch_limit)
        if esData:
            sku_list = list(esData.keys())
            db_data = getDataFromDb(sku_list)
            for singleDbRecord in db_data:
                dbPrice = singleDbRecord['price']
                dbDiscount = singleDbRecord['discount']
                esPrice = esData[singleDbRecord['sku']]['price']
                esDiscount = esData[singleDbRecord['sku']]['discount']
                if dbPrice != esPrice or dbDiscount != esDiscount:
                    print(":( Not matching sku:%s ---- db_price:%s ------es_price:%s------db_discount:%s------es_discount:%s" %(singleDbRecord['sku'], dbPrice, esPrice, dbDiscount, esDiscount))
                # else:
                #     pass
                    #print("Yay...matching sku:%s ---- db_price:%s ------es_price:%s------db_discount:%s------es_discount:%s" %(singleDbRecord['sku'], dbPrice, esPrice, dbDiscount, esDiscount))
        count = count + int(batch_limit)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skus", help='skus which needs tobe checked')
    parser.add_argument("--batch_limit", help='limit of records in single batch')
    parser.add_argument("--upto", help='restrict number of records instead of whole es count')
    argv = vars(parser.parse_args())
    skus = argv.get('skus', None)
    limit = argv.get('batch_limit', None)
    upto = argv.get('upto', None)
    if skus:
        skus = skus.split(",")
    if not limit:
        limit = 500
    if upto:
        restrictLimit = upto
    compareData(skus, limit, restrictLimit)
