import os
from collections import defaultdict
import sys
import time
import json
import requests
import psutil
import argparse
import operator
import csv
sys.path.append("/home/apis/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils

pasdb = DiscUtils.mysqlConnection('w')
cursor = pasdb.cursor()
es_conn = DiscUtils.esConn()

def getFBTs(product_id):
    query = "SELECT * FROM frequently_bought where product_id=%s" % product_id
    cursor.execute(query)
    row = cursor.fetchone()
    if not row:
        return []
    fbts = json.loads(row[1])
    product_ids = [x['product_id'] for x in fbts]
    query = { 
        "query": { 
            "terms": { "product_id": product_ids } 
        },
        "_source": ["product_id", "title_text_split"]
    }
    response = es_conn.search(index='livecore', body=query)
    if not response['hits']['hits']:
        print("No FBT data for this product")
        return
    print(response['hits']['hits'])
    product_to_doc_dict = {hit['_source']['product_id']: hit for hit in response['hits']['hits']}
    print(product_to_doc_dict)
    for i in range(len(fbts)):
        product_id = str(fbts[i]['product_id'])
        if not product_to_doc_dict.get(product_id):
            continue
        fbts[i]['name'] = product_to_doc_dict[product_id]['_source']['title_text_split']

    return fbts

def requestGetRecommendations(product_id, algo):
    r = requests.get(algo_2_api[algo] % product_id)
    r = json.loads(r.text)
    recommendations = []
    for p in r['result']:
        recommendations.append("%s(%s)" % (p['title'], p["product_id"]))

    return recommendations


algo_2_api = {
    'fbt_v1': 'http://prod-api.nyk00-int.network/apis/v2/product.frequentlyBought?id=%d&algo=v1',
    'fbt_v2': 'http://prod-api.nyk00-int.network/apis/v2/product.frequentlyBought?id=%d&algo=v2',
    'fbt_coccurence_direct': 'http://prod-api.nyk00-int.network/apis/v2/product.frequentlyBought?id=%d&algo=coccurence_direct',
    'fbt_coccurence_log': 'http://prod-api.nyk00-int.network/apis/v2/product.frequentlyBought?id=%d&algo=coccurence_log',
    'fbt_coccurence_sqrt': 'http://prod-api.nyk00-int.network/apis/v2/product.frequentlyBought?id=%d&algo=coccurence_sqrt',
    'fbt_coccurence_simple': 'http://prod-api.nyk00-int.network/apis/v2/product.frequentlyBought?id=%d&algo=coccurence_simple',

    'cab_coccurence_direct': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoBought?id=%d&algo=coccurence_direct',
    'cab_coccurence_log': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoBought?id=%d&algo=coccurence_log',
    'cab_coccurence_sqrt': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoBought?id=%d&algo=coccurence_sqrt',
    'cab_coccurence_simple': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoBought?id=%d&algo=coccurence_simple',

    'cav_coccurence_direct': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_direct',
    'cav_coccurence_log': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_log',
    'cav_coccurence_sqrt': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_sqrt',
    'cav_coccurence_simple': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_simple',
    'cav_coccurence_direct_mrp_cons': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_direct_mrp_cons',
    'cav_coccurence_log_mrp_cons': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_log_mrp_cons',
    'cav_coccurence_sqrt_mrp_cons': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_sqrt_mrp_cons',
    'cav_coccurence_simple_mrp_cons': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_simple_mrp_cons',
    'cav_coccurence_simple_with_l3_cons': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_simple_with_l3_cons',
    'cav_coccurence_simple_with_bought': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoViewed?id=%d&algo=coccurence_simple_with_bought',
    'cab_lsi_100': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoBought?id=%d&algo=lsi_100',
    'cab_order_lsi_100': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoBought?id=%d&algo=order_lsi_100',
    'cab_cf_1': 'http://prod-api.nyk00-int.network/apis/v2/recommendations.customersAlsoBought?id=%d&algo=cf_1'
}

def generate():
    print("Generating FBT for 500 popular products")
    query = {
        "size": 100,
        "sort": [
            {"popularity": {"order": "desc"}}
        ],
        "_source": ["product_id", "popularity", "title_text_split", "type"]
    }
    response = es_conn.search(index='livecore', body=query)
    if not response['hits']['hits']:
        print("No results for the query")
        return
    for algo, api in algo_2_api.items():
        with open('recommendation_csvs/' + algo+".csv", "w") as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["Product", "Recommendations"])
            for hit in response['hits']['hits']:
                recommendations = requestGetRecommendations(int(hit['_source']['product_id']), algo)
                csvwriter.writerow(["%s(%s)" % (hit["_source"]["title_text_split"], hit["_source"]["product_id"])] + recommendations)

def generate_from_file(file_name):
    print("Generating recommendations csv")
    with open(file_name) as product_file:
        product_ids = json.load(product_file)
    query = {
        "query": {
            "terms": {"product_id": product_ids}
        },
        "size": len(product_ids),
        "_source": ["product_id", "popularity", "title_text_split", "type"]
    }
    response = es_conn.search(index='livecore', body=query)
    if not response['hits']['hits']:
        print("No results for the query")
        return

    r = requests.get('http://prod-api.nyk00-int.network/apis/v2/product.listids?ids=' + ",".join([str(product_id) for product_id in product_ids]))
    r_json = r.json()

    product_2_name = {int(product['product_id']): product['title'] for product in r_json['result']['products']}
    #product_2_name = {int(hit['_source']['product_id']): hit['_source']['title_text_split'] for hit in response["hits"]['hits']}

    for algo, api in algo_2_api.items():
        with open('recommendation_csvs/' + algo+".csv", "w") as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["Product", "Recommendations"])
            for product_id in product_ids:
                recommendations = requestGetRecommendations(product_id, algo)
                csvwriter.writerow(["%s(%d)" % (product_2_name.get(product_id, '---'), product_id)] + recommendations)

if __name__ == "__main__":
    generate_from_file('/home/ubuntu/data/top_500_products_by_revenue.json')
    #generate()
