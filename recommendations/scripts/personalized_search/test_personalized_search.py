import csv
import argparse
import json
import tempfile
import boto3
import os
import re
import json
import requests
from IPython import embed

def calculate_difference(url, q, algo, user_id, page_no=1, size=20):
    response = requests.get(url, {'url': url, 'q': q, 'page_no': page_no, 'size': size, 'sort': 'relevance_new', 'fields': 'product_id'})
    personalized_response = requests.get(url, {'url': url, 'q': q, 'algo': algo, 'user_id': user_id, 'page_no': page_no, 'size': size, 'sort': 'relevance_new', 'fields': 'product_id'})
    normal_pids = [product['product_id'] for product in response.json()['response']['products']]
    personalized_pids = [product['product_id'] for product in personalized_response.json()['response']['products']]
    print(normal_pids)
    print(personalized_pids)
    return len(set(personalized_pids) - set(normal_pids))

def get_params_from_file(fname):
    with open(fname) as f:
        csv_reader = csv.DictReader(f)
        print(csv_reader.fieldnames)
        pairs = [{'customer_id': row['\ufeff"Customer ID (evar23)"'], 'search_term': row['Internal Search Term (Conversion) (evar6)']} for row in csv_reader if row['\ufeff"Customer ID (evar23)"'] and row['Internal Search Term (Conversion) (evar6)']]
        return pairs

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for testing personalized search')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--url')
    parser.add_argument('--file', required=True)
    parser.add_argument('--limit', type=int, default=0)

    argv = vars(parser.parse_args())
    pairs = get_params_from_file(argv['file'])
    difference = 0.
    if argv['limit']:
        for i in range(0, min(len(pairs), argv['limit'])):
            difference += calculate_difference(argv['url'], pairs[i]['search_term'], 'lsi_100', pairs[i]['customer_id'])
    else:
        for pair in pairs:
            difference += calculate_difference(argv['url'], pair['search_term'], 'lsi_100', pair['customer_id'])
    print(difference//len(pairs))
