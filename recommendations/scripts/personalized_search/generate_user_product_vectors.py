#Usage
#python generate_user_product_vectors.py --bucket-name=nykaa-dev-recommendations --input-dir="gensim_models/raw_cab_2018_till_11_sept/metric_customer_id/similarity_product_id/topics_100" --vector-len=100 --store-in-db --user-json="user_vectors.json" --product-json="product_vectors.json"
import argparse
import json

import sys
sys.path.append("/home/apis/nykaa")
from pas.v2.utils import Utils
sys.path.append("/home/ubuntu/nykaa_scripts/utils")
from gensimutils import GensimUtils

def _add_embedding_vectors_in_mysql(cursor, table, rows):
    values_str = ", ".join(["(%s, %s, %s, %s)" for i in range(len(rows))]) 
    values = tuple([_i for row in rows for _i in row]) 
    insert_recommendations_query = """ INSERT INTO %s(entity_id, entity_type, algo, embedding_vector) 
        VALUES %s ON DUPLICATE KEY UPDATE embedding_vector=VALUES(embedding_vector)
    """ % (table, values_str) 
    values = tuple([str(_i) for row in rows for _i in row])
    cursor.execute(insert_recommendations_query, values) 

def add_embedding_vectors_in_mysql(db, table, rows):
    cursor = db.cursor() 
    for i in range(0, len(rows), 1000):
        _add_embedding_vectors_in_mysql(cursor, table, rows[i:i+1000]) 
        db.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for generating topics')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--bucket-name', required=True) 
    parser.add_argument('--input-dir', required=True) 
    parser.add_argument('--vector-len', required=True, type=int) 
    parser.add_argument('--product-json') 
    parser.add_argument('--user-json') 
    parser.add_argument('--store-in-db', action='store_true')

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    bucket_name = argv['bucket_name']
    input_dir = argv['input_dir']
    vector_len = argv['vector_len']
    product_json = argv.get('product_json')
    user_json = argv.get('user_json')
    store_in_db = argv.get('store_in_db')

    print("Downloading the models")
    user_corpus_dict, user_tfidf, user_tfidf_lsi, user_lsi = GensimUtils.get_models(bucket_name, input_dir)

    print("Generating user vectors")
    user_vectors = {}
    for user_id, product_ids_bow in user_corpus_dict.items():
        user_vectors[user_id] = GensimUtils.generate_complete_vectors(user_lsi[product_ids_bow], vector_len)

    product_ids = list(set([product_tuple[0] for products_bow in user_corpus_dict.values() for product_tuple in products_bow]))

    print("Generating product vectors")
    product_vectors = {}
    for product_id in product_ids:
        product_vectors[str(product_id)] = GensimUtils.generate_complete_vectors(user_lsi[[[product_id, 1]]], vector_len)

    if product_json and user_json:
        print("Writing json file")
        with open(user_json, 'w') as f:
            json.dump(user_vectors, f)

        with open(product_json, 'w') as f:
            json.dump(product_vectors, f)

    if store_in_db:
        rows = []
        for user_id, vector in user_vectors.items():
            rows.append((user_id, 'user', 'lsi', json.dumps(vector)))

        for product_id, vector in product_vectors.items():
            rows.append((product_id, 'product', 'lsi', json.dumps(vector)))

        add_embedding_vectors_in_mysql(Utils.mysqlConnection('w'), 'embedding_vectors', rows)
