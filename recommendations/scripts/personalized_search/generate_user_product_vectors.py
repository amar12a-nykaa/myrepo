#Usage
#python generate_user_product_vectors.py --bucket-name=nykaa-dev-recommendations --input-dir="gensim_models/raw_cab_2018_till_11_sept/metric_customer_id/similarity_product_id/topics_100" --vector-len=100 --store-in-db --user-json="user_vectors.json" --product-json="product_vectors.json"
import argparse
import json
from gensim import models, matutils
from IPython import embed

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
    for i in range(0, len(rows), 500):
        _add_embedding_vectors_in_mysql(cursor, table, rows[i:i+500]) 
        db.commit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for generating topics')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--algo', required=True)
    parser.add_argument('--add-vectors-from-mysql-to-es', action='store_true') 
    parser.add_argument('--bucket-name') 
    parser.add_argument('--input-dir') 
    parser.add_argument('--vector-len', type=int) 
    parser.add_argument('--product-json') 
    parser.add_argument('--user-json') 
    parser.add_argument('--store-in-db', action='store_true')
    parser.add_argument('--add-product-children', action='store_true')
    parser.add_argument('--add-in-es', action='store_true')

    argv = vars(parser.parse_args())
    verbose = argv['verbose']
    algo = argv['algo']

    if argv['add_vectors_from_mysql_to_es']:
        print("Adding vectors from mysql to es")
        query = "SELECT entity_id, embedding_vector FROM embedding_vectors WHERE entity_type='product' AND algo='%s'" % algo
        rows = Utils.fetchResultsInBatch(Utils.mysqlConnection(), query, 1000)
        print("Total number of products from mysql: %d" % len(rows))
        product_id_2_sku = {product_id: sku for sku, product_id in Utils.scrollESForResults()['sku_2_product_id'].items()}
        docs = []
        embedding_vector_field_name = 'embedding_vector_%s' % algo
        for row in rows:
            if product_id_2_sku.get(row[0]):
                docs.append({'sku': product_id_2_sku[row[0]], embedding_vector_field_name: json.loads(row[1])})

        for i in range(0, len(docs), 1000):
            Utils.updateESCatalog(docs[i:i+1000])

        exit()

    bucket_name = argv['bucket_name']
    input_dir = argv['input_dir']
    vector_len = argv['vector_len']
    product_json = argv.get('product_json')
    user_json = argv.get('user_json')
    store_in_db = argv['store_in_db']
    add_in_es = argv['add_in_es']
    add_product_children = argv['add_product_children']

    print("Downloading the models")
    user_corpus_dict, user_tfidf, user_tfidf_lsi, user_lsi = GensimUtils.get_models(bucket_name, input_dir)

    print("Generating user vectors")
    user_vectors = {}

    norm_model = models.NormModel()
    for user_id, product_ids_bow in user_corpus_dict.items():
        user_vectors[user_id] = matutils.unitvec(matutils.sparse2full(user_lsi[norm_model.normalize(product_ids_bow)], vector_len)).tolist() #GensimUtils.generate_complete_vectors(user_lsi[norm_model.normalize(product_ids_bow)], vector_len)

    product_ids = list(set([product_tuple[0] for products_bow in user_corpus_dict.values() for product_tuple in products_bow]))

    print("Generating product vectors")
    product_vectors = {}
    for product_id in product_ids:
        product_vectors[str(product_id)] = matutils.unitvec(matutils.sparse2full(user_lsi[[[product_id, 1]]], vector_len)).tolist() #GensimUtils.generate_complete_vectors(user_lsi[[[product_id, 1]]], vector_len)

    if add_product_children:
        child_2_parent = Utils.scrollESForResults()['child_2_parent']
        for child_id, parent_id in child_2_parent.items():
            if product_vectors.get(str(parent_id)):
                product_vectors[str(child_id)] = product_vectors[str(parent_id)]

    if product_json and user_json:
        print("Writing json file")
        with open(user_json, 'w') as f:
            json.dump(user_vectors, f)

        with open(product_json, 'w') as f:
            json.dump(product_vectors, f)

    if store_in_db:
        print("Storing results in mysql DB")
        rows = []
        for user_id, vector in user_vectors.items():
            rows.append((user_id, 'user', algo, json.dumps(vector)))

        for product_id, vector in product_vectors.items():
            rows.append((product_id, 'product', algo, json.dumps(vector)))

        add_embedding_vectors_in_mysql(Utils.mysqlConnection('w'), 'embedding_vectors', rows)

    if add_in_es:
        print("Adding results in ES")
        product_id_2_sku = {product_id: sku for sku, product_id in Utils.scrollESForResults()['sku_2_product_id'].items()}

        docs = []
        embedding_vector_field_name = "embedding_vector_" % algo
        for product_id, vector in product_vectors.items():
            docs.append({'sku': product_id_2_sku['product_id'], embedding_vector_field_name: vector})

        Utils.updateESCatalog(docs)
