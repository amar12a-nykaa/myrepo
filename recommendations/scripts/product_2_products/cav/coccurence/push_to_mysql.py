import os
from collections import defaultdict
import sys
import argparse
import json
sys.path.append("/nykaa/api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils
import boto3

pasdb = DiscUtils.mysqlConnection()
cursor = pasdb.cursor()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Push To MYSQL')
    parser.add_argument('--bucket-name', '-b', required=True)
    parser.add_argument('--file', '-f', required=True)

    argv = vars(parser.parse_args())

    s3 = boto3.client('s3')
    file_obj = s3.get_object(Bucket=argv['bucket_name'], Key=argv['file'])

    create_recommendations_table_query = """ CREATE TABLE IF NOT EXISTS recommendations_v2 (
                                    entity_id INT UNSIGNED NOT NULL, 
                                    entity_type VARCHAR(50),
                                    recommendation_type VARCHAR(50),
                                    algo VARCHAR(50),
                                    recommended_products_json JSON,
                                    PRIMARY KEY (entity_id, entity_type, recommendation_type, algo)
                            )
    """
    cursor.execute(create_recommendations_table_query)

    recommendations = json.load(file_obj['Body'])

    def store_in_mysql(rows):
        if not rows:
            return
        values_str = ", ".join(["(%s, %s, %s, %s, %s)" for i in range(len(rows))])
        values = tuple([_i for row in rows for _i in row])
        insert_recommendations_query = """ INSERT INTO recommendations_v2(entity_id, entity_type, recommendation_type, algo, recommended_products_json)
                    VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json)
        """ % values_str
        cursor.execute(insert_recommendations_query, values)
        pasdb.commit()

    rows = []
    for recommendation in recommendations:
        rows.append((recommendation[0], 'product', 'viewed', recommendation[1], recommendation[2]))
        if len(rows) == 1000:
            store_in_mysql(rows)
            rows = []

    store_in_mysql(rows)

    nykaadb = DiscUtils.nykaaMysqlConnection()
    nykaa_cursor = nykaadb.cursor()

    def extract_data(query): 
        nykaa_cursor.execute(query) 
        rows = [] 
        BATCH_SIZE = 10000

        while True:
            batch_empty = True
            for row in nykaa_cursor.fetchmany(BATCH_SIZE):
                batch_empty = False
                rows.append(row)
            if batch_empty:
                break
        return rows

    query = "select parent_id, child_id from catalog_product_relation"
    rows = extract_data(query)
    child_2_parent = defaultdict(lambda: [])
    for row in rows:
        child_2_parent[row[0]].append(row[1])

    with open("child_product_2_parent.json", "r+") as f:
        for key, value in json.load(f).items():
            child_2_parent[int(value)].append(int(key))

    recommendations_dict = {str(recommendation[0])+"_"+str(recommendation[1]): recommendation[2] for recommendation in recommendations}

    for parent, variants in child_2_parent.items():
        rows = []
        for variant in variants:
            for algo in ['coccurence_simple', 'coccurence_direct', 'coccurence_log', 'coccurence_sqrt', 'coccurence_direct_mrp_cons', 'coccurence_log_mrp_cons', 
                         'coccurence_simple_mrp_cons', 'coccurence_sqrt_mrp_cons', 'coccurence_simple_desktop']:
                if recommendations_dict.get(str(parent) + "_" + algo):
                    rows.append((variant, 'product', 'viewed', algo, recommendations_dict[str(parent) + "_" + algo]))
        store_in_mysql(rows)
