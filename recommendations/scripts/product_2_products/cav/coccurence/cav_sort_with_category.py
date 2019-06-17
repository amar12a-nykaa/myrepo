import sys
import json
import argparse

sys.path.append("/home/ubuntu/api")
from pas.v2.utils import Utils, RecommendationsUtils

sys.path.append("/home/ubuntu/nykaa_scripts/utils/")
sys.path.append("/home/ubuntu/nykaa_scripts/sharedutils/")

def get_product_category():
    category_query = "SELECT entity_id, value from catalog_product_entity_varchar WHERE attribute_id = '807';"
    rows = Utils.fetchResultsInBatch(Utils.nykaaMysqlConnection(), category_query, 50000)
    print("Total rows extracted: %d" % len(rows))
    product_2_l3category = {row[0] : row[1] for row in rows}
    return product_2_l3category

def filter_by_l3_category(source_algo, algo, limit=None):
    product_2_l3category = get_product_category()
    if limit:
        query = "SELECT entity_id, recommended_products_json FROM `recommendations_v2` WHERE entity_type='product' and recommendation_type='viewed' and algo='%s' LIMIT %d" % (
        source_algo, limit)
    else:
        query = "SELECT entity_id, recommended_products_json FROM `recommendations_v2` WHERE entity_type='product' and recommendation_type='viewed' and algo='%s'" % source_algo
    mysql_conn = Utils.mysqlConnection('w')
    cursor = mysql_conn.cursor()
    cursor.execute(query)
    rows = []
    for row in cursor.fetchall():
        product_id = row[0]
        recommendations = json.loads(row[1])
        if(product_id in product_2_l3category):
            new_recommendations = [p for p in recommendations if p in product_2_l3category and product_2_l3category[p] == product_2_l3category[product_id]]
            new_recommendations += [p for p in recommendations if (p in product_2_l3category and product_2_l3category[p] != product_2_l3category[product_id])
                                    or p not in product_2_l3category]
        else:
            new_recommendations = recommendations
        if new_recommendations:
            rows.append((str(product_id), 'product', 'viewed', algo, json.dumps(new_recommendations)))

    RecommendationsUtils.add_recommendations_in_mysql(Utils.mysqlConnection('w'), 'recommendations_v2', rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-algo', '-s', required=True)
    parser.add_argument('--new-algo', '-n', required=True)
    parser.add_argument('--limit', '-l', type=int)

    argv = vars(parser.parse_args())
    source_algo = argv['source_algo']
    new_algo = argv['new_algo']
    filter_by_l3_category(source_algo, new_algo, argv.get('limit'))