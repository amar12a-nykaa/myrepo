import pandas as pd
import gc
import sys
sys.path.append("/home/apis/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/home/apis/discovery_api")
from disc.v2.utils import Utils as DiscUtils

df = pd.read_csv('order_customer_data_with_archived.csv')
products_df = df.filter(['product_id', 'product_name'])
products_df.head()
productid2name = dict(zip(products_df.product_id, products_df.product_name))
len(productid2name)

del products_df

unique_products = df['product_id'].unique()
unique_customers = df['customer_id'].unique()
df = df.filter(['customer_id', 'product_id'])

df['bought_count'] = 1
df = df.groupby(['customer_id', 'product_id']).agg({'bought_count': 'sum'}).reset_index()

df.drop(['bought_count'], axis=1, inplace=True)
df = df.sort_values(['customer_id'])

BATCH_SIZE = 5000000
offset = 0
final_df = pd.DataFrame([])
df_len = len(df)

while offset < df_len:
    last_idx = offset + BATCH_SIZE if offset + BATCH_SIZE < df_len else df_len
    print("Started processing data from offset %d till %d" % (offset, last_idx-1))
    _df = df[offset:last_idx]
    _df = pd.merge(_df, _df, on="customer_id", how="inner") 
    _df = _df[_df['product_id_x'] < _df['product_id_y']].reset_index(drop=True)
    _df = _df.drop('customer_id', axis=1)
    _df['customer_count'] = 1
    _df = _df.groupby(['product_id_x', 'product_id_y']).agg({'customer_count': 'sum'}).reset_index()
    final_df = final_df.append(_df)
    final_df = final_df.groupby(['product_id_x', 'product_id_y']).agg({'customer_count': 'sum'}).reset_index()
    offset = last_idx

def get_recommendations(product_id, recommendations_cnt=10, with_name=True):
    _df = final_df[(final_df['product_id_x'] == product_id) | (final_df['product_id_y'] == product_id)]
    rows = _df.values.tolist()
    product_similarity = []
    for row in rows:
        if row[0] == product_id:
            product_similarity.append((row[1], row[2]))
        elif row[1] == product_id:
            product_similarity.append((row[0], row[2]))
        else:
            raise Exception()
    sorted_similar_products = sorted(product_similarity, key=lambda x: x[1], reverse=True)
    if with_name:
        return [(_s[0], productid2name[_s[0]], _s[1]) for _s in sorted_similar_products[:recommendations_cnt]]
    else:
        return [_s[0] for _s in sorted_similar_products[:recommendations_cnt]]


# Writing data to csv
import csv
query = {
    "query": {
        "match_all": {}
    },
    "sort": [
        {"popularity": {"order": "desc"}}
    ],
    "_source": ["product_id"],
    "size": 500
}

response = PasUtils.makeESRequest(query, index='livecore')
with open('top_500_products_recommendations.csv', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    popular_products = [hit['_source']['product_id'] for hit in response['hits']['hits']]
    for product_id in popular_products:
        recommendations = get_recommendations(product_id, 10)
        row = [productid2name[product_id]] + [recommendation[1] for recommendation in recommendations]
        csv_writer.writerow(row)

pasdb = PasUtils.mysqlConnection('w')
cursor = pasdb.cursor()
create_recommendations_table_query = """ CREATE TABLE IF NOT EXISTS recommendations (
                            entity_id INT UNSIGNED NOT NULL, 
                            entity_type VARCHAR(50),
                            recommendation_type VARCHAR(50),
                            recommended_products_json JSON,
                            PRIMARY KEY (entity_id, entity_type, recommendation_type)
                            )
"""
cursor.execute(create_recommendations_table_query)
rows = []
for product_id in unique_products:
    recommendations = get_recommendations(product_id, 10, False)
    if not recommendations:
        continue
    recommendations_str = str(recommendations)
    rows.append((product_id, 'product', 'bought', recommendations_str))
    if len(rows) == 1000:
        values_str = ", ".join(["(%s, %s, %s, %s)" for i in range(len(rows))])
        values = tuple([_i for row in rows for _i in row])
        insert_recommendations_query = """ INSERT INTO recommendations(entity_id, entity_type, recommendation_type, recommended_products_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json);
        """ % values_str
        values = tuple([str(_i) for row in rows for _i in row])
        cursor.execute(insert_recommendations_query, values)
        pasdb.commit()
        rows = []
        
values_str = ", ".join(["(%s, %s, %s, %s)" for i in range(len(rows))])
values = tuple([str(_i) for row in rows for _i in row])
insert_recommendations_query = """ INSERT INTO recommendations(entity_id, entity_type, recommendation_type, recommended_products_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json);
""" % values_str
values = tuple([_i for row in rows for _i in row])
cursor.execute(insert_recommendations_query, values)
pasdb.commit()

