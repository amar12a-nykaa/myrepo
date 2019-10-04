import argparse
import sys
import pandas as pd
import numpy as np
import json
import os

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import EntityUtils
sys.path.append('/nykaa/scripts/sharedutils/')
from esutils import EsUtils

FREQUENCY_THRESHOLD = 3
GUIDE_SIZE = 20

def process_guides(filename='guide.csv'):
    file_path = '/nykaa/scripts/' + filename
    df = pd.read_csv(file_path, encoding="ISO-8859-1", sep='\t', header=0)
    #apply frequency threshold
    df = df[df.freq >= FREQUENCY_THRESHOLD]

    # exclude filters
    df.dropna(subset=['filter_name', 'filter_value'], inplace=True)
    # filter_exclude_list = ['Price', 'Avg Customer Rating', 'Discount', 'Gender', 'Star_Rating']
    # df = df[~df['filter_name'].isin(filter_exclude_list)]

    # normalize filter name
    # df['filter_name'] = df['filter_name'].apply(lambda x: x.replace(" ", "_").lower())

    #normalize filter value
    # def get_category_value(row):
    #     if row['filter_name'] == 'category':
    #         cat_list = row['filter_value'].split(':')
    #         cat_list = list(filter(None, cat_list))
    #         category = cat_list[-1]
    #         if len(cat_list) > 3:
    #             category = cat_list[2]
    #         row['filter_value'] = category
    #     return row
    # df = df.apply(get_category_value, axis=1)

    # get top 100 keywords
    keyword_frequency = df.groupby('keyword').agg({"freq": "sum"}).reset_index()
    keyword_frequency = keyword_frequency.sort_values(by='freq', ascending=False)
    keyword_frequency.drop('freq', axis=1, inplace=True)
    keyword_frequency = keyword_frequency[:100]

    # filter data for top 100 keywords
    df = pd.merge(df, keyword_frequency, on='keyword')

    guide_list = []
    keyword_list = list(map(str, keyword_frequency.iloc[:, 0]))
    for keyword in keyword_list:
        # remove filters present in keyword itself
        entities, coverage = EntityUtils.get_matched_entities(keyword)
        filter_list = list(entities.keys())
        temp_df = df[df['keyword'] == keyword]
        cat_df = temp_df[temp_df['filter_name'] == 'category']
        cat_df = cat_df.head(GUIDE_SIZE)
        non_cat_df = temp_df[temp_df['filter_name'] != 'category']
        temp_df = pd.concat([cat_df, non_cat_df])
        temp_df = temp_df.sort_values(by='freq', ascending=False)
        temp_df = temp_df[~temp_df['filter_name'].isin(filter_list)].reset_index(drop=True)
        # temp_df.drop(['freq'], axis=1, inplace=True)

        guide_list.append(temp_df)
    guide = pd.concat(guide_list).reset_index()
    guide.rename(columns={'filter_value': 'filter_id'}, inplace=True)
    return guide

def get_filters():
    mysql_conn = PasUtils.nykaaMysqlConnection(force_production=True)
    query = """select eov.option_id as filter_id, eov.value as filter_value, e.attribute_code as filter_name 
               from eav_attribute e 
                  join eav_attribute_option eo on e.attribute_id = eo.attribute_id
                  join eav_attribute_option_value eov on eo.option_id = eov.option_id and eov.store_id = 0
               where attribute_code like '%_v1' 
                  and e.attribute_id not in (654, 668, 722, 725, 732, 773, 821, 828, 829, 830)"""
    filters = pd.read_sql(query, con=mysql_conn)
    mysql_conn.close()

    filters['filter_name'] = filters['filter_name'].apply(lambda x: x[:-3])
    filters = filters.astype({'filter_id': str})
    mysql_conn = PasUtils.mysqlConnection()
    #get color codes
    query = """SELECT attribute_id as filter_id, group_concat(value) as color_code FROM attribute_values group by 1"""
    color_codes = pd.read_sql(query, con=mysql_conn)
    color_codes['color_code'] = color_codes['color_code'].apply(lambda x : x.split(',') if x else None)
    filters = pd.merge(filters, color_codes, how='left', on='filter_id')

    query = """select brand_id as filter_id, brand as filter_value, 'brand' as filter_name
                from brands"""
    brands = pd.read_sql(query, con=mysql_conn)

    query = """select id as filter_id, name as filter_value, 'category' as filter_name
                from l3_categories where url not like '%luxe%' and url not like '%shop-by-concern%'"""
    categories = pd.read_sql(query, con=mysql_conn)
    mysql_conn.close()

    price_intervals = np.array([['filter_id', 'filter_value', 'filter_name'],
                                ['0-499', 'Price<500', 'price'], ['500-999', 'Price:500-999', 'price'],
                                ['1000-1999', 'Price:1000-1999', 'price'], ['2000-3999', 'Price:2000-3999', 'price'],
                                ['4000-*', 'Price>4000', 'price']])
    price_data = pd.DataFrame(data=price_intervals[1:, :], columns=price_intervals[0, :])

    discount_intervals = np.array([['filter_id', 'filter_value', 'filter_name'],
                                   ['0-10', '0-10', 'Discount<10%'], ['10-*', 'Discount>10%', 'discount'],
                                   ['20-*', 'Discount>20%', 'discount'], ['30-*', 'Discount>30%', 'discount'],
                                   ['40-*', 'Discount>40%', 'discount']])
    discount_data = pd.DataFrame(data=discount_intervals[1:, :], columns=discount_intervals[0, :])
    
    star_ratings = np.array([['filter_id', 'filter_value', 'filter_name'],
                             ['1', 'Rating>1', 'star_rating'], ['2', 'Rating>2', 'star_rating'],
                             ['3', 'Rating>3', 'star_rating'], ['4', 'Rating>4', 'star_rating']])
    rating_data = pd.DataFrame(data=star_ratings[1:, :], columns=star_ratings[0, :])
    
    filters = pd.concat([filters, brands, categories, price_data, discount_data, rating_data])
    return filters


def override_filter_value(df):
    overrides = pd.read_csv('overrides.csv')
    for i, row in overrides.iterrows():
        row = dict(row)
        type = row['filter_name']
        entity_value = row['filter_value']
        replacement_text = row['display text']
        df['entity_value'] = df.apply(lambda x : replacement_text if (x['type'] == type and
                                                x['entity_id'] == str(entity_value)) else x['entity_value'], axis=1)
    return df
    
def insert_guides_in_es(guides, collection):
    guides.rename(columns={'freq': 'rank', 'filter_name': 'type', 'filter_id': 'entity_id', 'keyword': 'query',
                           'filter_value': 'entity_value'}, inplace=True)
    override_filter_value(guides)
    guides['_id'] = guides.index
    guide_color = guides[~guides['color_code'].isnull()]
    documents = guide_color.to_dict(orient='records')
    EsUtils.indexDocs(documents, collection)

    guide_non_color = guides[guides['color_code'].isnull()]
    guide_non_color.drop(['color_code'], axis=1, inplace=True)
    documents = guide_non_color.to_dict(orient='records')

    print(documents[:10])
    document_chunks = [documents[i:i + 1000] for i in range(0, len(documents), 1000)]
    for docs in document_chunks:
        EsUtils.indexDocs(docs, collection)

def index_guides(collection, active, inactive, swap, filename):
    print('Starting Processing')
    if collection:
        index = collection
    elif active:
        index = EsUtils.get_active_inactive_indexes('guide')['active_index']
    elif inactive:
        index = EsUtils.get_active_inactive_indexes('guide')['inactive_index']
    else:
        index = None

    if not index:
        return

    index_client = EsUtils.get_index_client()
    if index_client.exists(index):
        print("Deleting index: %s" % index)
        index_client.delete(index)
    schema = json.load(open(os.path.join(os.path.dirname(__file__), 'guide_schema.json')))
    index_client.create(index, schema)
    print("Creating index: %s" % index)

    guides = process_guides(filename)
    filters = get_filters()
    #guides = guides.astype({'filter_id': int})
    guides = guides.astype({'filter_id': str})
    guides = pd.merge(guides, filters, on=['filter_name', 'filter_id'])
    guides['filter_name'] = guides['filter_name'].apply(lambda x: x + '_range' if x in ['price', 'discount'] else x)
    guides['filter_name'] = guides['filter_name'].apply(lambda x: x + '_filter')
    insert_guides_in_es(guides, index)

    if swap:
      print("Swapping Index")
      indexes = EsUtils.get_active_inactive_indexes('guide')
      EsUtils.switch_index_alias('guide', indexes['active_index'], indexes['inactive_index'])

    print("Done Processing")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for feedback result')
    parser.add_argument('--filename', '-f', type=str, default='guide.csv')

    collection_state = parser.add_mutually_exclusive_group(required=True)
    collection_state.add_argument("--inactive", action='store_true')
    collection_state.add_argument("--active", action='store_true')
    collection_state.add_argument("--collection")

    parser.add_argument("--swap", action='store_true', help="Swap the Core")

    argv = vars(parser.parse_args())
    filename = argv['filename']

    index_guides(collection=argv['collection'], active=argv['active'], inactive=argv['inactive'], swap=argv['swap'],filename=filename)
