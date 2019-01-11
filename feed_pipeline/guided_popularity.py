import argparse
import sys
import pandas as pd

sys.path.append("/home/shweta/nykaa/fresh_nykaa_apis")
from pas.v2.utils import Utils

FREQUENCY_THRESHOLD = 3

def insert_guides_in_es(df):
    pass


def process_guides(filename='guide.csv'):
    df = pd.read_csv(filename, encoding="ISO-8859-1")

    #apply frequency threshold
    df = df[df.freq >= FREQUENCY_THRESHOLD]

    # exclude filters
    df.dropna(subset=['filter_name', 'filter_value'], inplace=True)
    filter_exclude_list = ['Price', 'Avg Customer Rating', 'Discount', 'Gender', 'Star_Rating']
    df = df[~df['filter_name'].isin(filter_exclude_list)]

    # normalize filter name
    df['filter_name'] = df['filter_name'].apply(lambda x: x.replace(" ", "_").lower())

    #normalize filter value
    def get_category_value(row):
        if row['filter_name'] == 'category':
            cat_list = row['filter_value'].split(':')
            cat_list = list(filter(None, cat_list))
            category = cat_list[-1]
            if len(cat_list) > 3:
                category = cat_list[2]
            row['filter_value'] = category
        return row
    df = df.apply(get_category_value, axis=1)

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
        entities = get_entities(keyword)
        filter_list = list(entities.keys())
        temp_df = df[df['keyword'] == keyword]
        temp_df = temp_df[~temp_df['filter_name'].isin(filter_list)].reset_index(drop=True)
        temp_df.drop(['freq'], axis=1, inplace=True)

        guide_list.append(temp_df)

    guide = pd.concat(guide_list).reset_index()
    return guide


def get_entities(query):
    query = query.lower()
    query_formatted = "".join(query.split())
    result = {}

    querydsl = {}
    querydsl['sort'] = {
        '_score': 'desc',
        'weight': 'desc'
    }
    querydsl['size'] = 10
    querydsl['query'] = {
        "multi_match": {
            "query": query,
            "fields": ["entity", "entity.shingle", "entity.shingle_search"]
        }
    }

    response = Utils.makeESRequest(querydsl, index='entity')
    docs = response['hits']['hits']

    for index, doc in enumerate(docs):
        doc = doc['_source']
        entity_type = doc['type'].lower()

        entity_name = ""
        entity_names_list = []
        if 'entity_synonyms' in doc:
            entity_names_list = doc['entity_synonyms']
        entity_names_list.insert(0, doc['entity'])
        for entity_name_orig in entity_names_list:
            entity_name = "".join(entity_name_orig.split())
        if entity_name.lower() in query_formatted:
            # take the longest matching entity for each type
            if entity_type in result and len(entity_name) < len(result[entity_type]['entity']):
                continue
            result[entity_type] = {
                'id': doc['id'],
                'entity': doc['entity'],
                'entity_formatted': entity_name,
                'rank': index
            }

    # drop entity if it is subtring of another entity
    drop_entities = []
    entity_type_list = result.keys()
    for entity_type in entity_type_list:
        for en_type, en_data in result.items():
            if en_type == entity_type:
                continue
            if result[entity_type]['entity_formatted'] in en_data['entity_formatted']:
                drop_entities.append(entity_type)
                break
    for entity in drop_entities:
        result.pop(entity, None)

    return result


def get_filters():
    mysql_conn = Utils.nykaaMysqlConnection(force_production=True)
    query = """select eov.option_id as filter_id, eov.value as filter_value, e.attribute_code as filter_name 
               from eav_attribute e 
                  join eav_attribute_option eo on e.attribute_id = eo.attribute_id
                  join eav_attribute_option_value eov on eo.option_id = eov.option_id and eov.store_id = 0
               where attribute_code like '%_v1' 
                  and e.attribute_id not in (654, 668, 722, 725, 732, 773, 821, 828, 829, 830)"""
    filters = pd.read_sql(query, con=mysql_conn)
    mysql_conn.close()

    filters['filter_name'] = filters['filter_name'].apply(lambda x : x[:-3])

    mysql_conn = Utils.mysqlConnection()
    query = """select brand_id as filter_id, brand as filter_value, 'brand' as filter_name
                from brands"""
    brands = pd.read_sql(query, con=mysql_conn)

    query = """select id as filter_id, name as filter_value, 'category' as filter_name
                from l3_categories where url not like '%luxe%' and url not like '%shop-by-concern%'"""
    categories = pd.read_sql(query, con=mysql_conn)
    mysql_conn.close()

    filters = pd.concat([filters, brands, categories])
    return filters


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for feedback result')
    parser.add_argument('--filename', '-f', type=str, default='guide.csv')

    # collection_state = parser.add_mutually_exclusive_group(required=True)
    # collection_state.add_argument("--inactive", action='store_true')
    # collection_state.add_argument("--active", action='store_true')
    # collection_state.add_argument("--collection")

    parser.add_argument("--swap", action='store_true', help="Swap the Core")

    argv = vars(parser.parse_args())
    filename = argv['filename']
    guides = process_guides(filename)
    filters = get_filters()

    guides = pd.merge(guides, filters, on=['filter_name', 'filter_value'])
    print(guides.shape)
