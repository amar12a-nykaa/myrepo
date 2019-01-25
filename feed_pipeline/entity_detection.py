import argparse
import sys
import pandas as pd
import json
import os
import unicodedata
from nltk.stem import PorterStemmer

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils
sys.path.append('/nykaa/scripts/sharedutils/')
from esutils import EsUtils

FREQUENCY_THRESHOLD = 3

def strip_accents(text):
    return ''.join(char for char in unicodedata.normalize('NFKD', text) if unicodedata.category(char) != 'Mn')


def get_entities(query):
    porter = PorterStemmer()
    special_brand_list = ['faces','acnes']
    ignore_brand_list = ["charcoalmask", "blush"]
    brand_synonym_reverse = {
        "faces canada": "faces",
        "wet and wild": "wet n wild",
        "lotus professional": "lotus herbals",
        "sugar cosmetics": "sugar",
        "la girl": "l.a. girl",
        "mac ": "m.a.c ",
    }

    query = strip_accents(query.lower())
    for synonym, brand in brand_synonym_reverse.items():
        query = query.replace(synonym, brand)
    queryListRaw = query.split()
    query_formatted_raw = ''.join(queryListRaw)

    queryList = [porter.stem(e) if e not in special_brand_list else e for e in queryListRaw]
    query_formatted = "".join(queryList)

    q_length = len(queryListRaw)

    result = {}

    querydsl = {}
    querydsl['sort'] = [{ '_score': 'desc'}, {'weight': 'desc'}]
    querydsl['size'] = 10
    querydsl['query'] = {
        "multi_match": {
            "query": query,
            "fields": ["entity", "entity.shingle", "entity.shingle_search"]
        }
    }

    response = Utils.makeESRequest(querydsl, index='entity')
    docs = response['hits']['hits']
    entity_dict = []
    for index, doc in enumerate(docs):
        doc = doc['_source']
        entity_type = doc['type'].lower()

        entity_name = ""
        entity_name_raw = ""
        entity_names_list = []
        if 'entity_synonyms' in doc:
            entity_names_list = doc['entity_synonyms']
        entity_names_list.insert(0, doc['entity'])
        for entity_name_orig in entity_names_list:
            word_list = entity_name_orig.lower().split()
            word_list = [strip_accents(e) for e in word_list]
            entity_name_raw = ''.join(word_list)

            word_list = [porter.stem(e) if e not in special_brand_list else e for e in word_list]
            entity_name = "".join(word_list)

        if((entity_name in query_formatted or entity_name in query_formatted_raw) or
                (entity_name_raw in query_formatted or entity_name_raw in query_formatted_raw)):
            if entity_type == 'brand' and entity_name in special_brand_list and entity_name not in queryList:
                continue
            if entity_type == 'brand' and entity_name in ignore_brand_list:
                continue
            entity_dict.append(''.join(entity_names_list[0].split()).lower())
            if entity_type in result and len(entity_name) < len(result[entity_type]['entity']):
                continue
            result[entity_type] = {
                'id': doc['id'],
                'entity': doc['entity'],
                'entity_formatted': entity_name,
                'entity_formatted_raw': entity_name_raw,
                'rank': index
            }

    # drop entity if it is subtring of another entity
    drop_entities = []

    entity_type_list = result.keys()
    for entity_type in entity_type_list:
        for en_type, en_data in result.items():
            if en_type == entity_type:
                continue
            if en_type in drop_entities:
                continue
            if result[entity_type]['entity_formatted_raw'] in en_data['entity_formatted_raw'] or \
                result[entity_type]['entity_formatted'] in en_data['entity_formatted']:
                drop_entities.append(entity_type)
                break

    for entity in drop_entities:
        result.pop(entity, None)

    for key, value in result.items():
        words = value['entity_formatted']
        words_raw = ''.join(strip_accents(value['entity'].lower()).split())

        dropList = [e for e in queryListRaw if e in words or porter.stem(e) in words]
        queryListRaw = [e for e in queryListRaw if e not in dropList]

        dropList = [e for e in queryListRaw if e in words_raw or porter.stem(e) in words_raw]
        queryListRaw = [e for e in queryListRaw if e not in dropList]

    f_length = len(queryListRaw)
    coverage = (q_length - f_length)*100/(q_length)
    result['match'] = coverage

    return result


if __name__ == '__main__':
    df = pd.read_csv('keywords.csv')
    #df.drop(['Searches'], inplace=True, axis=1)

    def get_entity(row):
        entities = get_entities(row['Keyword'])
        dict = {}
        for entity_type, entity in entities.items():
            if entity_type == 'match':
                row['coverage'] = entity
            else:
                dict[entity_type] = entity['entity']
        row['entities'] = dict
        return row
    df['entities'] = ""
    df = df.apply(get_entity, axis=1)
    df.to_csv('keyword_entity.csv', index=False)
