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

def stem(s):
    slen = len(s)
    if (slen < 3 or s[slen - 1] != 's'):
        return s

    s2 = s[slen - 2]
    if s2 == 'u' or s2 == 's':
        return s
    if s2 == 'e':
        if slen > 3 and s[slen - 3] == 'i' and s[slen - 4] != 'a' and s[slen - 4] != 'e':
            return s[:-2]
        if s[slen - 3] == 'i' or s[slen - 3] == 'a' or s[slen - 3] == 'o' or s[slen - 3] == 'e':
            return s

    return s[:-1]

def get_entities(query):
    def iskdiffhelper(string, pattern, m, n, dp):
        if dp[m][n] != -1:
            return dp[m][n]

        if n == 0:
            dp[m][n] = m
            return m

        if m == 0:
            dp[m][n] = 0
            return 0

        minimum = min(iskdiffhelper(string, pattern, m - 1, n, dp) + 1,
                      iskdiffhelper(string, pattern, m, n - 1, dp) + 1)
        if string[n - 1] == pattern[m - 1]:
            minimum = min(minimum, iskdiffhelper(string, pattern, m - 1, n - 1, dp))
        else:
            minimum = min(minimum, iskdiffhelper(string, pattern, m - 1, n - 1, dp) + 1)

        dp[m][n] = minimum
        return dp[m][n]

    def iskdiff(string, pattern, k):
        m = len(pattern)
        n = len(string)

        dp = [[-1 for x in range(n + 1)] for y in range(m + 1)]
        for x in range(n):
            sol = iskdiffhelper(string, pattern, m, n - x, dp)
            if sol <= k:
                return True

        return False

    def fuzzylen(str_len):
        # fuzziness
        if str_len > 5:
            k = 2
        elif str_len >= 3:
            k = 1
        else:
            k = 0

        return k

    porter = PorterStemmer()

    special_brand_list = ['faces']

    query = query.lower()
    queryList = query.split()
    queryListRaw = [strip_accents(e) for e in queryList]
    query_formatted_raw = ''.join(queryListRaw)

    queryList = [porter.stem(e) if e not in special_brand_list else e for e in queryListRaw]
    q_length = len(queryList)
    query_formatted = "".join(queryList)

    # print(query_formatted)

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
    # print(docs)
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

        # print("%s %s" %(entity_name, query_formatted))
        if((entity_name in query_formatted or entity_name in query_formatted_raw) or
                (entity_name_raw in query_formatted or entity_name_raw in query_formatted_raw)):
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
    # print(result)
    drop_entities = []

    entity_type_list = result.keys()
    for entity_type in entity_type_list:
        for en_type, en_data in result.items():
            if en_type == entity_type:
                continue
            if en_type in drop_entities:
                continue
            if result[entity_type]['entity_formatted_raw'] in en_data['entity_formatted_raw']:
                drop_entities.append(entity_type)
                break

    # print(drop_entities)
    for entity in drop_entities:
        result.pop(entity, None)

    # print(queryList)
    for key, value in result.items():
        words = value['entity_formatted']
        words_raw = value['entity_formatted_raw']

        dropList = [e for e in queryListRaw if e in words or porter.stem(e) in words]
        queryListRaw = [e for e in queryListRaw if e not in dropList]

        dropList = [e for e in queryListRaw if e in words_raw or porter.stem(e) in words_raw]
        queryListRaw = [e for e in queryListRaw if e not in dropList]


    # print(queryList)
    f_length = len(queryListRaw)
    coverage = (q_length - f_length)*100/(q_length)
    result['match'] = coverage


    # result['match'] = word_break(''.join(query.split()), entity_dict)

    return result

def word_break(s, dict):
    dp = [None] * (len(s) + 1)
    dp[0] = list()

    for i in range(0, len(s)):
        if dp[i] is None:
            continue
        for word in dict:
            l = len(word)
            end = i + l
            if end > len(s):
                continue
            if s[i:end] == word:
                if dp[end] is None:
                    dp[end] = list()
                dp[end].append(word)

    result = list()
    # if dp[len(s)] is None:
    #     return "Not Possible"
    # return "Possible"
    if dp[len(s)] is None:
        return ["Not Possible"]
    dfs(dp, len(s), result, [])
    return result


def dfs(dp, end, result, tmp):
    if end <= 0:
        tmp.reverse()
        path = ' '.join(tmp)
        result.append(path)
        return

    for word in dp[end]:
        tmp.append(word)
        dfs(dp, end - len(word), result, tmp)
        tmp.remove(word)

if __name__ == '__main__':
    # print(get_entities('Panty liners'))
    # print(PorterStemmer().stem('blue eye liner'))
    # print(PorterStemmer().stem('beauties'))

    df = pd.read_csv('keywords.csv')
    df.drop(['Searches'], inplace=True, axis=1)

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
    df.to_csv('keyword_entity_7.csv', index=False)

    # print(get_entities('lakme 9 to 5 lipstick'))
    # print(get_entities('face primer'))
    # print(get_entities('lip balm'))
