import argparse
import datetime
import json
import os
import os.path
import pprint
import re
import sys
import traceback
import re
import urllib.request
from collections import OrderedDict
from contextlib import closing

import arrow
import mysql.connector
import numpy
import omniture
import pandas as pd
import pymongo
from IPython import embed
from pymongo import UpdateOne
from stemming.porter2 import stem

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

sys.path.append("/nykaa/scripts/sharedutils")
from mongoutils import MongoUtils



from ensure_mongo_indexes import ensure_mongo_indices_now

from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize
ps = PorterStemmer()

DAILY_THRESHOLD = 3
POPULARITY_DECAY_FACTOR = 0.5

client = MongoUtils.getClient()
search_terms_daily = client['search']['search_terms_daily']
search_terms_daily_men = client['search']['search_terms_daily_men']
search_terms_normalized = client['search']['search_terms_normalized_daily']
search_terms_normalized_men = client['search']['search_terms_normalized_daily_men']
corrected_search_query = client['search']['corrected_search_query']

ensure_mongo_indices_now()

correct_term_list = ["correct words","everyuth","kerastase","farsali","krylon","armaf","Cosrx","focallure","ennscloset",
                     "studiowest","odonil","gucci","kryolan","sephora","foreo","footwear","rhoda","Fenty","Hilary","spoolie","jovees","devacurl","biore",
                     "quirky","stay","parampara","dermadew","kokoglam","embryolisse","tigi","mediker","dermacol","Anastasia","essie","sale","bajaj","burberry",
                     "sesa","sigma","spencer","puna","modicare","hugo","gelatin","stila","ordinary","spawake","mederma","mauri","benetint","amaira","meon","tony",
                     "renee","boxes","aashka","prepair","meilin","krea","dress","sivanna","etude","kadhi","laneige","gucci","jaclyn","hilary",
                     "anastasia","becca","sigma","farsali","majirel","satthwa","fenty","vibrator","focallure","krylon","tigi","armaf","cosrx","soumis","studiowest",
                     "evion","darmacol","odonil","comedogenic","suthol","suwasthi","kerastase","nexus","footwear","badescu","rebonding","jeffree","devacurl","odbo",
                     "sesderma","tilbury","dildo","glatt","essie","ethiglo","prada","dermadew","trylo","nycil","cipla","biore","giambattista","luxliss","parampara",
                     "dyson","episoft","vcare","ofra","nizoral","elnett","mediker","photostable","urbangabru","ketomac","popfeel","igora","wishtrend","jefree",
                     "hillary","skeyndor","raga","dresses","protectant","boroline","burts","seth","gelatin","panstick","goradia","policy","crimping","bajaj",
                     "spencer","glue","reloaded","dior","hoola","opticare","mario","bees","prepair","clothes","oxyglow","chapstick","stencil","smokers","clutcher",
                     "supra","artistry","cerave","suncross","suncare","stilla","skinfood","caprese","cantu","cameleon","glamego","hamdard","arish","soda","benzoyl",
                     "tape","catrice","liplove","clarina","glister","colorista","mistral","scholl","bathrobe","fastrack","christian","mauri","emolene","bioaqua",
                     "lodhradi","chance","bedhead","dawn","bake","persistence","bloating","carmex","roulette","acnestar","safi","flormar","margo","nudestix","instaglam"]

def create_missing_indices():
  indices = search_terms_daily.list_indexes()
  if 'query_1' not in [x['name'] for x in indices]:
    print("Creating missing index .. ")
    search_terms_daily.create_index([("query", pymongo.ASCENDING)])
  indices_men = search_terms_daily_men.list_indexes()
  if 'query_1' not in [x['name'] for x in indices_men]:
    print("Creating missing index .. ")
    search_terms_daily_men.create_index([("query", pymongo.ASCENDING)])

create_missing_indices()

def get_corrections_map():
    # url = "https://nyk-aggregator-api.nykaa.com/api/getRedisData?key_type=query_replace&nested=true"
    # response = json.loads(urllib.request.urlopen(url).read().decode('utf-8'))
    response = {}
    df = pd.read_csv("/nykaa/scripts/correction_file.csv")
    for i, row in df.iterrows():
        row = dict(row)
        response[row["query"]] = row["corrected_query"]
    return response

def special_chars_present(query):
    blacklisted_terms = ["fairness", "fair", "whitening", "lightening"]
    try:
        if re.search(r'%2[0-9A-F]', query):
            return True
        for term in blacklisted_terms:
            pattern = '\\b' + term + '\\b'
            match = re.search(pattern, query)
            if match:
                return True
    except:
        pass
    return False


CORRECTIONS_MAP = get_corrections_map()

def is_result_present(query,store):
    must_not = []
    must = []

    must_not.append({"term": {"is_service": True}})
    must.append({"term": {"is_searchable_i": 1}})
    must.append(
        {
            "multi_match": {
                "minimum_should_match": '100%',
                "query": query,
                "fuzziness": 0,
                "fields": ["title_brand_category.shingle"],
                "prefix_length": 1,
            }
        }
    )
    if store == 'men':
        must.append({"terms": {"catalog_tag.keyword": ['men']}})
    else:
        must.append({"terms": {"catalog_tag.keyword": ['nykaa']}})
    count_querydsl = {"query": {"bool": {"must": must, "must_not": must_not}}}
    es = DiscUtils.esConn()
    #print("Count query dsl:", count_querydsl)
    response_count = es.count(body=count_querydsl, index='livecore')
    docs_count = response_count['count']
    if docs_count > 0:
        return True
    return False

def getQuerySuggestion(query_id, query, algo):
    global CORRECTIONS_MAP
    if query in CORRECTIONS_MAP:
        modified_query = CORRECTIONS_MAP.get(query)
        return modified_query
    for term in corrected_search_query.find({"_id": query_id, "query" : query}):
        modified_query = term["suggested_query"]
        return modified_query

    if algo == 'default':
        es_query = """{
             "size": 1,
             "query": {
                 "match": {
                   "title_brand_category": "%s"
                 }
             }, 
             "suggest": 
              { 
                "text":"%s", 
                "term_suggester": 
                { 
                  "term": { 
                    "field": "title_brand_category"
                  }
                }
              }
        }""" % (query,query)
        es_result = DiscUtils.makeESRequest(es_query, "livecore")
        doc_found = es_result['hits']['hits'][0]['_source']['title_brand_category'] if len(es_result['hits']['hits']) > 0 else ""
        doc_found = doc_found.lower()
        doc_found = doc_found.split()
        if es_result.get('suggest', {}).get('term_suggester'):
            modified_query = query.lower()
            for term_suggestion in es_result['suggest']['term_suggester']:
                if term_suggestion.get('text') in doc_found:
                    continue
                if term_suggestion.get('text') in correct_term_list:
                    continue
                if term_suggestion.get('options') and term_suggestion['options'][0]['score'] > 0.7:
                    frequency = term_suggestion['options'][0]['freq']
                    new_term = term_suggestion['options'][0]['text']
                    for suggestion in term_suggestion['options'][1:]:
                        if suggestion['freq'] > frequency and suggestion['score'] > 0.7:
                            frequency = suggestion['freq']
                            new_term = suggestion['text']
                    modified_query = modified_query.replace(term_suggestion['text'], new_term)
            return modified_query
        return query
    return query

def normalize(a):
    if not max(a) - min(a):
        return 0
    return (a - min(a)) / (max(a) - min(a))

def normalize_search_terms(store):
    assert store in ['nykaa', 'men']
    if store == 'nykaa':
        coll_search_terms = search_terms_daily
        coll_search_normalized = search_terms_normalized
    else:
        coll_search_terms = search_terms_daily_men
        coll_search_normalized = search_terms_normalized_men
    coll_search_normalized.remove({})
    print("Normalizing:", store)
    date_buckets = [(0,15),(16,30),(31,45),(46,60),(61,75),(76,90),(91,105),(106,120),(121,135),(136,150),(151,165),(165,180)]
    dfs = []
    ignore_window_start = arrow.get('2020-03-24', 'YYYY-MM-DD').datetime.replace(tzinfo=None)
    ignore_window_end = arrow.get('2020-05-03', 'YYYY-MM-DD').datetime.replace(tzinfo=None)
    bucket_results = []
    for bucket_id, date_bucket in enumerate(date_buckets):
        startday = date_bucket[1] * -1 
        endday = date_bucket[0] * -1 
        startdate = arrow.now().replace(days=startday, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(tzinfo=None) 
        enddate = arrow.now().replace(days=endday, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(tzinfo=None)
        print(startdate, enddate)
        if startdate >= ignore_window_start and enddate < ignore_window_end:
            continue
        if enddate > ignore_window_start and enddate < ignore_window_end:
            enddate = ignore_window_start
        elif startdate < ignore_window_end and enddate > ignore_window_end:
            startdate = ignore_window_end
        print("calculating for %s %s"%(startdate, enddate))
        bucket_results = []
        # TODO need to set count sum to count
        for term in coll_search_terms.aggregate([
            {"$match": {"date": {"$gte": startdate, "$lte": enddate},"internal_search_term_conversion_instance": {"$gte": DAILY_THRESHOLD}}},
            {"$project": {"term": {"$toLower": "$term"}, "date": "$date","count": "$internal_search_term_conversion_instance"}},
            {"$group": {"_id": "$term", "count": {"$sum": "$count"}}},
            {"$sort": {"count": -1}},], allowDiskUse=True):
            term['id'] = term.pop("_id")
            bucket_results.append(term)
        

        if not bucket_results:
            print("Skipping popularity computation")
            continue
        
        print("Computing popularity")
        df = pd.DataFrame(bucket_results)
        df['norm_count'] = normalize(df['count'])
        multiplication_factor = POPULARITY_DECAY_FACTOR ** (bucket_id + 1)
        df['popularity'] = multiplication_factor * (df['norm_count'])
        dfs.append(df.loc[:, ['id', 'popularity']].set_index(['id']))

    if dfs:
        final_df = dfs[0]
        for i in range(1, len(dfs)):
            final_df = pd.DataFrame.add(final_df, dfs[i], fill_value=0)
        final_df.popularity = final_df.popularity.fillna(0)
        # final_df['popularity_recent'] = 100 * normalize(final_df['popularity'])
        # final_df.drop(['popularity'], axis=1, inplace=True)
        #print(final_df)

    # Calculate total popularity
    # print ("Calculating total popularity")
    # date_6months_ago = arrow.now().replace(days=-6*30, hour=0, minute=0, second=0, microsecond=0, tzinfo=None).datetime.replace(tzinfo=None)
    #
    # bucket_results = []
    # for term in search_terms_daily.aggregate([
    #     #{"$match": {"term" : {"$in": ['Lipstick', 'nars']}}},
    #     {"$match": {"date": {"$gte": date_6months_ago}, "internal_search_term_conversion_instance": {"$gte": DAILY_THRESHOLD}}},
    #     #{"$match": {"date": {"$gte": date_6months_ago, "$lte": enddate}, "internal_search_term_conversion_instance": {"$gte": DAILY_THRESHOLD}}},
    #     {"$project": {"term": { "$toLower": "$term"}, "date":"$date","count": "$internal_search_term_conversion_instance" }},
    #     {"$group": {"_id": "$term", "count": {"$sum": "$count"} }},
    #     {"$sort":{ "count": -1}},
    # ], allowDiskUse=True):
    #     term['id'] = term.pop("_id")
    #     bucket_results.append(term)
    #
    # df = pd.DataFrame(bucket_results)
    # df['norm_count'] = normalize(df['count'])
    # df['popularity_total'] = df['norm_count']
    # df = df.set_index('id')

    # a = pd.merge(df, final_df, how='outer', left_index=True, right_index=True).reset_index()
    a = final_df.reset_index()
    # a.popularity_recent = a.popularity_recent.fillna(0)
    # a['popularity'] = 100 * normalize(0.7 * a['popularity_total'] + 0.3 * a['popularity_recent'])
    # a.popularity = a.popularity.fillna(0)

    requests = []
    corrections = []
    #brand_index = normalize_array("select brand as term from nykaa.brands where brand like 'l%'")
    #category_index = normalize_array("select name as term from nykaa.l3_categories")

    coll_search_normalized.remove({})
    cats_stemmed = set([ps.stem(x['name']) for x in PasUtils.mysql_read("select name from l3_categories ")])
    brands_stemmed = set([ps.stem(x['brand']) for x in PasUtils.mysql_read("select brand from brands")])
    cats_brands_stemmed = cats_stemmed | brands_stemmed

    for i, row in a.iterrows():
        query = row['id'].lower()
        if ps.stem(query) in cats_brands_stemmed:
            a.drop(i, inplace=True)
        elif special_chars_present(query):
            a.drop(i, inplace=True)
    
    a['popularity'] = 50 * normalize(a['popularity'])
    a = a.sort_values(by='popularity')
    for i, row in a.iterrows():
        query = row['id'].lower()
        if ps.stem(query) in cats_brands_stemmed:
          continue

        algo = 'default'
        query_id = re.sub('[^A-Za-z0-9]+', '_', row['id'].lower())

        try:
            suggested_query = getQuerySuggestion(query_id, query, algo)
            nz_query = is_result_present(suggested_query, store)
            #print("suggested_query: ",suggested_query)
            #print("nzquery:", nz_query)
            if store == 'men' and nz_query is False:
                pass
            else:
                requests.append(UpdateOne({"_id":  query_id}, {"$set": {"query": row['id'].lower(), 'popularity': row['popularity'],
                                        "suggested_query": suggested_query.lower(), "nz_query": nz_query}}, upsert=True))

            corrections.append(UpdateOne({"_id":  query_id},
                                         {"$set": {"query": row['id'].lower(),"suggested_query": suggested_query.lower(), "algo": algo}},upsert=True))
        except:
            print(traceback.format_exc())
            print(row)
        #print("--------------Writing in MongoDb in batch of 10,000-------------     "+str(startdate)+"------"+str(enddate))
        if i % 10000 == 0:
            if requests:
                print("---------------Writing in Normalized_search of: ------------------",store)
                coll_search_normalized.bulk_write(requests)
                print("-------------Writing ends here----------------------------------",store)
            else:
                print("Requests is empty !! Nothing to write in collection_search_normalized of:",store)
            corrected_search_query.bulk_write(corrections)
            requests = []
            corrections = []
    if requests:
        coll_search_normalized.bulk_write(requests)
        corrected_search_query.bulk_write(corrections)




if __name__ == "__main__":
    for store in ['nykaa']:
        normalize_search_terms(store)
