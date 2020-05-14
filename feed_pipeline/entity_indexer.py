#!/usr/bin/python
import argparse
import json
import pandas as pd
import sys
import os
from nltk.stem import PorterStemmer

sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter
from esutils import EsUtils
from idutils import createId
from categoryutils import getVariants

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/nykaa/scripts/utils")
import searchutils as SearchUtils


filter_attribute_map = [("656","concern"), ("661","preference"), ("659","formulation"), ("664","finish"), ("658","color"),
                        ("655","gender"), ("657","skin_type"), ("677","hair_type"), ("857","ingredient"),
                        ("665","skin_tone"), ("663","coverage"), ("812","wiring"), ("813","padding"),
                        ("815","fabric"), ("822","pattern"), ("823","rise")]
FILTER_WEIGHT = 50
ASSORTMENT_WEIGHT = 1


class EntityIndexer:
  DOCS_BATCH_SIZE = 1000

  
  def index_assortment_gap(collection):
    docs = []
    df = pd.read_csv('/nykaa/scripts/feed_pipeline/entity_assortment_gaps_config.csv')
    brand_list = list(df['Brands'])

    ctr = LoopCounter(name='Assortment Gap Indexing')
    for row in brand_list:
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)

      assortment_doc = {
        "_id": createId(row),
        "entity": row,
        "weight": ASSORTMENT_WEIGHT,
        "type": "assortment_gap",
        "id": ctr.count
      }
      for tag in SearchUtils.VALID_CATALOG_TAGS:
        store_doc = assortment_doc.copy()
        store_doc['store'] = tag
        store_doc['_id'] = assortment_doc['_id'] + tag
        docs.append(store_doc)
      
      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

      print(row, ctr.count)

    EsUtils.indexDocs(docs, collection)

  
  def index_brands(collection):
    docs = []
  
    synonyms = {
      'The Body Shop' : ['Body Shop'],
      "The Face Shop": ["Face Shop"],
      "The Body Care": ["Body Care"],
      "Make Up Forever": ["Makeup Forever"],
      "Maybelline New York": ["Maybelline"],
      "NYX Professional Makeup": ["NYX"],
      "Twenty Dresses By Nykaa Fashion": ["Twenty Dresses"],
      "Mondano By Nykaa Fashion": ["Mondano"],
      "RSVP By Nykaa Fashion": ["RSVP"],
      "Huda Beauty": ["Huda"],
      "Vaadi Herbals": ["Vaadi"],
      "Kama Ayurveda": ["Kama"],
      "Layer'r": ["Layer"],
    }
    mysql_conn = PasUtils.mysqlConnection()
    query = "SELECT brand_id, brand, brand_popularity, store_popularity, brand_url FROM brands ORDER BY brand_popularity DESC"
    results = PasUtils.fetchResults(mysql_conn, query)
    ctr = LoopCounter(name='Brand Indexing')
    for row in results:
      ctr += 1 
      if ctr.should_print():
        print(ctr.summary)

      brand_doc = {
        "_id": createId(row['brand']), 
        "entity": row['brand'], 
        "weight": row['brand_popularity'], 
        "type": "brand",
        "id": row['brand_id']
      }

      if row['brand'] in synonyms:
        brand_doc["entity_synonyms"] = synonyms[row['brand']]
      
      store_popularity = json.loads(row['store_popularity'])
      for tag, value in store_popularity.items():
        if value <= 0.0001:
          continue
        store_doc = brand_doc.copy()
        store_doc['store'] = tag
        store_doc['_id'] = brand_doc['_id'] + tag
        docs.append(store_doc)
      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        docs = []

      print(row['brand'], ctr.count)

    EsUtils.indexDocs(docs, collection)


  def index_categories(collection):
    query = """SELECT id as category_id, name as category_name, url, category_popularity, store FROM l3_categories
      where url not like '%shop-by-concern%' order by store, name, category_popularity desc"""
    df_cat = EntityIndexer.get_category('category', query)
    
    query = """SELECT id as category_id, name as category_name, category_popularity, store FROM all_categories
                        order by store, name, category_popularity desc"""
    df_l1_cat = EntityIndexer.get_category('l1_category', query)
    df = df_cat.append([df_l1_cat])
    
    category_priority = pd.read_csv('primary_category.csv')
    category_priority.id = category_priority.id.astype(str)
    category_priority["factor"] = 100
    df = pd.merge(df, category_priority, how="left", on="id")
    df.factor = df.factor.fillna(1)
    df.weight = df.apply(lambda x: x['weight']*x['factor'], axis=1)
    df = df.sort_values(by='weight', ascending=False)
    primary_cat = df.drop_duplicates(subset=['store', 'name_id'], keep='first', inplace=False)
    df.id = df.id.astype(str)
    secondary_cat = df.groupby(['store', 'name_id']).id.agg([('id', ','.join)]).reset_index()
    secondary_cat.rename(columns={'id': 'secondary_ids'}, inplace=True)
    secondary_cat_types = df.groupby(['store', 'name_id']).type.agg([('type', ','.join)]).reset_index()
    secondary_cat_types.rename(columns={'type': 'category_types'}, inplace=True)
    secondary_cat = pd.merge(secondary_cat, secondary_cat_types, on=['store', 'name_id'])
    data = pd.merge(primary_cat, secondary_cat, on=['store', 'name_id'])

    ctr = LoopCounter(name='Category Indexing')
    docs = []
    final = pd.DataFrame()
    for id, row in data.iterrows():
      row = dict(row)
      ctr += 1
      if ctr.should_print():
        print(ctr.summary)
      types = row.pop('category_types')
      types = set(types.split(','))
      for type in types:
        id = type + "_" + str(row['id']) + "_" + row['entity'] + "_" + row['store']
        secondary_ids = ','.join(list(set(row['secondary_ids'].split(','))))
        docs.append({"_id": createId(id), "entity": row['entity'], "weight": row['weight'],
                     "type": type, "id": row['id'],"store": row['store'], 'secondary_ids': secondary_ids})
      if len(docs) >= 100:
        EsUtils.indexDocs(docs, collection)
        final = final.append(docs, ignore_index=True)
        docs = []
    final = final.append(docs, ignore_index=True)
    EsUtils.indexDocs(docs, collection)
  

  def get_category(type, query):
    stemmer = PorterStemmer()

    def getCategoryDoc(row, variant):
      variant = variant.strip()
      
      doc = {
        "entity": variant,
        "weight": row['category_popularity'],
        "type": type,
        "id": row['category_id'],
        "name_id": stemmer.stem(variant),
        "store": row['store']
      }
      return doc
    
    mysql_conn = PasUtils.mysqlConnection()
    results = PasUtils.fetchResults(mysql_conn, query)
    final_list = []
    
    for row in results:
      variants = getVariants(row['category_id'])
      if variants:
        for variant in variants:
          final_list.append(getCategoryDoc(row, variant))
      else:
        final_list.append(getCategoryDoc(row, row['category_name']))
    df = pd.DataFrame()
    df = df.append(final_list, ignore_index=True)
    return df

  
  def index_filters(collection):
    mysql_conn = PasUtils.nykaaMysqlConnection()
    synonyms = {'10777': {'name': 'Acne/Blemishes', 'synonym': ['acne', 'anti acne', 'blemishes', 'anti blemishes']},
                '10753': {'name': 'Dark Spots/Pigmentation', 'synonym': ['dark spots', 'pigmentation', 'anti pigmentation']},
                '10771': {'name': 'Pores/Blackheads/Whiteheads', 'synonym': ['pores', 'blackheads', 'whiteheads']},
                '10749': {'name': 'Fine Lines/Wrinkles', 'synonym': ['fines lines', 'wrinkle', 'anti wrinkle']},
                '10776': {'name': 'Anti-ageing', 'synonym': ['ageing', 'anti ageing']},
                '10770': {'name': 'Brightening/Fairness', 'synonym': ['brightening', 'fairness']},
                '91637': {'name': 'Hairfall & Thinning', 'synonym': ['anti hairfall', 'hairfall', 'thinning']},
                '91638': {'name': 'Dry & Frizzy Hair', 'synonym': ['dry hair', 'frizzy hair']},
                '10755': {'name': 'Dandruff', 'synonym': ['anti dandruff']},
                '80231': {'name': 'Tan Removal', 'synonym': ['tan', 'anti tan', 'de tan']},
                '12089': {'name': 'Lotion/Body Butter', 'synonym': ['lotion', 'body butter']},
                '10711': {'name': 'Female', 'synonym': ['women', 'woman', 'ladies']},
                '10710': {'name': 'Male', 'synonym': ['men', 'man']},
                '67293': {'name': 'Solid/Plain', 'synonym': ['solid', 'plain']},
                '96358': {'name': 'Embellished/Sequined', 'synonym': ['embellished', 'sequined']},
                '10887': {'name': 'Medium/Wheatish Skin', 'synonym': ['medium skin', 'wheatish skin']},
                '10886': {'name': 'Fair/Light Skin', 'synonym': ['fair skin', 'light skin']},
                '10888': {'name': 'Dusky/Dark Skin', 'synonym': ['dusky skin', 'dark skin']},
                '11075': {'name': 'Normal hair'},
                '11079': {'name': 'Curly hair'},
                '11073': {'name': 'Straight hair'},
                '11076': {'name': 'Fine hair'},
                '11074': {'name': 'Oily hair'},
                '11078': {'name': 'Dry hair'},
                '11077': {'name': 'Dull Hair'},
                '11072': {'name': 'Thick hair'},
                '11071': {'name': 'Thin hair'},
                '91639': {'name': 'Wavy hair'},
                '91643': {'name': 'Argan'},
                '10781': {'name': 'Dry skin'},
                '10779': {'name': 'Oily skin'},
                '10780': {'name': 'Normal skin'},
                '10778': {'name': 'Sensitive skin'},
                '10782': {'name': 'Combination skin'},
    }
    for filt in filter_attribute_map:
      id = filt[0]
      filter = filt[1]
      query = """select eov.value as name, eov.option_id as filter_id from eav_attribute_option eo join eav_attribute_option_value eov
                    on eo.option_id = eov.option_id and eov.store_id = 0 where attribute_id = %s"""%id
      results = PasUtils.fetchResults(mysql_conn, query)
      ctr = LoopCounter(name='%s Indexing' % filter)
      docs = []
      for row in results:
        ctr += 1
        if ctr.should_print():
          print(ctr.summary)

        filter_doc = {
          "_id": createId(row['name']),
          "entity": row['name'].strip(),
          "weight": FILTER_WEIGHT,
          "type": filter,
          "id": str(row['filter_id'])
        }
        if filter_doc["id"] in synonyms:
          filter_doc["entity"] = synonyms[filter_doc["id"]]["name"]
          filter_doc["_id"] = createId(filter_doc["entity"])
          if 'synonym' in synonyms[filter_doc["id"]]:
            filter_doc["entity_synonyms"] = synonyms[filter_doc["id"]]["synonym"]
        for tag in SearchUtils.VALID_CATALOG_TAGS:
          store_doc = filter_doc.copy()
          store_doc['store'] = tag
          store_doc['_id'] = filter_doc['_id'] + tag
          docs.append(store_doc)
        if len(docs) >= 100:
          EsUtils.indexDocs(docs, collection)
          docs = []

        print(filter, filter_doc["entity"], filter_doc["id"])
      EsUtils.indexDocs(docs, collection)

  
  def index(collection=None, active=None, inactive=None, swap=False, index_categories_arg=False,
                      index_brands_arg=False, index_filters_arg=False, index_all=False):
    index = None
    print('Starting Processing')
    if collection: 
      index = collection 
    elif active:
      index = EsUtils.get_active_inactive_indexes('entity')['active_index']
    elif inactive:
      index = EsUtils.get_active_inactive_indexes('entity')['inactive_index']
    else:
      index = None

    if index_all:
      index_categories_arg = True
      index_brands_arg = True
      index_filters_arg = True

    if index:
      #clear the index
      index_client = EsUtils.get_index_client()
      if index_client.exists(index):
        print("Deleting index: %s" % index)
        index_client.delete(index)
      schema = json.load(open(os.path.join(os.path.dirname(__file__), 'entity_schema.json')))
      index_client.create(index, schema)
      print("Creating index: %s" % index)

      EntityIndexer.index_assortment_gap(index)
      if index_filters_arg:
        EntityIndexer.index_filters(index)
      if index_categories_arg:
        EntityIndexer.index_categories(index)
      if index_brands_arg:
        EntityIndexer.index_brands(index)

    if swap:
      print("Swapping Index")
      indexes = EsUtils.get_active_inactive_indexes('entity')
      EsUtils.switch_index_alias('entity', indexes['active_index'], indexes['inactive_index'])


if __name__ == "__main__":
  parser = argparse.ArgumentParser()

  group = parser.add_argument_group('group')
  group.add_argument("-c", "--category", action='store_true')
  group.add_argument("-b", "--brand", action='store_true')
  group.add_argument("--filters", action='store_true')

  collection_state = parser.add_mutually_exclusive_group(required=True)
  collection_state.add_argument("--inactive", action='store_true')
  collection_state.add_argument("--active", action='store_true')
  collection_state.add_argument("--collection")
  
  parser.add_argument("--swap", action='store_true', help="Swap the Core")
  argv = vars(parser.parse_args())

  required_args = ['category', 'brand', 'filters']
  index_all = not any([argv[x] for x in required_args])

  EntityIndexer.index(collection=argv['collection'], active=argv['active'], inactive=argv['inactive'],
                            swap=argv['swap'], index_categories_arg=argv['category'], index_brands_arg=argv['brand'],
                            index_filters_arg=argv['filters'], index_all=index_all)
