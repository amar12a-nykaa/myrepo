import argparse
import sys

sys.path.append("/home/shweta/nykaa/fresh_nykaa_apis")
from pas.v2.utils import Utils

def process_guides(filename='guide.csv'):
    pass

def getEntities(query):
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
      print("%s %s" % (entity_name, query))
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Argument parser for feedback result')
    parser.add_argument('--filename', '-f', type=str, default='guide.csv')

    argv = vars(parser.parse_args())
    filename = argv['filename']
    process_guides(filename)
    # print(getEntities("liquid lipstick"))
    # print(getEntities("lakme lipstick"))
    # print(getEntities("nude matte lipstick"))
    # print(getEntities("Nykaa Cosmetics"))
    # print(getEntities("Makeup Revolution"))
    # print(getEntities("liquid matte lipstick"))
    # print(getEntities("lips"))
    # print(getEntities("The Body Shop"))
    # print(getEntities("perfumes for women"))
    # print(getEntities("red lipstick"))
    # print(getEntities("pink lipstick"))

    