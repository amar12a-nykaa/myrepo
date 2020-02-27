import argparse
import json
import pandas as pd
import sys
import time
import elasticsearch
import os

sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter
from esutils import EsUtils
from idutils import createId

sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

feedback_collection = 'feedback'
ES_SCHEMA = json.load(open(os.path.join(os.path.dirname(__file__), 'schema.json')))


def index_data(collection, filename):
    data = pd.read_csv(filename, chunksize=500)
    ctr = LoopCounter(name='Feedback Indexing')
    for chunk in data:
        docs = []
        for i, row in chunk.iterrows():
            ctr += 1
            if ctr.should_print():
                print(ctr.summary)
            row = dict(row)
            doc = {
                "_id": createId(row['search_term']+str(row['product_id'])),
                "weight": row["normalized_revenue"],
                "query": row["search_term"],
                "f_id": row["product_id"],
                "f_length": len(row["search_term"].split()),
                "store": "nykaa"
            }
            docs.append(doc)
        EsUtils.indexDocs(docs, collection)
    return


def index_engine(collection=None, active=None, inactive=None, swap=False, filename=None):
    assert len([x for x in [collection, active, inactive] if x]) == 1, "Only one of the following should be true"
    index = None

    if collection:
        index = collection
    elif active:
        index = EsUtils.get_active_inactive_indexes(feedback_collection)['active_index']
    elif inactive:
        index = EsUtils.get_active_inactive_indexes(feedback_collection)['inactive_index']
    if not index:
        return False

    print("Index: %s" % index)

    es = DiscUtils.esConn()
    index_client = elasticsearch.client.IndicesClient(es)
    if index_client.exists(index=index):
        print("Deleting index: %s" % index)
        index_client.delete(index=index)
    index_client.create(index=index, body=ES_SCHEMA)

    index_data(index, filename)

    if swap:
        print("Swapping Index")
        indexes = EsUtils.get_active_inactive_indexes(feedback_collection)
        EsUtils.switch_index_alias(feedback_collection, indexes['active_index'], indexes['inactive_index'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    collection_state = parser.add_mutually_exclusive_group(required=True)
    collection_state.add_argument("--inactive", action='store_true')
    collection_state.add_argument("--active", action='store_true')
    collection_state.add_argument("--collection")
    parser.add_argument("--swap", action='store_true', help="Swap the Core")
    parser.add_argument("--filename", '-f', type=str, required=True)

    argv = vars(parser.parse_args())

    startts = time.time()
    index_engine(collection=argv['collection'], active=argv['active'], inactive=argv['inactive'], swap=argv['swap'], filename=argv['filename'])
    mins = round((time.time() - startts) / 60, 2)
    print("Time taken: %s mins" % mins)
