from pymongo import MongoClient

client = MongoClient()
popularity_coll = client['search']['popularity']

print(popularity_coll.find_one())
