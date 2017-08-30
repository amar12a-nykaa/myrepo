from pymongo import MongoClient 

client = MongoClient()

raw_data = client['search']['raw_data']
processed_data = client['search']['processed_data']
popularity = client['search']['popularity']

last_date  = raw_data.aggregate([{"$max":"$date"}])
