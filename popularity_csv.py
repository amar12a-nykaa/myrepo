from pymongo import MongoClient

client = MongoClient("172.30.3.5")
popularity_coll = client['search']['popularity']

print(popularity_coll.find_one())
#res = list(popularity_coll.aggregate([{"$match":{"productid" : {"$in":["14668", "14670", "14681", "14682", "14687", "14690", "14692", "14695", "14696", "14709"]}}}, {"$sort": {"popularity":-1}}]))

res = list(popularity_coll.aggregate([{"$match":{"productid" : {"$in":["37835", "10976", "18500", "5997", "35523", "28273", "3062", "28076", "1279", "5989"]}}}, {"$sort": {"popularity":-1}}]))
#print(res)
import csv
with open('/tmp/names.csv', 'w') as csvfile:
	fieldnames = ['productid', 'orders', 'On', 'views', 'popularity', 'Cn', 'cart_additions', 'Vn', '_id']
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

	writer.writeheader()

	for prod in res:
		writer.writerow(prod)

