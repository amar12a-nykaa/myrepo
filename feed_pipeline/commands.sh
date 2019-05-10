# MongoExport Data
mongoexport --db search --collection raw_data --out /tmp/raw_data.json
mongoexport --db search --collection popularity --out /tmp/popularity.json
scpny /tmp/popularity.json ubuntu@admin:/tmp/popularity.json

# Mongo Import Data
mongoimport --db search --collection raw_data --file /tmp/raw_data.json
mongoimport --db search --collection popularity --file /tmp/popularity.json

# 
db.raw_data.update({date:{$gte: ISODate("2017-05-01T00:00:00Z")}}, {$set: {platform: "app"}}, {multi:true})

# MongoDump DB
mongodump --gzip --db search --archive=/tmp/search.gz 
scpny /tmp/search.gz ubuntu@admin:/tmp/search.gz
# MongoRestore DB
mongorestore --gzip --db search --archive=/tmp/search.gz 

#Sum of mrp of top products
curl http://localhost/apis/v2/category.list?category_id=18 | jq '.result.products' | jq '.[0:10]' | jq 'map(.mrp)' | jq 'add'



40 - 30 - 10 - 10 Revenue
curl http://localhost/apis/v2/category.list?category_id=687 | jq '.result.products' | jq '.[0:10]' | jq 'map(.mrp)'  | jq add
7377

curl http://localhost/apis/v2/category.list?category_id=18 | jq '.result.products' | jq '.[0:10]' | jq 'map(.mrp)'  | jq add
2304

curl http://localhost/apis/v2/category.list?category_id=249| jq '.result.products' | jq '.[0:10]' | jq 'map(.mrp)'  | jq add
6773

