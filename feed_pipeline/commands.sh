# MongoExport Data
mongoexport --db search --collection raw_data --out /tmp/raw_data.json
# Mongo Import Data
mongoimport --db search --collection popularity --file /tmp/popularity.json

 db.raw_data.update({date:{$gte: ISODate("2017-05-01T00:00:00Z")}}, {$set: {platform: "app"}}, {multi:true})
