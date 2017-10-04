ase search
show tables 
db.raw_data.aggregate(
  [
    {"$match": {parent_id:{$in:['12217']}, platform: {$in: ["app",'web']}}}, 
    {"$group": {_id: {year:{"$year": "$date"}, "month": {$month: "$date"}}, orders: {$sum:"$orders"}, cart_additions: {$sum: "$cart_additions"}, views: {$sum:"$views"}, revenue: {$sum:"$revenue"}, units: {$sum:"$units"}}},
    {"$sort": {"_id.year":1, "_id.month":1, _id:1}},
    {"$out": "out"}
  ])
    
    {"$match": {date: {"$gte": ISODate("2017-08-01T"), "$lt": ISODate("2017-09-01T")}}},
    {"$group": {_id: "$date", orders: {$sum:"$orders"}, cart_additions: {$sum: "$cart_additions"}, views: {$sum:"$views"}}},
