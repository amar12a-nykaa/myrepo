
#Sum of mrp of top products
curl http://localhost/apis/v2/category.list?category_id=18 | jq '.result.products' | jq 'map(.mrp)' | jq 'add'
