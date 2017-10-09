dev.nykaa.com:8983/solr/autocomplete/update?stream.body=<delete><query>*:*</query></delete>&commit=true

#Sum of mrp of top products
curl http://localhost/apis/v1/category.list?category_id=18 | jq '.result.products' | jq 'map(.mrp)' | jq 'add'
