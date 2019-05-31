import records

N = 10000
db = records.Database('mysql://api:aU%v#sq1@price-api-qa.cjmplqztt198.ap-southeast-1.rds.amazonaws.com/nykaa')
rows = db.query('select sku, quantity, mrp from products limit ' + str(N))    # or db.query_file('sqls/active-users.sql')
print(rows.export('csv'))

