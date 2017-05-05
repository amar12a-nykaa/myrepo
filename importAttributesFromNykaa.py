#!/usr/bin/python
import sys
sys.path.append('/home/apis/nykaa/')
import traceback
import mysql.connector
from contextlib import closing
from pas.v1.utils import Utils
from mysql.connector.errors import IntegrityError

class NykaaDBConnection:

  def connection():
    host = 'proddbnaykaa-2016-reports01.ciel4c1bqlwh.ap-southeast-1.rds.amazonaws.com'
    user = 'anik'
    password = 'slATy:2Rl9Me5mR'
    db = 'nykaalive1'

    return mysql.connector.connect(host=host, user=user, password=password, database=db)

  def fetchResults(connection, query, values=None):
    results = []
    with closing(connection.cursor()) as cursor:
      cursor.execute(query, values)
      results = [dict(zip([col[0] for col in cursor.description], row))
                   for row in cursor.fetchall()]
    return results

nykaa_mysql_conn = NykaaDBConnection.connection() 
query = "SELECT option_id, value FROM eav_attribute_option_value GROUP BY option_id"
results = NykaaDBConnection.fetchResults(nykaa_mysql_conn, query)

pws_mysql_conn = Utils.mysqlConnection('w')
pws_cursor = pws_mysql_conn.cursor()

for option in results:
  option_id = option['option_id'] 
  name = option['value'] 

  if name and name.strip():
    color_codes = []
    color_query = "SELECT * FROM colorfamily_codes WHERE color_id=%s"
    color_results = NykaaDBConnection.fetchResults(nykaa_mysql_conn, color_query, (option_id,))
    if color_results:
      color_codes = color_results[0]['color_code'].split(',') if color_results[0]['color_code'].strip() else []

    try:
      # Write to PWS API
      query = "INSERT INTO filter_attributes (id, name) VALUES (%s, %s)"
      pws_cursor.execute(query, (option_id, name))
      if color_codes:
        rows = []
        for code in color_codes:
          rows.append((option_id, code.strip()))
        query = "INSERT INTO attribute_values (attribute_id, value) VALUES (%s, %s)"
        pws_cursor.executemany(query, rows)

      pws_mysql_conn.commit()
    except IntegrityError as e:
      print("%s already exists in table"%option_id)
    except Exception as e:
      print("problem with %s: %s"%(option_id, str(e)))

pws_cursor.close()
pws_mysql_conn.close()
