import traceback
import psycopg2
import mysql.connector
from contextlib import closing

import sys
sys.path.append('/home/ubuntu/nykaa_scripts/utils')
from recoutils import RecoUtils

env_details = RecoUtils.get_env_details()

class MysqlRedshiftUtils:

    def mysqlConnection(env, connection_details=None):
        if env_details['env'] == 'prod':
            host = "dbmaster.ciel4c1bqlwh.ap-southeast-1.rds.amazonaws.com"
            password = 'Cheaj92pDHtDq8hU'
        elif env_details['env'] in ['non_prod', 'preprod']:
            host = 'price-api-preprod.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
            password = 'yNKNy33xG'
        else:
            raise Exception('Unknow env')
        user = 'recommendation'
        #user = 'api'
        #password = 'aU%v#sq1'
        db = 'nykaa'
        for i in [0, 1, 2]:
            try:
                if connection_details is not None and isinstance(connection_details, dict):
                    connection_details['host'] = host
                    connection_details['user'] = user
                    connection_details['password'] = password
                    connection_details['database'] = db
                return mysql.connector.connect(host=host, user=user, password=password, database=db)
            except:
                print("MySQL connection failed! Retyring %d.." % i)
                if i == 2:
                    print(traceback.format_exc())
                    print("MySQL connection failed 3 times. Giving up..")
                    raise

    def mlMysqlConnection(connection_details=None):
        if env_details['env'] == 'prod':
            host = "ml-db-master.nykaa-internal.com"
            password = 'fjxcneXnq2gptsTs'
        elif env_details['env'] in ['non_prod', 'preprod', 'qa']:
            host = 'nka-preprod-ml.cjmplqztt198.ap-southeast-1.rds.amazonaws.com'
            password = 'JKaPHGB4JWXM'
        else:
            raise Exception('Unknow env')
        user = 'ml'
        db = 'nykaa_ml'
        for i in [0, 1, 2]:
            try:
                if connection_details is not None and isinstance(connection_details, dict):
                    connection_details['host'] = host
                    connection_details['user'] = user
                    connection_details['password'] = password
                    connection_details['database'] = db
                return mysql.connector.connect(host=host, user=user, password=password, database=db)
            except:
                print("MySQL connection failed! Retyring %d.." % i)
                if i == 2:
                    print(traceback.format_exc())
                    print("MySQL connection failed 3 times. Giving up..")
                    raise

    def redshiftConnection():
        if env_details['env'] == 'prod':
            host = 'dwhcluster.cy0qwrxs0juz.ap-southeast-1.redshift.amazonaws.com'
        elif env_details['env'] in ['non_prod', 'preprod', 'qa']:
            host = 'nka-preprod-dwhcluster.c742iibw9j1g.ap-southeast-1.redshift.amazonaws.com'
        else:
            raise Exception('Unknown env')
        port = 5439
        username = 'dwh_redshift_ro'
        password = 'GSrjC7hYPC9V'
        dbname = 'datawarehouse'
        con = psycopg2.connect(dbname=dbname, host=host, port=port, user=username, password=password)
        return con

    def fetchResultsInBatch(connection, query, batch_size):
        rows= []
        with closing(connection.cursor()) as cursor:
            cursor.execute(query)
            while True:
                batch_empty = True
                for row in cursor.fetchmany(batch_size):
                    batch_empty = False
                    rows.append(row)
                if batch_empty:
                    break
        return rows

    def _add_recommendations_in_mysql(cursor, table, rows):
        values_str = ", ".join(["(%s, %s, %s, %s, %s, %s)" for i in range(len(rows))])
        values = tuple([_i for row in rows for _i in row])
        insert_recommendations_query = """ INSERT INTO %s(catalog_tag_filter, entity_id, entity_type, recommendation_type, algo, recommended_products_json)
            VALUES %s ON DUPLICATE KEY UPDATE recommended_products_json=VALUES(recommended_products_json)
        """ % (table, values_str)
        values = tuple([str(_i) for row in rows for _i in row])
        #print(insert_recommendations_query)
        #print(values)
        cursor.execute(insert_recommendations_query, values)

    def add_recommendations_in_mysql(db, table, rows):
        cursor = db.cursor()
        for i in range(0, len(rows), 500):
            MysqlRedshiftUtils._add_recommendations_in_mysql(cursor, table, rows[i:i+500])
            db.commit()
