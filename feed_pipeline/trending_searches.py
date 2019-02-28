import os
import sys
import pandas as pd
import re
from nltk.stem import PorterStemmer
# from utils import EntityUtils
from datetime import date, timedelta, datetime

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

porter = PorterStemmer()
previous = ''

def word_clean(word):
    word = str(word).lower()
    ls = re.split('[^A-Za-z+&0-9]', str(word))
    l = []
    for e in ls:
        l.append(porter.stem(e))
    word = "".join(l)
    return word

def get_trending_searches():
    file_path = '/nykaa/scripts/feed_pipeline/trending.csv'
    df = pd.read_csv(file_path)
    # renaming columns
    df.columns = ['date', 'ist', 'frequency', 'ctr']
    df.drop(df[(df.frequency < 10) | (df.ctr < 10)].index,inplace=True)
    # changing date format
    df['date'] = [datetime.strptime(x, '%B %d, %Y') for x in df['date']]
    df = df.astype({"date": str})
    previous=df['date'].max()

    df['cleaned_term'] = df['ist'].map(word_clean)
    idx = df.groupby(['cleaned_term'])['frequency'].transform(max) == df['frequency']
    temp=pd.DataFrame
    temp = df[idx]

    temp = temp.groupby(['cleaned_term'],as_index=False).agg({'ist': 'first'})
    # grouping all the exact matched terms on same date with aggregation on freq,ctr
    df = df.groupby(['cleaned_term', 'date'], as_index=False).agg({'frequency': 'sum','ctr': 'sum'})
    #df.to_csv('/home/abc/temp1.csv')

    df = pd.merge(df, temp, on='cleaned_term')

    df.drop(df[(df.frequency < 100) & ((df.date) == (previous))].index, inplace=True)
    #df = df.groupby(['cleaned_term'], as_index=False).filter(lambda x: x['date'].max() == (previous))

    df_prev = df.ix[df.date == previous]
    df_prev = df_prev.drop(['date', 'ctr', 'ist'], axis=1)
    df_prev.columns = ['cleaned_term', 'frequency_prev']


    df = df.groupby(['cleaned_term','ist'], as_index=False).agg({'frequency': 'sum','ctr': 'sum'})

    #print(df.shape)
    df = pd.merge(df, df_prev, on='cleaned_term',how='left')
    df = df.dropna()
    #print(df.shape)
    df = df.astype({"frequency_prev": int})
    #df.to_csv('/home/abc/test.csv')

    filter = ((df.frequency==df.frequency_prev)) | ((df.frequency>df.frequency_prev) & df.frequency_prev/(df.frequency-df.frequency_prev) < 0.6)

    df = df.drop(df[ filter].index)

    df=df.drop(df[(df.ctr / df.frequency) < 0.3].index)
    df = df.sort_values(['frequency', 'ctr'], ascending=False)

    return df.head(5)

def insert_trending_searches(data):
    mysql_conn = Utils.mysqlConnection('w')
    cursor = mysql_conn.cursor()

    if not Utils.mysql_read("SHOW TABLES LIKE 'trending_searches'", connection=mysql_conn):
        Utils.mysql_write("create table trending_searches(type VARCHAR(64),url VARCHAR(255),q VARCHAR(255))",
                          connection=mysql_conn)
    Utils.mysql_write("delete from trending_searches", connection=mysql_conn)

    for index, row in data.iterrows():
        word = row['ist']
        ls = word.split()
        word = " ".join(ls)
        url = "/search/result/?q=" + word.replace(" ", "+")
        print(url)
        values = ('query',url, word)
        query = """INSERT INTO trending_searches (type, url,q) VALUES ("%s","%s","%s") """ % (values)

        cursor.execute(query)
        mysql_conn.commit()

    cursor.close()
    mysql_conn.close()


if __name__ == '__main__':
    data = pd.DataFrame
    data = get_trending_searches()

    insert_trending_searches(data)