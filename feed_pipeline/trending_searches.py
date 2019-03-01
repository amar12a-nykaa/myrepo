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

def group_filter(x,prev,algo):
    data = pd.DataFrame(x)
    if  data['date'].max() != prev:
        return False

    if algo == 3:
        l = len(data.index)
        if l > 2:
            mid = l // 2 + 1
            data = data.sort_values(['date'])
            oldSum = data[:mid]['frequency'].sum()
            newSum = data[-(l - mid):]['frequency'].sum()
            if newSum > 2.5 * oldSum:
                return True
        else:
            sum = data['frequency'].sum()
            if (sum > 500):
                print(data.ist)
                return True
        return False

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

    df = df.groupby(['cleaned_term', 'ist']).filter(group_filter, prev=previous, algo=3)
    #df.to_csv('temp.csv')
    # print(df.head(5))
    df = df.groupby(['cleaned_term', 'ist']).agg({'frequency': 'sum', 'ctr': 'sum'})
    df = df.drop(df[(df.ctr / df.frequency) < 0.25].index)
    df = df.sort_values(['frequency', 'ctr'], ascending=False)

    #df.to_csv('output_algo3.csv')

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
    print(data)
    insert_trending_searches(data)