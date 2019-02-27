import os
import sys
import time
import json
import psutil
import argparse
import pandas as pd
import re
from pandas import Series
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import numpy as np
from nltk.stem import PorterStemmer
# from utils import EntityUtils
from datetime import date, timedelta, datetime

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils

# print(EntityUtils.get_matched_entities('lakme'))
# exit()

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


# ctr calculaion
def ctr_calc(x):
    if x[0] != 0:
        return (x[1]) * 100 // x[0]
    else:
        return 0


def date_type(x):
    global previous
    x = x.date()
    x = str(x)
    previous = max(previous, x)
    return x


def to_list(x):
    return tuple(x)


def group_filter(x):
    diff = 0
    temp = 0
    flag = True
    dftemp = x[0:0]
    for index, each in x.iterrows():
        if flag:
            flag = False
            temp = each.frequency
            continue
        if each.frequency - temp < diff:
            return dftemp
        diff = each.frequency - temp
        temp = each.frequency
    # print(x)
    return x


def popular_searches(df, df_new):
    dftemp = df[0:0]
    for i in range(len(df.index)):
        if (df.iloc[i]['frequency'] - df_new.iloc[i]['frequency'])!=0 and (df_new.iloc[i]['frequency']) / (df.iloc[i]['frequency'] - df_new.iloc[i]['frequency']) > 0.8:
            dftemp = dftemp.append(df.iloc[i], ignore_index=True)
    return dftemp


def get_trending_searches():
    df = pd.read_csv('trending.csv')
    # renaming columns
    df.columns = ['date', 'internal_search_term', 'frequency', 'click_interaction_instance']
    df.drop(df[(df.frequency < 10)].index,inplace=True)
    df.drop(df[(df.click_interaction_instance < 5)].index, inplace=True)
    # changing date format
    df['date'] = [datetime.strptime(x, '%B %d, %Y') for x in df['date']]
    df['date'] = df['date'].dt.normalize()
    df['date'] = df['date'].apply(date_type)
    print(previous)

    df['cleaned_term'] = df['internal_search_term'].map(word_clean)
    idx = df.groupby(['cleaned_term'])['frequency'].transform(max) == df['frequency']
    temp = df[idx]
    temp.drop(['frequency','click_interaction_instance','date'], axis=1, inplace=True)

    # grouping all the exact matched terms on same date with aggregation on freq,ctr
    df = df.groupby(['cleaned_term', 'date'], as_index=False).agg({'frequency': 'sum',
                                                                   'click_interaction_instance': 'sum'})
    df=pd.merge(df,temp,on='cleaned_term')
    df.to_csv('temp.csv', index=False)

    df.drop(df[(df.frequency < 100) & ((df.date) == (previous))].index, inplace=True)


    df = df.groupby(['cleaned_term'], as_index=False).filter(lambda x: x['date'].max() == (previous))
    # print(df)
    df_new = pd.DataFrame
    df_new = df.ix[df.date == previous]

    df = df.groupby(['cleaned_term','internal_search_term'], as_index=False).agg({'frequency': 'sum',
                                                           'click_interaction_instance': 'sum'})

    # top 3 terms which are suddenly into popular list
    data = pd.DataFrame
    data = popular_searches(df, df_new)

    # delete rows whose CTR < 30%
    #data.drop(data[(data.frequency // data.click_interaction_instance) < 0.3].index, inplace=True)
    data = data.sort_values(['frequency', 'click_interaction_instance'], ascending=False)
    data = data.head(5)

    print (data)
    # top 3 frequently searched terms
    df = df.sort_values(['frequency', 'click_interaction_instance'], ascending=False)
    df = df.head(3)

    return data

def insert_trending_searches(data):
    mysql_conn = Utils.mysqlConnection('w')
    cursor = mysql_conn.cursor()

    if not Utils.mysql_read("SHOW TABLES LIKE 'trending_searches'", connection=mysql_conn):
        Utils.mysql_write("create table trending_searches(type VARCHAR(64) DEFAULT 'query',url VARCHAR(255),q VARCHAR(255))",
                          connection=mysql_conn)
    Utils.mysql_write("delete from trending_searches", connection=mysql_conn)

    query = "INSERT INTO trending_searches (type, url,q) VALUES ('%s', '%s','%s') "

    for index, row in data.iterrows():
        word = row['internal_search_term']
        ls = word.split()
        word = " ".join(ls)
        url = "/search/result/?q=" + word.replace(" ", "+")
        print(url)
        values = (url, word)
        query = """INSERT INTO trending_searches ( url,q) VALUES ('%s','%s') """ % (values)

        cursor.execute(query)
        mysql_conn.commit()

    cursor.close()
    mysql_conn.close()


if __name__ == '__main__':
    data = pd.DataFrame
    data = get_trending_searches()

    #insert_trending_searches(data)