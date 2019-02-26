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
    print(x[0], x[1])
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
    print(df_new.iloc[0, 0])
    for i in range(len(df.index)):
        if (df_new.iloc[i, 2] * 100) / (df.iloc[i, 2] - df_new.iloc[i, 2]) > 30:
            dftemp = dftemp.append(df.iloc[i], ignore_index=True)
    return dftemp


def get_trending_searches():
    df = pd.read_csv('trending.csv')
    # renaming columns
    df.columns = ['date', 'internal_search_term', 'frequency', 'click_interaction_instance']
    df.drop(df[(df.frequency < 10)].index,inplace=True)
    # changing date format
    df['date'] = [datetime.strptime(x, '%B %d, %Y') for x in df['date']]
    df['date'] = df['date'].dt.normalize()
    df['date'] = df['date'].apply(date_type)
    print(previous)

    df['cleaned_term'] = df['internal_search_term'].map(word_clean)
    idx = df.groupby(['cleaned_term'])['frequency'].transform(max) == df['frequency']
    temp = df[idx]
    print(temp)
    temp.drop(['frequency'], axis=1, inplace=True)
    temp.drop(['click_interaction_instance'], axis=1, inplace=True)


    # grouping all the exact matched terms on same date with aggregation on freq,ctr
    df = df.groupby(['cleaned_term', 'date'], as_index=False).agg({'frequency': 'sum',
                                                                   'click_interaction_instance': 'sum'})
    df.join(temp, lsuffix='_caller', rsuffix='_other')
    df.to_csv('temp.csv', index=False)

    df.drop(df[(df.frequency < 100) & ((df.date) == (previous))].index, inplace=True)


    df = df.groupby(['cleaned_term'], as_index=False).filter(lambda x: x['date'].max() == (previous))
    # print(df)
    df_new = pd.DataFrame
    df_new = df.ix[df.date == previous]
    df_new.drop(['date'], axis=1, inplace=True)
    df_new = df_new.sort_values(['cleaned_term'])

    df = df.groupby(['cleaned_term'], as_index=False).agg({'frequency': 'sum',
                                                           'click_interaction_instance': 'sum'})
    df = df.sort_values(['cleaned_term'])
    # print(df.columns)
    # print(df_new.columns)
    # top 3 terms which are suddenly into popular list
    data = pd.DataFrame
    data = popular_searches(df, df_new)
    # print(data.head(5))
    # data.columns = ['cleaned_term','internal_search_term','frequency','click_interaction_instance']
    data.drop(data[data.frequency < 100].index, inplace=True)

    # add column for CTR
    # print(data.head(5))
    data['CTR'] = data[['frequency', 'click_interaction_instance']].apply(ctr_calc, axis=1)
    data.drop(data[data.CTR < 30].index, inplace=True)
    data = data.sort_values(['frequency', 'click_interaction_instance'], ascending=False)
    data = data.head(3)
    data.drop(['CTR'], axis=1, inplace=True)

    # top 3 frequently searched terms
    df = df[df.frequency > 500]
    df = df.sort_values(['frequency', 'click_interaction_instance'], ascending=False)
    df = df.head(3)
    print(df)
    print(data)
    result = pd.concat([df, data])
    return result


def insert_trending_searches(data):
    mysql_conn = Utils.mysqlConnection('w')
    cursor = mysql_conn.cursor()

    if not Utils.mysql_read("SHOW TABLES LIKE 'trending_searches'", connection=mysql_conn):
        Utils.mysql_write("create table trending_searches(type varchar(64),url varchar(255),q varchar(255))",
                          connection=mysql_conn)
    Utils.mysql_write("delete from trending_searches", connection=mysql_conn)

    query = "INSERT INTO trending_searches (type, url,q) VALUES ('%s', '%s','%s') "

    for index, row in data.iterrows():
        word = row['internal_search_term']
        ls = word.split()
        word = " ".join(ls)
        url = "/search/result/?q=" + word.replace(" ", "+")
        print(url)
        values = ('query', url, word)
        query = """INSERT INTO trending_searches (type, url,q) VALUES ('%s', '%s','%s') """ % (values)

        cursor.execute(query)
        mysql_conn.commit()

    cursor.close()
    mysql_conn.close()


if __name__ == '__main__':
    data = pd.DataFrame
    data = get_trending_searches()

    insert_trending_searches(data)