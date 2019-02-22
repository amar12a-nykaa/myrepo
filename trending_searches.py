import pandas as pd
import re
from pandas import Series
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from pas.v2.utils import Utils
import numpy as np
from nltk.stem import PorterStemmer
#from utils import EntityUtils
from datetime import date, timedelta, datetime
# encoding=utf8
import os
import sys

# print(EntityUtils.get_matched_entities('lakme'))
# exit()

reload(sys)
sys.setdefaultencoding('utf8')

porter = PorterStemmer()
previous = date.today() - timedelta(10)
previous = previous.strftime('%Y-%m-%d')
previous = str(previous)
#print (previous)

def word_clean(word):
    word = str(word).lower()
    ls = re.split('[^A-Za-z+&0-9]', str(word))
    l = []
    for e in ls:
         l.append(porter.stem(e))
    word = "".join(l)
    return word

#ctr calculaion
def ctr_calc(x):
    if x[0] != 0:
        return (x[1])*100/x[0]
    else:
        return 0

def date_type(x):
    x = x.date()
    x = str(x)
    return x

def group_filter(x):
    diff = 0
    temp = 0
    flag = True
    dftemp = x[0:0]
    for index,each in x.iterrows():
        if flag:
            flag = False
            temp = each.frequency
            continue
        if each.frequency-temp < diff:
            return dftemp
        diff = each.frequency - temp
        temp = each.frequency
    #print(x)
    return x

def popular_searches(df,df_new):
    row_list = []
    for i in range(len(df.index)):
        if (df_new.iloc[i, 3] * 100) / (df.iloc[i, 2] - df_new.iloc[i, 3]) > 30:
            row_list.append(df.values[i])
    print (df.columns)
    print (df_new.columns)
    return row_list


def get_trending_searches():

    df = pd.read_csv('/home/abc/trending_search/trending.csv')
    #renaming columns
    df.columns = ['date','internal_search_term','frequency','click_interaction_instance']
    #changing date format

    df['date'] = [datetime.strptime(x, '%B %d, %Y') for x in df['date'] ]
    df['date'] = df['date'].dt.normalize()
    df['date'] = df['date'].apply(date_type)

    #df['internal_search_term']=df['internal_search_term'].str.lower()

    df['cleaned_term'] = df['internal_search_term'].map(word_clean)

    #grouping all the exact matched terms on same date with aggregation on freq,ctr
    df = df.groupby(['cleaned_term','date'],as_index=False).agg({'frequency' : 'sum',
	                                            'click_interaction_instance' : 'sum',
	                                            'internal_search_term': list })

    df.drop(df[(df.frequency < 100) & ((df.date)==(previous))].index, inplace=True)

    df['internal_search_term'] = df['internal_search_term'].apply(lambda x:x[0])

    df = df.groupby(['cleaned_term'],as_index=False).filter(lambda x: x['date'].max() == (previous))

    df = df.sort_values(['cleaned_term','date'])

    df.to_csv('output1.csv')

    df_new = pd.DataFrame
    df_new = df[df.date == previous]

    df = df.groupby(['cleaned_term'],as_index=False).agg({'frequency' : 'sum',
	                                            'click_interaction_instance' : 'sum',
	                                            'internal_search_term': list })

    df['internal_search_term'] = df['internal_search_term'].apply(lambda x:x[0])

    row_list = []
    row_list = popular_searches(df,df_new)
    data = pd.DataFrame(row_list)

    df = df[df.frequency>500]
    df = df.sort_values(['frequency','click_interaction_instance'],ascending=False)
    df = df.head(5)

    #df.drop(df.columns[0],axis=1,inplace=True)
    #df.drop(df.columns[2],axis=1,inplace=True)

    df.to_csv('output_topFreq.csv')
    return df

'''
    data.columns=['cleaned_term','internal_search_term','frequency','click_interaction_instance']
    data.drop(data[data.frequency < 100 ].index, inplace=True)

    data['CTR']=data[['frequency','click_interaction_instance']].apply(ctr_calc,axis=1)
    data.drop(data[data.CTR < 30].index, inplace=True)

    #print(data)
    #data=data.sort_values(['cleaned_term'])

    data.to_csv('output_sum30.csv')
'''

if __name__ == '__main__':

    get_trending_searches()
    mysql_conn = Utils.mysqlConnection('w')
    cursor = mysql_conn.cursor()
    if not Utils.mysql_read("SHOW TABLES LIKE 'trending_searches'",connection=mysql_conn):
        Utils.mysql_write("create table trending_searches(search_term varchar(32), url varchar(32), rank int",connection=mysql_conn)
    Utils.mysql_write("delete from trending_searches",connection=mysql_conn)

    data = pd.DataFrame
    data = get_trending_searches()

    query = "INSERT INTO trending_searches (search_term, url, rank) VALUES ('%s', '%s', '%s') "

    for index, row in data.iterrows():
        url = "/search/result/?q=" + row['internal_search_term'].replace(" ", "+")
        print(url)
        values = (row.internal_search_term,url,row.frequency)
        cursor.execute(query, values)
        mysql_conn.commit()

    cursor.close()
    mysql_conn.close()






