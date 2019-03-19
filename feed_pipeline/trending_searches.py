import sys
import pandas as pd
import argparse
import arrow
import re
import urllib
from nltk.stem import PorterStemmer
from datetime import date,datetime,timedelta

sys.path.append("/nykaa/api")
from pas.v2.utils import Utils, EntityUtils

porter = PorterStemmer()
RESULT_SIZE = 5


def word_clean(word):
    word = str(word).lower()
    ls = re.split('[^A-Za-z+&0-9]', str(word))
    l = []
    for e in ls:
        l.append(porter.stem(e))
    word = "".join(l)
    return word

def get_entities(row):
    result,coverage = EntityUtils.get_matched_entities(row['ist'])
    row['brand']=result['brand']['entity'] if 'brand' in result else 'None'
    row['category'] = result['category']['entity'] if 'category' in result else 'None'
    row['coverage'] = coverage

    return row

def calculate_ctr(row):
    row['ctr_ratio'] = float(row['ctr']*100)/row['frequency']
    return row

def get_yesterday_trending():
    mysql_conn = Utils.mysqlConnection('r')

    if not Utils.mysql_read("SHOW TABLES LIKE 'trending_searches'", connection=mysql_conn):
        return None
    prev=[]
    for each in Utils.mysql_read("select word from trending_searches"):
        prev.append(word_clean(each.get('q')))
    return prev

def get_results(final_df,prev_trending_terms):
    result = []
    included_list = {'brand': [], 'category': []}
    count = 0
    for index, row in final_df.iterrows():
        brand = row['brand']
        category = row['category']
        if brand and brand in included_list['brand']:
            continue
        if category and category in included_list['category']:
            continue
        if row['cleaned_term'] in prev_trending_terms:
            continue
        included_list['brand'].append(brand)
        included_list['category'].append(category)
        result.append(row['ist'])
        count = count + 1
        if count >= RESULT_SIZE:
            break
    return result

def select_version(final_df,prev_trending_terms,algo):

    if algo == 1:
        final_df = final_df[(final_df.coverage == 100)]
        result = get_results(final_df,prev_trending_terms)
        return result

    if algo == 2:
        final_df = final_df[(final_df.brand != 'None')]
        result = get_results(final_df, prev_trending_terms)
        return result

    if algo == 3:
        result = []
        count = 0
        for index, row in final_df.iterrows():
            if row['cleaned_term'] in prev_trending_terms:
                continue
            result.append(row['ist'])
            count = count + 1
            if count >= RESULT_SIZE:
                break
        return result

def get_trending_searches(filename):
    flag1 = 0
    flag2 = 0
    df = pd.DataFrame

    for i in range(4):
        temp = str(date.today() - timedelta(i + 1)).split('-')
        temp = ''.join(temp)
        from pathlib import Path
        filepath = '/nykaa/adminftp/' + 'trendingRawData' + temp + '.csv'
        if not Path(filepath).is_file():
            flag1 = 1
            break;
        if flag2 == 0:
            df = pd.read_csv(filepath)
            flag2 = 1
            continue
        df_temp = pd.read_csv(filepath)
        df = pd.concat([df, df_temp], ignore_index=True)
        print(df)

    if flag1 == 1:
        filepath = '/nykaa/adminftp/' + filename
        df = pd.read_csv(filename)

    # renaming columns
    df.columns = ['date', 'ist', 'frequency', 'ctr']
    df.drop(df[df.frequency < 10].index, inplace=True)

    # changing date format
    df['date'] = [datetime.strptime(x, '%B %d, %Y').date() for x in df['date']]
    df['date'] = df['date'].astype(str)
    df['cleaned_term'] = df['ist'].map(word_clean)
    idx = df.groupby(['cleaned_term'])['frequency'].transform(max) == df['frequency']
    temp = df[idx]
    temp = temp.groupby(['cleaned_term'], as_index=False).agg({'ist': 'first'})

    df = df.groupby(['cleaned_term', 'date'], as_index=False).agg({'frequency': 'sum', 'ctr': 'sum'})
    df = pd.merge(df, temp, on='cleaned_term')
    df['ctr_ratio'] = 0
    df = df.apply(calculate_ctr, axis=1)
    df.drop(df[df.ctr_ratio < 10].index, inplace=True)

    previous = df['date'].max()
    previous_date = arrow.get(previous, 'YYYY-MM-DD')
    start = previous_date.shift(days=-3).format('YYYY-MM-DD')

    df_yesterday = df[df.date == previous]
    df_remaining = df[(df.date >= start) & (df.date < previous)]

    # deleting searches whose yesterday.freq < 100 yesterday.CTR < 40%
    df_yesterday = df_yesterday[(df_yesterday.frequency > 100) & (df_yesterday.ctr_ratio > 40)]
    df_yesterday.drop(['ctr_ratio', 'date'], axis=1, inplace=True)

    df_remaining = df_remaining.groupby(['cleaned_term', 'ist'], as_index=False).agg({'frequency': 'mean', 'ctr': 'mean'})
    df_remaining.rename(columns={'frequency': 'avg_frequency', 'ctr': 'avg_ctr'}, inplace=True)

    final_df = pd.merge(df_yesterday, df_remaining, how='left', on=['cleaned_term','ist'])
    final_df['avg_frequency'].fillna(0)
    total_yesterday = final_df['frequency'].sum()
    total_remaining = final_df['avg_frequency'].sum()
    final_df['avg_frequency'] = (final_df['avg_frequency']*total_yesterday)/total_remaining
    final_df = final_df[final_df.frequency >= 1.3 * final_df.avg_frequency]

    #entities detection
    final_df['brand'] = ''
    final_df['category'] = ''
    final_df['coverage'] = 0
    final_df = final_df.apply(get_entities, axis=1)

    prev_trending_terms = get_yesterday_trending()
    result = select_version(final_df, prev_trending_terms, algo=1)

    return result


def insert_trending_searches(data):
    mysql_conn = Utils.mysqlConnection('w')
    cursor = mysql_conn.cursor()

    if not Utils.mysql_read("SHOW TABLES LIKE 'trending_searches'", connection=mysql_conn):
        Utils.mysql_write("create table trending_searches(type VARCHAR(64),url VARCHAR(255),q VARCHAR(255))",
                          connection=mysql_conn)
    Utils.mysql_write("delete from trending_searches", connection=mysql_conn)

    for word in data:
        ls = word.split()
        word = " ".join(ls)
        url = "/search/result/?" + str(urllib.parse.urlencode({'q': word}))
        values = ('query',url, word)
        query = """INSERT INTO trending_searches (type, url,q) VALUES ("%s","%s","%s") """ % (values)

        cursor.execute(query)
        mysql_conn.commit()

    cursor.close()
    mysql_conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--filename", type=str, default='trending.csv')
    argv = vars(parser.parse_args())
    data = get_trending_searches(filename=argv['filename'])
    print(data)
    insert_trending_searches(data)