import sys
import pandas as pd
import argparse
import arrow
import re
import os
import urllib
from nltk.stem import PorterStemmer
from datetime import date,datetime,timedelta

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import EntityUtils

sys.path.append('/nykaa/scripts/sharedutils/')
from dateutils import enumerate_dates


porter = PorterStemmer()
RESULT_SIZE = 5
date_2days_before = (date.today() - timedelta(days=2)).strftime('%d-%m-%Y')
date_today = date.today().strftime('%d-%m-%Y')

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
    entities = list(result.keys())
    row['brand'] = ''
    row['category'] = ''
    row['coverage'] = coverage
    if 'brand' in result :
        row['brand'] = result['brand']['entity']
        entities.remove('brand')
    if 'category' in result:
        row['category'] = result['category']['entity']
        entities.remove('category')
    if len(entities) > 0:
        row['other_entities'] = True

    return row

def calculate_ctr(row):
    row['ctr_ratio'] = float(row['ctr']*100)/row['frequency']
    return row

def get_yesterday_trending():
    mysql_conn = PasUtils.mysqlConnection('r')

    if not PasUtils.mysql_read("SHOW TABLES LIKE 'trending_searches'", connection=mysql_conn):
        return None
    prev=[]
    for each in PasUtils.mysql_read("select q from trending_searches where date>=%s" % date_2days_before):
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
        if prev_trending_terms is not None and row['cleaned_term'] in prev_trending_terms:
            continue
        included_list['brand'].append(brand)
        included_list['category'].append(category)
        word = brand + ' ' + category
        result.append(word.strip())
        count = count + 1
        if count >= RESULT_SIZE:
            break
    return result

def select_version(final_df,prev_trending_terms,algo):

    if algo == 1:
        final_df = final_df[(final_df['other_entities'] == False)]
        final_df = final_df[(final_df.coverage == 100) & (~((final_df.brand=='') & (final_df.category=='')))]
        result = get_results(final_df,prev_trending_terms)
        return result

    if algo == 2:
        final_df = final_df[(final_df.brand != '')]
        result = get_results(final_df, prev_trending_terms)
        return result

    if algo == 3:
        result = get_results(final_df, prev_trending_terms)
        return result

def read_file(filepath):
    def unzip_file(path_to_zip_file):
        import zipfile
        import os
        zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
        zip_ref.extractall(os.path.dirname(path_to_zip_file))
        zip_ref.close()

    if not os.path.isfile(filepath):
        print("[ERROR] File does not exist: %s" % filepath)
        return

    extention = os.path.splitext(filepath)[1]
    if extention == '.zip':
        csvfilepath = os.path.splitext(filepath)[0] + '.csv'
        os.system("rm %s")
        try:
            os.remove(csvfilepath)
        except OSError:
            pass
        unzip_file(filepath)
        assert os.path.isfile(csvfilepath), 'Failed to extract CSV from %s' % filepath

        filepath = csvfilepath

    data=pd.read_csv(filepath)
    return data

def get_trending_searches(filename):
    flag = 0
    dates = enumerate_dates(-4,-1)
    for date in dates:
        date = ''.join(str(date.date()).split('-'))
        filepath = '/nykaa/adminftp/' + 'trendingRawData' + date + '.csv'
        if not os.path.exists(filepath):
            filepath = '/nykaa/adminftp/' + 'trendingRawData' + date + '.zip'

        df_temp=read_file(filepath)
        try:
            df = pd.concat([df, df_temp], ignore_index=True)
        except:
            df = df_temp
            if df_temp is None:
                flag = 1

    if flag == 1:
        filepath = 'feed_pipeline/' + filename
        df = pd.read_csv(filepath)

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
    # normalising each term freequency
    final_df['avg_frequency'] = (final_df['avg_frequency']*total_yesterday)/total_remaining
    final_df = final_df[final_df.frequency >= 1.3 * final_df.avg_frequency]

    #entities detection
    final_df['brand'] = ''
    final_df['category'] = ''
    final_df['coverage'] = 0
    final_df['other_entities'] = False
    final_df = final_df.apply(get_entities, axis=1)

    prev_trending_terms = get_yesterday_trending()
    final_df = final_df.sort_values(['avg_frequency'], ascending=False)
    result = select_version(final_df, prev_trending_terms, algo=1)

    return result


def insert_trending_searches(data):
    mysql_conn = PasUtils.mysqlConnection('w')
    cursor = mysql_conn.cursor()

    if not PasUtils.mysql_read("SHOW TABLES LIKE 'trending_searches'", connection=mysql_conn):
        PasUtils.mysql_write("create table trending_searches(type VARCHAR(64),url VARCHAR(255),q VARCHAR(255),date VARCHAR(64))",
                          connection=mysql_conn)
    PasUtils.mysql_write("delete from trending_searches where date<=%s" % date_2days_before , connection=mysql_conn)
    date_today = datetime.today().strftime('%d-%m-%Y')
    for word in data:
        ls = word.split()
        word = " ".join(ls)
        url = "/search/result/?" + str(urllib.parse.urlencode({'q': word}))
        values = ('query',url, word, date_today)
        query = """INSERT INTO trending_searches (type, url,q, date) VALUES ("%s","%s","%s","%s") """ % (values)

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
