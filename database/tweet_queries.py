# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - tweet_queries.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

from pymongo import MongoClient, ASCENDING

from config import DATABASE, TEST_USERNAME
from tools.logger import logger

"""
Group of queries to store and retrief data from the tweets collections.
The queries start with 'q_' 
Queries return a dict or a lists of dicts when suitable
"""

"""
IMPLEMENTED QUERIES
-  q_get_nr_tweets_per_day(username, begin_date, end_date)

"""





# tweets_collection_name = 'tweetsxxx'
tweets_collection_name = 'tweets'


def get_collection():
    client = MongoClient()
    db = client[DATABASE]
    collection = db[tweets_collection_name]
    return collection


def q_insert_many_tweets():
    pass


def q_insert_one_tweet():
    pass


def q_update_one_tweet():
    pass


def q_get_nr_tweets_per_day(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    tweet_collection = get_collection()
    q = [
        {'$match': {'username': username,
                    'datetime': {'$gte': begin_date, '$lte': end_date}}},
        {'$group': {'_id': '$date', 'nr_tweets': {'$sum': 1}}},
        {'$project': {'date': {'$dateFromString': {'dateString': '$_id'}},
                      'nr_tweets': 1, '_id': 0}},
        {'$sort': {'date': ASCENDING}}
    ]
    cursor = tweet_collection.aggregate(q)
    return list(cursor)

def q_test():
    xxx=get_collection()
    import pandas as pd
    x = pd.DataFrame({'date':[datetime.now()]})
    # x['date']=pd.to_datetime(x['date'])
    x['date']=pd.to_datetime(x['date'].values.astype(datetime),unit='ns')

    print(x)
    d1=x.iloc[0].values[0]
    print(type(d1),d1)
    d2=pd.Timestamp(d1)
    print(type(d2),d2)


    # xxx.insert_one({
    #     'date':datetime.now(),
    #     'pd_date':d1,
    #     'pd_ts':d2
    # })


if __name__ == '__main__':
    # print(tweets_collection_name)
    print(q_get_nr_tweets_per_day(TEST_USERNAME))
    # q_test()
