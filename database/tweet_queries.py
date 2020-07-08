# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - tweet_queries.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
from datetime import datetime
from pprint import pprint

from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

from config import DATABASE, TEST_USERNAME
from tools.logger import logger

"""
Group of queries to store and retrief data from the tweets collections.
The queries start with 'q_' 
Queries accept and return a dict or a lists of dicts when suitable

"""

"""
IMPLEMENTED QUERIES
-  q_get_nr_tweets_per_day(username, begin_date, end_date)

"""

collection_name = 'tweets'


def get_collection():
    client = MongoClient()
    db = client[DATABASE]
    collection = db[collection_name]
    return collection


def setup_collection():  # Todo: add indexes
    collection = get_collection()
    collection.create_index('tweet_id', unique=True)


def q_get_nr_tweets_per_day(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    collection = get_collection()
    q = [
        {'$match': {'username': username,
                    'datetime': {'$gte': begin_date, '$lte': end_date}}},
        {'$group': {'_id': '$date', 'nr_tweets': {'$sum': 1}}},
        {'$project': {'date': {'$dateFromString': {'dateString': '$_id'}},
                      'nr_tweets': 1, '_id': 0}},
        {'$sort': {'date': ASCENDING}}
    ]
    cursor = collection.aggregate(q)
    return list(cursor)


def q_save_a_tweet(tweet):
    collection = get_collection()
    try:
        result = collection.insert_one(tweet)
    except DuplicateKeyError as e:
        logger.error(f"Duplicate: {tweet['tweet_id']} - {tweet['date']} - {tweet['name']}")
    except:
        logger.error(f'Unknown error: {sys.exc_info()[0]}')
        raise


def q_test():
    xxx = get_collection()
    import pandas as pd
    x = pd.DataFrame({'date': [datetime.now()]})
    # x['date']=pd.to_datetime(x['date'])
    x['date'] = pd.to_datetime(x['date'].values.astype(datetime), unit='ns')

    print(x)
    d1 = x.iloc[0].values[0]
    print(type(d1), d1)
    d2 = pd.Timestamp(d1)
    print(type(d2), d2)

    # xxx.insert_one({
    #     'date':datetime.now(),
    #     'pd_date':d1,
    #     'pd_ts':d2
    # })


if __name__ == '__main__':
    # print(collection_name)
    # setup_collection()
    print(q_get_nr_tweets_per_day(TEST_USERNAME))
    # q_test()
