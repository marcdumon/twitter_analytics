# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - tweet_queries.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
from datetime import datetime

from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

from database.config_facade import SystemCfg
from tools.logger import logger

"""
Group of queries to store and retrief data from the tweets collections.
The queries start with 'q_' 
Queries accept and return a dict or a lists of dicts when suitable

Convention:
-----------
- documnet:     d
- query:        q
- projection:   p
- sort:         s
- filter:       f
- update:       u
- pipeline      pl
- match         m
- group:        g

IMPLEMENTED QUERIES
-------------------
- q_get_nr_tweets_per_day(username, session_begin_date, session_end_date)
- q_save_a_tweet(tweet)
- q_update_a_tweet(tweet)
"""
system_cfg = SystemCfg()
database = system_cfg.database
collection_name = 'tweets'


def get_collection():
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


def setup_collection():  # Todo: add indexes
    collection = get_collection()
    collection.create_index('tweet_id', unique=True)


def q_get_nr_tweets_per_day(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    collection = get_collection()

    m = {'$match': {'username': username,
                    'datetime': {'$gte': begin_date,
                                 '$lte': end_date}}}
    g = {'$group': {'_id': '$date',
                    'nr_tweets': {'$sum': 1}}}
    p = {'$project': {'date': {'$dateFromString': {'dateString': '$_id'}},
                      'nr_tweets': 1, '_id': 0}}
    s = {'$sort': {'date': ASCENDING}}

    cursor = collection.aggregate([m, g, p, s])
    return list(cursor)


def q_save_a_tweet(tweet):
    collection = get_collection()
    try:
        result = collection.insert_one(tweet)
    except DuplicateKeyError as e:
        logger.debug(f"Duplicate: {tweet['tweet_id']} - {tweet['date']} - {tweet['name']}")
    except:
        logger.error(f'Unknown error: {sys.exc_info()[0]}')
        raise


def q_update_a_tweet(tweet):
    collection = get_collection()
    f = {'tweet_id': tweet['tweet_id']}
    u = {'$set': tweet}
    try:
        result = collection.update_one(f, u, upsert=True)
        logger.debug(f"Updated: {result.raw_result} - {tweet['tweet_id']} - {tweet['date']} - {tweet['name']}")
    except DuplicateKeyError as e:
        logger.debug(f"Duplicate: {tweet['tweet_id']} - {tweet['date']} - {tweet['name']}")


def q_tweets_scraping_log():
    pass


def q_update_profile_scraping_log():
    pass


if __name__ == '__main__':
    # print(collection_name)
    setup_collection()
    # print(q_get_nr_tweets_per_day(TEST_USERNAME))
    # q_test()
