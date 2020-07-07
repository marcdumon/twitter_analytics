# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - tweet_queries.py
# md
# --------------------------------------------------------------------------------------------------------
from pymongo import MongoClient

from config import DATABASE
from tools.logger import logger

tweets_collection_name = 'tweets'


def get_collection():
    client = MongoClient()
    db = client[DATABASE]
    collection = db[tweets_collection_name]
    return collection


def insert_many_tweets():
    pass


def insert_one_tweet():
    pass


def update_one_tweet():
    pass


def test():
    pass


if __name__ == '__main__':
    test()
    print(tweets_collection_name)
