# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - proxy_queries.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
from datetime import datetime
from pprint import pprint

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError, BulkWriteError

from config import DATABASE, TEST_USERNAME
from tools.logger import logger

"""
Group of queries to store and retrief data from the proxies collection.
The queries start with 'q_' 
Queries accept and return a dict or a lists of dicts when suitable

"""

"""
IMPLEMENTED QUERIES
- q_save_many_proxies(proxies)
- 

"""

collection_name = 'proxies'


def get_collection():
    client = MongoClient()
    db = client[DATABASE]
    collection = db[collection_name]
    return collection


def setup_collection():
    collection = get_collection()
    collection.create_index([('ip', DESCENDING), ('port', DESCENDING)], unique=True)


def q_save_a_proxy(proxy):
    collection = get_collection()
    try:
        collection.insert_one(proxy)
    except DuplicateKeyError as e:
        logger.error(f"Duplicate proxy: {proxy['ip']}:{proxy['port']}")


def q_save_many_proxies(proxies):
    collection = get_collection()
    try:
        collection.insert_many(proxies, ordered=False)
    except BulkWriteError as e:
        logger.error(f'Error insterting many proxies.')
        logger.error(f'Error message: {e}')


if __name__ == '__main__':
    pass
    # setup_collection()
