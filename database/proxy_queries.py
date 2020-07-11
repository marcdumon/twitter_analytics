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
- q_get_proxies(q)
- q_save_a_proxy(proxy)
- q_update_a_proxy_test(proxy_test)
- q_reset_a_proxy_success_flag(proxy)
- q_set_a_proxy_success_flag(proxy, success_flag)
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


def q_get_proxies(q): # Todo: remove q here ?
    collection = get_collection()
    p = {'ip': 1, 'port': 1, 'delay': 1, 'blacklisted': 1, '_id': 0}
    cursor = collection.find(q, p)
    proxies = list(cursor)

    return proxies


def q_save_a_proxy(proxy):
    collection = get_collection()
    d = proxy
    # New proxies have not been tested
    d['delay'] = 999999
    d['blacklisted'] = True
    d['error_code'] = 0
    d['n_blacklisted'] = 0
    d['n_tested'] = 0
    d['success']=True
    d['n_used']=0
    d['n_failed']=0
    try:
        collection.insert_one(d)
    except DuplicateKeyError as e:
        logger.warning(f"Duplicate proxy: {proxy['ip']}:{proxy['port']}")


def q_update_a_proxy_test(proxy_test):
    collection = get_collection()
    f = {'ip': proxy_test['ip'],
         'port': proxy_test['port']}
    u = {'$set': {'delay': proxy_test['delay'],
                  'blacklisted': proxy_test['blacklisted'],
                  'error_code': proxy_test['error_code']},
         '$inc': {'n_blacklisted': int(proxy_test['blacklisted']),
                  'n_tested': 1}}
    collection.update_one(f, u, upsert=True)


def q_set_a_proxy_success_flag(proxy, success_flag):
    collection = get_collection()
    f = {'ip': proxy['ip'], 'port': proxy['port']}
    u = {'$set': {'success': success_flag}}
    if success_flag:
        u['$inc'] = {'n_used': 1}
    else:
        u['$inc'] = {'n_used': 1, 'n_failed': 1}
    collection.update_one(f, u, upsert=True)


def q_reset_a_proxy_success_flag(proxy):
    collection = get_collection()
    f = {'ip': proxy['ip'], 'port': proxy['port']}
    u = {'$set': {'success': True, 'n_used': 0, 'n_failed': 0}}
    collection.update_one(f, u, upsert=True)


if __name__ == '__main__':
    pass
    # setup_collection()
    # proxy_test = {'ip': '1.1.1.1', 'port': '2332', 'delay': .03, 'blacklisted': False}
    # q_update_a_proxy_test(proxy_test)
    # col =get_collection()
    # for proxy in col.find():
    #     col.update_one({'_id':proxy['_id']},
    #                    {'$set': {'n_blacklisted': int(proxy['blacklisted']), 'n_tested': 1}})
