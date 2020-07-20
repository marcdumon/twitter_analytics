# --------------------------------------------------------------------------------------------------------
# 2020/07/20
# src - log_queries.py
# md
# --------------------------------------------------------------------------------------------------------


"""
Group of queries to store and retrief log data.
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
- q_save_log(log)
-
"""
from pprint import pprint

from pymongo import MongoClient, DESCENDING

from database.config_facade import SystemCfg

system_cfg = SystemCfg()
database = system_cfg.database
collection_name = 'logs'


def get_collection():  # Todo: same function in many modules. Put in tools?
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


def setup_collection():
    collection = get_collection()
    # collection.create_index([('ip', DESCENDING), ('port', DESCENDING)], unique=True)


def q_save_log(log):
    collection = get_collection()
    collection.insert_one(log)


def q_get_max_sesion_id():
    collection = get_collection()
    cursor= collection.find().sort([('session_id', -1)]).limit(1)
    max_session_id=list(cursor)[0]['session_id'] if list(cursor) else -1
    return max_session_id


if __name__ == '__main__':
    setup_collection()
