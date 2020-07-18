# --------------------------------------------------------------------------------------------------------
# 2020/07/15
# src - db_management.py
# md
# --------------------------------------------------------------------------------------------------------

"""
A collection of queries to manage the db.

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

IMPLEMENTED QUERIES AND FUNCTIONS
---------------------------------

- q_add_field(collection_name, field_name, value)
- q_remove_field(collection_name, field_name)
- q_rename_field(collection_name, old_field_name, new_field_name)

# - q_copy_field
"""

from pymongo import MongoClient

# from config import DATABASE
from database.control_facade import Scraping_cfg, SystemCfg
from tools.logger import logger

system_cfg=SystemCfg()


# Todo: Refactor: put everything in a class. DbManagement.copy_collection().xxx

def get_collection(collection_name, db_name=system_cfg.database):
    db = get_database()[db_name]
    collection = db[collection_name]
    return collection


def get_database(db_name=system_cfg.database):
    client = MongoClient()
    db = client[db_name]
    return db



def q_remove_field(collection_name, field_name):
    collection = get_collection(collection_name)
    f = {}
    u = {'$unset': {field_name: 1}}
    result = collection.update_many(f, u, False, None, True)
    logger.info(f'Result remove field {field_name}: {result.raw_result}')


def q_rename_field(collection_name, old_field_name, new_field_name):
    collection = get_collection(collection_name)
    f = {}
    u = {'$rename': {old_field_name: new_field_name}}
    result = collection.update_many(f, u, False, None, True)
    logger.info(f'Result rename field {old_field_name} into {new_field_name}: {result.raw_result}')


def q_add_field(collection_name, field_name, value=None):
    collection = get_collection(collection_name)
    f = {}
    u = {'$set': {field_name: value}}
    result = collection.update_many(f, u, False, None, True)
    logger.info(f'Result adding field {field_name} to value {value}: {result.raw_result}')


# def q_copy_field(collection_name, from_field_name, to_field_name): # Todo:  f'${from_field_name}'}} doesn't work
#     collection = get_collection(collection_name)
#     f = {}
#     u = {'$set': {to_field_name: f'${from_field_name}'}}
#     result = collection.update_many(f, u, False, None, True)
#     logger.info(f'Result copy field {from_field_name} to {to_field_name}: {result.raw_result}')


if __name__ == '__main__':
    pass
    # q_remove_field('proxies', 'xxx')
    # q_copy_field('proxies', 'delay', 'xxx')
    # q_add_field('proxies', 'scrape_n_failed_total', 0)
