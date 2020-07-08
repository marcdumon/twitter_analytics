# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - profile_queries.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

from pymongo import MongoClient, ASCENDING

from config import DATABASE, TEST_USERNAME
from tools.logger import logger

"""
Group of queries to store and retrief data from the profile collections.
The queries start with 'q_' 
Queries return a dict or a lists of dicts when suitable
"""

"""
IMPLEMENTED QUERIES
-  q_get_profile(username)

"""
collection_name = 'profiles'


def get_collection():
    client = MongoClient()
    db = client[DATABASE]
    collection = db[collection_name]
    return collection

def setup_collection(): # Todo: add indexes
    pass

def q_get_profile(username):
    collection = get_collection()
    q = {'username': username}
    doc = collection.find_one(q)
    return doc


if __name__ == '__main__':
    u = TEST_USERNAME
    q_get_profile(u)
