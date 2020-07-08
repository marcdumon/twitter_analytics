# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - profile_queries.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
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
- q_get_profile(username)
- q_save_a_profile(username)

"""
collection_name = 'profiles'


def get_collection():
    client = MongoClient()
    db = client[DATABASE]
    collection = db[collection_name]
    return collection


def setup_collection():  # Todo: add indexes
    pass


def q_get_profile(username):
    collection = get_collection()
    q = {'username': username}
    doc = collection.find_one(q)
    return doc


def q_save_a_profile(profile):
    collection = get_collection()
    try:
        f = {'user_id': profile['id']}
        u = {'$set': {'username': profile['username'],
                      'name': profile['name'],
                      'bio': profile['bio'],
                      'join_datetime': profile['join_datetime'],
                      'join_date': profile['join_date'],
                      'join_time': profile['join_time'],
                      'url': profile['url'],
                      'location': profile['location'],
                      'private': profile['private'],
                      'verified': profile['verified'],
                      'background_image': profile['background_image'],
                      'avatar': profile['avatar'],
                      },
             '$push': {'timestamp': datetime.now(),
                       'followers': int(profile['followers']),
                       'following': int(profile['following']),
                       'likes': int(profile['likes']),
                       'tweets': int(profile['tweets']),
                       'media': int(profile['media']),
                       }}
        collection.update_one(f, u, upsert=True)
    except:
        logger.error(f'Unknown error: {sys.exc_info()[0]}')
        raise


if __name__ == '__main__':
    u = TEST_USERNAME
    q_get_profile(u)
