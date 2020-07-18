# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - profile_queries.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from database.control_facade import SystemCfg
from tools.logger import logger

"""
Group of queries to store and retrief data from the profile collections.
The queries start with 'q_' 
Queries return a dict or a lists of dicts when suitable

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
- q_get_a_profile(username)
- q_get_profiles()
- q_save_a_profile(username)
- q_set_a_scrape_flag(username, flag)

"""
system_cfg = SystemCfg()
database = system_cfg.database
collection_name = 'profiles'


def get_collection():
    client = MongoClient()
    db = client[database]
    collection = db[collection_name]
    return collection


def setup_collection():  # Todo: add indexes
    pass


def q_get_a_profile(username):
    collection = get_collection()
    q = {'username': username}
    doc = collection.find_one(q)
    return doc


def q_get_profiles():
    collection = get_collection()
    cursor = collection.find()
    return list(cursor)


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
                      'scrape_ok': 0
                      },
             '$push': {'timestamp': datetime.now(),
                       'followers': int(profile['followers']),
                       'following': int(profile['following']),
                       'likes': int(profile['likes']),
                       'tweets': int(profile['tweets']),
                       'media': int(profile['media']),
                       }}
        try:
            collection.update_one(f, u, upsert=True)
        except DuplicateKeyError as e:
            logger.error('*' * 3000)
            logger.error('profile=')
            logger.error((profile))
            logger.error(e)
            logger.error('*' * 3000)
    except:
        logger.error(f'Unknown error: {sys.exc_info()[0]}')
        raise


def q_set_a_scrape_flag(username, flag):
    collection = get_collection()
    f = {'username': username}
    u = {'$set': {'scrape_flag': flag}}
    collection.update_one(f, u)


if __name__ == '__main__':
    u = 'franckentheo'
    p = q_get_a_profile(u)
    print(p['join_date'])
