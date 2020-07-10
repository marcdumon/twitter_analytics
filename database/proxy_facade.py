# --------------------------------------------------------------------------------------------------------
# 2020/07/08
# src - proxy_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime
from pprint import pprint

import pandas as pd
from config import TEST_USERNAME
from database.profile_queries import q_get_profile, q_save_a_profile
from database.proxy_queries import q_save_proxies, q_save_a_proxy, q_get_proxies, q_update_a_proxy_test
from database.tweet_queries import q_get_nr_tweets_per_day, q_save_a_tweet
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrieve proxy data via proxy queries from the proxies collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------
- get_proxies(only_blacklisted)
- save_a_proxy_test(proxy, delay)
- save_proxies(tweets_df)
- set_all_proxies(delay, blacklisted, error_code)
"""


def get_proxies(only_blacklisted=False):
    q = {'blacklisted': True} if only_blacklisted else {}
    proxies = q_get_proxies(q)
    proxies_df = pd.DataFrame(proxies)
    return proxies_df


def save_a_proxy_test(proxy_test):
    # proxy_test = {'ip': ip, 'port': port, 'delay': delay, 'blacklisted': blacklisted}
    q_update_a_proxy_test(proxy_test)


def save_proxies(proxies_df):
    for _, row in proxies_df.iterrows():
        proxy = row.to_dict()
        q_save_a_proxy(proxy)


def set_all_proxies(delay=0, blacklisted=False, error_code=-1):
    for proxy in q_get_proxies({}):
        proxy_test = proxy
        proxy_test['delay'] = delay
        proxy_test['blacklisted'] = blacklisted
        proxy_test['error_code'] = error_code
        q_update_a_proxy_test(proxy_test)


if __name__ == '__main__':
    # get_proxies()
    set_all_proxies()
