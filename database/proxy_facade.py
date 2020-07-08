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
from database.proxy_queries import q_save_many_proxies, q_save_a_proxy
from database.tweet_queries import q_get_nr_tweets_per_day, q_save_a_tweet
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrieve proxy data via proxy queries from the proxies collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable
"""

"""
IMPLEMENTED FUNCTIONS
- save_proxies(tweets_df)
"""


def save_proxies(proxies_df):
    for _, row in proxies_df.iterrows():
        proxy = row.to_dict()
        q_save_a_proxy(proxy)


if __name__ == '__main__':
    pass
