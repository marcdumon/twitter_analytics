# --------------------------------------------------------------------------------------------------------
# 2020/07/08
# src - proxy_facade.py
# md
# --------------------------------------------------------------------------------------------------------

import pandas as pd

from database.proxy_queries import q_save_a_proxy, q_get_proxies, q_update_a_proxy_test, q_set_a_proxy_scrape_success_flag, q_reset_a_proxy_scrape_success_flag
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrieve proxy data via proxy queries from the proxies collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------
- get_proxies(blacklisted=None, max_delay=None)
- save_a_proxy_test(proxy, delay)
- save_proxies(tweets_df)
- set_a_proxy_scrape_success_flag(proxy, scrape_success_flag)
- set_proxies(delay, blacklisted, error_code)
"""


def get_proxies(blacklisted=None, max_delay=None, scrape_success=None):
    q = {}
    if blacklisted: q['blacklisted'] = blacklisted
    if max_delay: q['$and'] = [{'delay': {'$gt': 0}},
                               {'delay': {'$lte': max_delay}}]
    if scrape_success: q['scrape_success'] = scrape_success
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


def set_proxies(delay=0, blacklisted=False, error_code=-1):
    for proxy in q_get_proxies({}):
        proxy_test = proxy
        proxy_test['delay'] = delay
        proxy_test['blacklisted'] = blacklisted
        proxy_test['error_code'] = error_code
        q_update_a_proxy_test(proxy_test)


def set_a_proxy_scrape_success_flag(proxy, scrape_success_flag):
    q_set_a_proxy_scrape_success_flag(proxy, scrape_success_flag)


def reset_proxies_scrape_success_flag():
    for proxy in q_get_proxies({}):
        proxy = {'ip': proxy['ip'], 'port': proxy['port']}
        q_reset_a_proxy_scrape_success_flag(proxy)


if __name__ == '__main__':
    # get_proxies()
    # set_all_proxies()
    # print(get_proxies(blacklisted=False, max_delay=100))
    # print(get_proxies())
    reset_proxies_scrape_success_flag()
