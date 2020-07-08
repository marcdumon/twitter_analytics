# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - twitter_scraper.py
# md
# --------------------------------------------------------------------------------------------------------
import inspect
import os
import sys
import time
from asyncio import TimeoutError
from datetime import datetime, timedelta

import pandas as pd
import twint
import twint.output
import twint.storage.panda
from aiohttp import ServerDisconnectedError, ClientHttpProxyError, ClientProxyConnectionError, ClientOSError

from business.proxy_manager import get_a_proxy_server
from config import SCRAPE_WITH_PROXY
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()


class TwitterScraper:
    """
    Base class to scrape twitter tweets and profile for a username and send that data to the the scraping_controller for further handeling
    """
    _name = ''
    _twint_command = None
    _use_proxy_server = SCRAPE_WITH_PROXY

    def __init__(self, username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
        self.username = username
        self.begin_date, self.end_date = begin_date, end_date
        self.proxy_server = get_a_proxy_server() if self._use_proxy_server else None  # Tocheck:

    def execute_scraping(self):
        while True:
            try:
                scraped_df = self._scrape_using_twint()
                return scraped_df
            except:
                print('error')
                raise

    def _scrape_using_twint(self):
        c = twint.Config()
        twint.storage.panda.clean()
        c.Username = self.username
        c.Debug = False
        c.Pandas = True
        c.Since = datetime.strftime(self.begin_date, '%Y-%m-%d')
        c.Until = datetime.strftime(self.end_date, '%Y-%m-%d')
        if self.proxy_server:
            c.Proxy_host, c.Proxy_port = self.proxy_server['ip'], self.proxy_server['port']
            c.Proxy_type = 'http'
            logger.debug(f"Scraping using proxy server {self.proxy_server['ip']}:{self.proxy_server['port']}")
        self._twint_command(c)
        # Get both tweets_df and profile; One of them will be None
        tweets_df = twint.storage.panda.Tweets_df
        profile_df = twint.storage.panda.User_df
        twitter_df = tweets_df if profile_df is None else profile_df
        return twitter_df


class TweetScraper(TwitterScraper):
    def __init__(self, username, begin_date='2000-01-01', end_date='2035-01-01'):
        self._name = 'tweets'
        self._twint_command = twint.run.Search
        super(TweetScraper, self).__init__(username, begin_date, end_date)


class ProfileScraper(TwitterScraper):
    def __init__(self, username):
        self._name = 'profile'
        self._twint_command = twint.run.Lookup
        super(ProfileScraper, self).__init__(username)


if __name__ == '__main__':
    # x = TwitterScraper('xxx')
    xx = TweetScraper('smienos')

    xx.begin_date = datetime(2018,9,17)
    xx.end_date = datetime(2018,9,19)

    # xx = ProfileScraper('marcdumon', True)
    df = xx._scrape_using_twint()
    print(df)
