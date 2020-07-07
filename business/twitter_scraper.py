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

from business.proxy_controller import get_a_proxy_server
from config import SCRAPE_WITH_PROXY
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()


class TwitterScraper:
    """
    Base class to scrape twitter tweets and profile for a username and send that data to the database for storage
    """
    _name = ''
    _twint_command = None
    _use_proxy_server = SCRAPE_WITH_PROXY

    # Todo: Is it better to make username a class variable and dates instance variables of all instance or all class variables.
    #       What are the consequences for multiprocessing?
    # username = '' # I choose to make username and dates instance variables because it's more beautifull

    def __init__(self, username, begin_date='2000-01-01', end_date='2000-01-01'):
        self.username = username
        self.begin_date, self.end_date = begin_date, end_date
        self.proxy_server = get_a_proxy_server() if self._use_proxy_server else None  # Tocheck:

    def get_twitter_data(self):
        c = twint.Config()
        twint.storage.panda.clean()
        c.Username = self.username
        c.Debug = False
        c.Pandas = True
        c.Since = self.begin_date
        c.Until = self.end_date
        if self.proxy_server:
            c.Proxy_host, c.Proxy_port = self.proxy_server['ip'], self.proxy_server['port']
            c.Proxy_type = 'http'
        self._twint_command(c)
        # Get both Tweets_df and User_df; One of them will be None
        tweets_df = twint.storage.panda.Tweets_df
        profile_df = twint.storage.panda.User_df
        return tweets_df if profile_df is None else profile_df

    def execute_scraping(self):
        logger.info(f'Start scraping {self._name} for user: {self.username} starting on {self.begin_date} and ending on {self.end_date}')

        try:
            success = False
            while not success:
                twitter_df = self.get_twitter_data()
                print('=' * 150)
                print(twitter_df)
                print('=' * 150)
                success = True
                1/0
        except:
            pass


class TweetScraper(TwitterScraper):
    def __init__(self, username, begin_date='2000-01-01', end_date='2035-01-01'):
        self._name = 'tweets'
        self._twint_command = twint.run.Search
        super(TweetScraper, self).__init__(username,begin_date,end_date)

        pass


class ProfileScraper(TwitterScraper):
    def __init__(self, username):
        self._name = 'profile'
        self._twint_command = twint.run.Lookup
        super(ProfileScraper, self).__init__(username)


if __name__ == '__main__':
    x = TwitterScraper('xxx')
    xx = TweetScraper('marcdumon')
    xx.begin_date = '2020-01-01'
    xx.end_date = '2020-02-01'

    # xx = ProfileScraper('marcdumon', True)
    xx.get_twitter_data()
