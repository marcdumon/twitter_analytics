# --------------------------------------------------------------------------------------------------------
# 2020/07/07
# src - temp.py
# md
# --------------------------------------------------------------------------------------------------------

import concurrent.futures
import multiprocessing as mp
import os
import sys
from queue import Empty

import numpy as np
import time

from aiohttp import ServerDisconnectedError, ClientOSError
from concurrent.futures import TimeoutError
from database.proxy_facade import get_proxies
from database.twitter_facade import get_profiles, get_usernames, set_a_scrape_flag, reset_all_scrape_flags

from datetime import datetime, date, timedelta
import pandas as pd

from business.proxy_manager import get_a_proxy_server
from business.proxy_scraper import ProxyScraper
from business.twitter_scraper import TweetScraper, ProfileScraper
from config import USERS_LIST, SCRAPE_WITH_PROXY, END_DATE, BEGIN_DATE, SCRAPE_ONLY_MISSING_DATES, TEST_USERNAME, TIME_DELTA, DATA_TYPES
from database.proxy_facade import save_proxies
from database.twitter_facade import get_join_date, get_nr_tweets_per_day, save_tweets, save_profiles
from tools.logger import logger


def scrape_users_tweets():
    reset_all_scrape_flags()
    usernames_df = get_usernames()
    # Proxy queue
    proxy_queue = populate_proxy_queue()
    usernames = [(username, proxy_queue) for username in usernames_df['username']]
    with mp.Pool(processes=25) as pool:
        result = pool.starmap(scrape_a_user_tweets, usernames)


def populate_proxy_queue():
    proxy_df = get_proxies(blacklisted=False, max_delay=25)
    proxy_df.sort_values(by='delay')
    manager = mp.Manager()
    proxy_queue = manager.Queue()
    for _, proxy in proxy_df.iterrows():
        proxy_queue.put((proxy['ip'], proxy['port']))
    return proxy_queue


def scrape_a_user_tweets(username, proxy_queue):
    if proxy_queue.qsize()==0: # Todo: risk to have double proxies, if a runing process puts back a proxy
        proxy_queue=populate_proxy_queue()
    set_a_scrape_flag(username, 'START')
    periods_to_scrape = _determine_scrape_periods(username)
    for (begin_date, end_date) in periods_to_scrape:
        success, fail_counter = False, 0
        while (not success) and (fail_counter < 10):
            try:
                tweets_df, proxy = _scrape_a_user_tweets(username, proxy_queue, begin_date, end_date)
                if not tweets_df.empty:
                    logger.info(f'Saving {len(tweets_df)} tweets')
                    save_tweets(tweets_df)
                set_a_scrape_flag(username, 'END')
                proxy_queue.put(proxy)
                success = True

            except ValueError as e:
                set_a_scrape_flag(username, 'ValueError')
                fail_counter += 1
                time.sleep(10 * fail_counter)
                logger.error(f'Error for username {username}. Fail counter = {fail_counter}\n{e}')

            except ServerDisconnectedError as e:
                set_a_scrape_flag(username, 'ServerDisconnectedError')
                fail_counter += 1
                time.sleep(10 * fail_counter)
                logger.error(f'Error for username {username}. Fail counter = {fail_counter}\n{e}')

            except ClientOSError as e:
                set_a_scrape_flag(username, 'ClientOSError')
                fail_counter += 1
                time.sleep(10 * fail_counter)
                logger.error(f'Error for username {username}. Fail counter = {fail_counter}\n{e}')

            except TimeoutError as e:
                set_a_scrape_flag(username, 'TimeoutError')
                fail_counter += 1
                time.sleep(10 * fail_counter)
                logger.error(f'Error for username {username}. Fail counter = {fail_counter}\n{e}')

            # except TimeoutError as e:
            #     set_a_scrape_flag(username, 'TimeoutError')
            # except TimeoutError as e:
            #     set_a_scrape_flag(username, 'TimeoutError')
            # except TimeoutError as e:
            #     set_a_scrape_flag(username, 'TimeoutError')
            # except TimeoutError as e:
            #     set_a_scrape_flag(username, 'TimeoutError')
            # except TimeoutError as e:
            #     set_a_scrape_flag(username, 'TimeoutError')
            # except TimeoutError as e:
            #     set_a_scrape_flag(username, 'TimeoutError')

            except:
                print('x' * 3000)
                print(sys.exc_info()[0])
                # Do something with the proxy
                raise


def _scrape_a_user_tweets(username, proxy_queue, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    tweet_scraper = TweetScraper(username, begin_date, end_date)
    ip, port = '0.0.0.0', '0'

    if SCRAPE_WITH_PROXY:
        print(f'Len proxy queue = {proxy_queue.qsize()}')
        ip, port = proxy_queue.get()
        tweet_scraper.proxy_server = {'ip': ip, 'port': port}
    tweet_scraper.twint_hide_terminal_output = True
    logger.info(f'Start scraping tweets for: {username} starting on {begin_date.date()} and ending on {end_date.date()} with proxy {ip}:{port} ')
    tweets_df = tweet_scraper.execute_scraping()
    return tweets_df, (ip, port)


def _determine_scrape_periods(username):
    # no need to scrape before join_date
    join_date = get_join_date(username)
    begin_date, end_date = max(BEGIN_DATE, join_date), min(END_DATE, datetime.today())
    # no need to scrape same dates again
    if SCRAPE_ONLY_MISSING_DATES:
        scrape_periods = _get_periods_without_min_tweets(username, begin_date=begin_date, end_date=end_date, min_tweets=1)
    else:
        scrape_periods = [(begin_date, end_date)]
    scrape_periods = _split_periods(scrape_periods)
    return scrape_periods


def _get_periods_without_min_tweets(username, begin_date, end_date, min_tweets=1):
    """
    Gets all the dates when 'username' has 'min_tweets' nr of tweets stored in the database.
    Returns a list of tuples with begin and end date of the period when the 'username' has less or equal amounts of tweets as 'min_tweets' stored in the database.
    We split the periods that are longer than TIME_DELTA.
    """
    # A bit hacky !!! but it works
    days_with_tweets = get_nr_tweets_per_day(username, begin_date, end_date)
    days_with_tweets = days_with_tweets[days_with_tweets['nr_tweets'] >= min_tweets]
    days_with_tweets = [d.to_pydatetime() for d in days_with_tweets['date'] if begin_date < d.to_pydatetime() < end_date]
    # add begin_date at the beginning en end_date + 1 day at the end
    days_with_tweets.insert(0, begin_date - timedelta(days=1))  # begin_date - 1 day because we'll add a day when creating the dateranges
    days_with_tweets.append((end_date + timedelta(days=1)))

    # Create the periods without min_tweets amount of saved tweets
    missing_tweets_periods = [(b + timedelta(days=1), e - timedelta(days=1))
                              for b, e in zip(days_with_tweets[:-1], days_with_tweets[1:])  # construct the periods
                              if e - b > timedelta(days=1)]
    return missing_tweets_periods


def _split_periods(periods):
    # Split the periods into parts with a maximal length of 'TIME_DELTA' days
    splitted_periods = []
    for b, e in periods:
        if e - b <= timedelta(days=TIME_DELTA):
            splitted_periods.append((b, e))
        else:
            while e - b >= timedelta(days=TIME_DELTA):
                splitted_periods.append((b, b + timedelta(days=TIME_DELTA - 1)))
                b = b + timedelta(days=TIME_DELTA)
                # The last part of the splitting
                if e - b < timedelta(TIME_DELTA):
                    splitted_periods.append((b, e))
    return splitted_periods


if __name__ == '__main__':
    # main_process()
    scrape_users_tweets()
