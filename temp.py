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

from aiohttp import ServerDisconnectedError, ClientOSError, ClientHttpProxyError
from concurrent.futures import TimeoutError
from database.proxy_facade import get_proxies, set_a_proxy_success_flag, reset_proxies_success_flag
from database.twitter_facade import get_profiles, get_usernames, set_a_scrape_flag, reset_all_scrape_flags

from datetime import datetime, date, timedelta
import pandas as pd

from business.proxy_scraper import ProxyScraper
from business.twitter_scraper import TweetScraper, ProfileScraper
from config import USERS_LIST, SCRAPE_WITH_PROXY, END_DATE, BEGIN_DATE, SCRAPE_ONLY_MISSING_DATES, TEST_USERNAME, TIME_DELTA, DATA_TYPES
from database.proxy_facade import save_proxies
from database.twitter_facade import get_join_date, get_nr_tweets_per_day, save_tweets, save_profiles
from tools.logger import logger

from tools.utils import dt2str, str2d


def reset_users_proxies():
    reset_proxies_success_flag()
    reset_all_scrape_flags()


def scrape_users_tweets(restart=False, processes=10, max_delay=15):
    usernames_df = get_usernames()
    if restart: usernames_df = usernames_df[usernames_df['scrape_flag'] != 'END']
    logger.info(f'Scraping {len(usernames_df)} users. using {2} processes/threads and proxies with max_delay {max_delay} sec.')
    proxy_queue = populate_proxy_queue(max_delay=max_delay)
    usernames = [(username, proxy_queue) for username in usernames_df['username']]
    # Todo: What's better, multiprocess or multi threads ?
    # with mp.Pool(processes=2) as pool:
    with mp.pool.ThreadPool(processes=processes) as pool:
        result = pool.starmap(scrape_a_user_tweets, usernames)


def populate_proxy_queue(proxy_queue=None, max_delay=3):
    proxy_df = get_proxies(blacklisted=False, max_delay=max_delay, success=True)
    if not proxy_queue:  # New queue
        manager = mp.Manager()
        proxy_queue = manager.Queue()
    for _, proxy in proxy_df.iterrows():
        proxy_queue.put((proxy['ip'], proxy['port']))
    logger.info(f'Proxy queue poulates. Contains {proxy_queue.qsize()} servers')
    return proxy_queue


def scrape_a_user_tweets(username, proxy_queue):
    if proxy_queue.qsize() <= 1:  # Risk of double proxies when a thread put banck the proxy
        proxy_queue = populate_proxy_queue(proxy_queue)

    set_a_scrape_flag(username, 'START')
    periods_to_scrape = _determine_scrape_periods(username)
    for (begin_date, end_date) in periods_to_scrape:
        success, fail_counter = False, 0
        bd, ed = dt2str(begin_date), dt2str(end_date)
        while (not success) and (fail_counter < 5):
            proxy = {}
            if SCRAPE_WITH_PROXY:
                # Todo: proxy_queue already created, now descide to use it or not?
                logger.info(f'Len proxy queue = {proxy_queue.qsize()}')
                ip, port = proxy_queue.get()
                proxy = {'ip': ip, 'port': port}
            try:
                tweets_df = _scrape_a_user_tweets(username, proxy, begin_date, end_date)
                if not tweets_df.empty:
                    logger.info(f'Saving {len(tweets_df)} tweets for {username}. {bd} - {ed} ')
                    save_tweets(tweets_df)
                    set_a_scrape_flag(username, 'BUSSY')
                logger.warning(f'Put back proxy {proxy}')
                proxy_queue.put((proxy['ip'], proxy['port']))
                set_a_proxy_success_flag(proxy, True)
                success = True


            except ValueError as e:
                set_a_scrape_flag(username, 'ValueError')
                fail_counter += 1
                logger.error(f'ValueError Error for username {username}. {bd} - {ed} - Fail counter = {fail_counter}')
                logger.error(e)
                time.sleep(10 * fail_counter)
                logger.error(f'Put back proxy {proxy} after ValueError Error for username {username}. {bd} - {ed}')
                proxy_queue.put((proxy['ip'], proxy['port']))
                set_a_proxy_success_flag(proxy, False)

            except ServerDisconnectedError as e:
                set_a_scrape_flag(username, 'ServerDisconnectedError')
                fail_counter += 1
                logger.error(f'ServerDisconnectedError Error for username {username}. {bd} - {ed} - Fail counter = {fail_counter}')
                logger.error(e)
                time.sleep(10 * fail_counter)
                logger.error(f'Put back proxy {proxy} after ServerDisconnectedError Error for username {username}. {bd} - {ed}')
                proxy_queue.put((proxy['ip'], proxy['port']))
                set_a_proxy_success_flag(proxy, False)

            except ClientOSError as e:
                set_a_scrape_flag(username, 'ClientOSError')
                fail_counter += 1
                logger.error(f'ClientOSError Error for username {username}. {bd} - {ed} - Fail counter = {fail_counter}')
                logger.error(e)
                time.sleep(10 * fail_counter)
                logger.error(f'Put back proxy {proxy} after ClientOSError Error for username {username}. {bd} - {ed}')
                proxy_queue.put((proxy['ip'], proxy['port']))
                set_a_proxy_success_flag(proxy, False)


            except TimeoutError as e:
                set_a_scrape_flag(username, 'TimeoutError')
                fail_counter += 1
                logger.error(f'TimeoutError Error for username {username}. {bd} - {ed} - Fail counter = {fail_counter}')
                logger.error(e)
                time.sleep(10 * fail_counter)
                logger.error(f'Put back proxy {proxy} after TimeoutError Error for username {username}. {bd} - {ed}')
                proxy_queue.put((proxy['ip'], proxy['port']))
                set_a_proxy_success_flag(proxy, False)

            except ClientHttpProxyError as e:
                set_a_scrape_flag(username, 'ClientHttpProxyError')
                fail_counter += 1
                logger.error(f'ClientHttpProxyError Error for username {username}. {bd} - {ed} - Fail counter = {fail_counter}')
                logger.error(e)
                time.sleep(10 * fail_counter)
                logger.error(f'Put back proxy {proxy} after ClientHttpProxyError Error for username {username}. {bd} - {ed}')
                proxy_queue.put((proxy['ip'], proxy['port']))
                set_a_proxy_success_flag(proxy, False)

            except IndexError as e:
                set_a_scrape_flag(username, 'IndexError')
                fail_counter += 1
                logger.error(f'IndexError Error for username {username}. {bd} - {ed} - Fail counter = {fail_counter}')
                logger.error(e)
                time.sleep(10 * fail_counter)
                logger.error(f'Put back proxy {proxy} after IndexError Error for username {username}. {bd} - {ed}')
                proxy_queue.put((proxy['ip'], proxy['port']))
                set_a_proxy_success_flag(proxy, False)

            except Empty as e:  # Queue emprt
                set_a_scrape_flag(username, 'Empty')
                fail_counter += 1
                time.sleep(10 * fail_counter)
                logger.error(f'Empty Error for username {username}. {bd} - {ed} - Fail counter = {fail_counter}. Repopulate queue')
                populate_proxy_queue()

            except:
                print('x' * 3000)
                print(sys.exc_info()[0])
                print(sys.exc_info())
                # Do something with the proxy
                raise

    set_a_scrape_flag(username, 'END')


def _scrape_a_user_tweets(username, proxy=None, begin_date=str2d('2000-01-01'), end_date=str2d('2035-01-01')):
    tweet_scraper = TweetScraper(username, begin_date, end_date)
    if proxy: tweet_scraper.proxy_server = proxy
    tweet_scraper.twint_hide_terminal_output = True
    tweet_scraper.twint_show_stats = False
    tweet_scraper.twint_show_count = False
    logger.info(f'Start scraping tweets for: {username} starting on {begin_date.date()} and ending on {end_date.date()} with proxy {proxy}')
    tweets_df = tweet_scraper.execute_scraping()
    return tweets_df


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
    days_with_tweets = get_nr_tweets_per_day(username, begin_date, end_date)  # Can return empty ex elsampe sd=datetime(2011,12,31) ed=datetime(2012,1,1)
    if not days_with_tweets.empty:  # Can return empty ex elsampe sd=datetime(2011,12,31) ed=datetime(2012,1,1)
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
    else:
        return [(begin_date, end_date)]


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
    # reset_all_scrape_flags()
    # _scrape_a_user_tweets('franckentheo', None, datetime(2020, 1, 1), datetime(2020, 2, 1))
    scrape_users_tweets()
    # reset_users_proxies()
