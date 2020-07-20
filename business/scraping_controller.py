# --------------------------------------------------------------------------------------------------------
# 2020/07/06
# src - scraping_controller.py
# md
# --------------------------------------------------------------------------------------------------------

import multiprocessing as mp
import sys
import time
from concurrent.futures import TimeoutError
from datetime import datetime, timedelta
from queue import Empty

import pandas as pd
from aiohttp import ServerDisconnectedError, ClientOSError, ClientHttpProxyError

from business.proxy_scraper import ProxyScraper
from business.twitter_scraper import TweetScraper, ProfileScraper
from database.config_facade import SystemCfg, Scraping_cfg
from database.proxy_facade import get_proxies, update_proxy_stats
from database.proxy_facade import save_proxies
from database.twitter_facade import get_join_date, get_nr_tweets_per_day, save_tweets, save_a_profile, get_a_profile
from database.log_facade import log_scraping_profile, log_scraping_tweets, get_max_sesion_id
from database.twitter_facade import get_usernames, reset_all_scrape_flags
from tools.logger import logger

"""
A collection of functions to control scraping and saving proxy servers, Twitter tweets and profiles
"""

# See: https://www.cloudcity.io/blog/2019/02/27/things-i-wish-they-told-me-about-multiprocessing-in-python/

####################################################################################################################################################################################
# Todo: Refactor: Now: multiprocessing inside instance. Better oudside and eah process creates instance? What about proxy queue shqring ?
system_cfg = SystemCfg()
scraping_cfg = Scraping_cfg()


class TwitterScrapingSession:
    def __init__(self):
        self.usersnames_df = pd.DataFrame()
        self.scrape_profiles = False
        self.scrape_tweets = False
        manager = mp.Manager()
        self.proxy_queue = manager.Queue()

        self.n_processes = scraping_cfg.n_processes
        self.begin_date = scraping_cfg.begin
        self.end_date = min(scraping_cfg.end, datetime.today().date())
        self.timedelta = scraping_cfg.time_delta
        self.max_proxy_delay = scraping_cfg.max_proxy_delay
        self.max_fails = scraping_cfg.max_fails
        self.missing_dates = scraping_cfg.missing_dates
        self.min_tweets = scraping_cfg.min_tweets
        self.session_id = get_max_sesion_id() + 1
        logger.info(
            f'Start Twitter Scraping. | n_processes={self.n_processes}, begin_date={self.begin_date}, end_date={self.end_date}, timedelta={self.timedelta}, missing_dates={self.missing_dates}')

    @property
    def all_users(self):
        self.usersnames_df = get_usernames()
        return self

    def sample_users(self, samples=10):
        self.usersnames_df = get_usernames()
        self.usersnames_df = self.usersnames_df.sample(samples)
        return self

    def users_list(self, usernames, only_new=True):
        usernames = [u.lower() for u in usernames]
        if only_new:
            for username in usernames.copy():  # Todo: verify copy() is necessary
                if get_a_profile(username):
                    logger.debug(f'Username {username} already exists')
                    usernames.remove(username)
        self.usersnames_df = pd.DataFrame(usernames, columns=['username'])
        return self

    @property
    def profiles(self):
        self.scrape_profiles = True
        return self

    @property
    def tweets(self):
        self.scrape_tweets = True
        return self

    # @property
    def start_scraping(self):
        if not (self.scrape_profiles or self.scrape_tweets):
            logger.warning(f'Nothing to do. Did you forget "profiles" or "tweets" instruction?')
            return None
        if self.usersnames_df.empty:
            logger.warning(f'Nothing to do. Did you forget to set "all_users" or "users_list"? Or all users already exist?')
            return None

        mp_iterable = [(username,) for username in self.usersnames_df['username']]
        mp_funcs = []
        if self.scrape_profiles: mp_funcs.append(self.scrape_a_user_profile)
        if self.scrape_tweets: mp_funcs.append(self.scrape_a_user_tweets)
        processes = min(len(self.usersnames_df), self.n_processes)
        for mp_func in mp_funcs:
            self._populate_proxy_queue()
            with mp.Pool(processes=processes) as pool:
                result = pool.starmap(mp_func, mp_iterable)

    def scrape_a_user_profile(self, username): # Todo: implement Exception trapping + proxy stats
        self._check_proxy_queue()
        proxy = self.proxy_queue.get()
        profile_scraper = ProfileScraper(username)
        profile_scraper.proxy_server = proxy

        logger.info(f'Start scraping profile | {username}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}')
        log_scraping_profile(self.session_id,'start', 'profile', username, proxy=proxy)

        profile_df = profile_scraper.execute_scraping()
        if not profile_df.empty:
            logger.info(f'Saving profile | {username}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}')
            save_a_profile(profile_df)

        log_scraping_profile(self.session_id,'end', 'profile', username)
        self._release_proxy_server(proxy)

    def scrape_a_user_tweets(self, username):
        log_scraping_tweets(self.session_id,'begin', 'session', username, self.begin_date, self.end_date)
        self._check_proxy_queue()
        periods_to_scrape = self._calculate_scrape_periods(username)
        for begin_date, end_date in periods_to_scrape:
            fail_counter = 0
            while fail_counter < self.max_fails:
                proxy = self.proxy_queue.get()
                logger.info(
                    f'Start scraping tweets | {username}, {begin_date} | {end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
                tweet_scraper = TweetScraper(username, begin_date, end_date)
                tweet_scraper.proxy_server = proxy
                try:
                    tweets_df = tweet_scraper.execute_scraping()
                except ValueError as e:
                    fail_counter += 1
                    self.handle_error('ValueError', e, username, begin_date, end_date, proxy, fail_counter)
                except ServerDisconnectedError as e:
                    fail_counter += 1
                    self.handle_error('ServerDisconnectedError', e, username, begin_date, end_date, proxy, fail_counter)
                except ClientOSError as e:
                    fail_counter += 1
                    self.handle_error('ClientOSError', e, username, begin_date, end_date, proxy, fail_counter)
                except TimeoutError as e:
                    fail_counter += 1
                    self.handle_error('TimeoutError', e, username, begin_date, end_date, proxy, fail_counter)
                except ClientHttpProxyError as e:
                    fail_counter += 1
                    self.handle_error('ClientHttpProxyError', e, username, begin_date, end_date, proxy, fail_counter)
                except IndexError as e:
                    fail_counter += 1
                    self.handle_error('IndexError', e, username, begin_date, end_date, proxy, fail_counter)
                except Empty as e:  # Queue emprt
                    logger.error(
                        f'Empty Error | {username}, {begin_date} | {end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
                    self._populate_proxy_queue()
                except:
                    print('x' * 3000)
                    print(sys.exc_info()[0])
                    print(sys.exc_info())
                else:
                    logger.info(
                        f'Saving {len(tweets_df)} tweets | {username}, {begin_date} | {end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}')
                    if not tweets_df.empty: save_tweets(tweets_df)
                    log_scraping_tweets(self.session_id,'ok', 'period', username, begin_date, end_date=end_date, n_tweets=len(tweets_df), proxy=proxy)
                    update_proxy_stats('ok', proxy)
                    break  # the wile-loop
                finally:
                    self._release_proxy_server(proxy)
                    if fail_counter >= self.max_fails:
                        log_scraping_tweets(self.session_id,'fail', 'period', username, begin_date, end_date, proxy=proxy)
        # All periods scraped.
        log_scraping_tweets(self.session_id,'end', 'session', username, self.begin_date, self.end_date)

    def handle_error(self, flag, e, username, begin_date, end_date, proxy, fail_counter):
        logger.error(
            f'ValueError | {username}, {begin_date} | {end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
        logger.error(e)
        update_proxy_stats(flag, proxy)
        time.sleep(10)

    def _release_proxy_server(self, proxy):
        logger.info(f'Put back proxy {proxy["ip"]}:{proxy["port"]}')
        self.proxy_queue.put({'ip': proxy['ip'], 'port': proxy['port']})

    def _check_proxy_queue(self):
        if self.proxy_queue.qsize() <= 1:
            self.max_proxy_delay *= 1.2
            self._populate_proxy_queue()
            logger.debug(f' | proxies queue length: {self.proxy_queue.qsize()}')

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def _calculate_scrape_periods(self, username):
        # no need to start_scraping before join_date
        join_date = get_join_date(username)
        begin_date, end_date = max(self.begin_date, join_date.date()), min(self.end_date, datetime.today().date())
        # no need to start_scraping same dates again
        if self.missing_dates:
            scrape_periods = self._get_periods_without_min_tweets(username, begin_date=begin_date, end_date=end_date)
        else:
            scrape_periods = [(begin_date, end_date)]
        scrape_periods = self._split_periods(scrape_periods)
        return scrape_periods

    def _get_periods_without_min_tweets(self, username, begin_date, end_date):
        """
        Gets all the dates when 'username' has 'min_tweets' nr of tweets stored in the database.
        Returns a list of tuples with begin_date and end_date date of the period when the 'username' has less or equal amounts of tweets as 'min_tweets' stored in the database.
        We split the periods that are longer than TIME_DELTA.
        """
        # A bit hacky !!! but it works
        # Todo: Refactor: use df.query() here?
        days_with_tweets = get_nr_tweets_per_day(username, begin_date, end_date)  # Can return empty ex elsampe sd=datetime(2011,12,31) ed=datetime(2012,1,1)
        if not days_with_tweets.empty:  # Can return empty ex elsampe sd=datetime(2011,12,31) ed=datetime(2012,1,1)
            days_with_tweets = days_with_tweets[days_with_tweets['nr_tweets'] >= self.min_tweets]
            days_with_tweets = [d.to_pydatetime() for d in days_with_tweets['date'] if begin_date < d.to_pydatetime() < end_date]
            # add begin_date at the beginning en end_date + 1 day at the end_date
            days_with_tweets.insert(0, begin_date - timedelta(days=1))  # begin_date - 1 day because we'loging_level add a day when creating the dateranges
            days_with_tweets.append((end_date + timedelta(days=1)))

            # Create the periods without min_tweets amount of saved tweets
            missing_tweets_periods = [(b + timedelta(days=1), e - timedelta(days=1))
                                      for b, e in zip(days_with_tweets[:-1], days_with_tweets[1:])  # construct the periods
                                      if e - b > timedelta(days=1)]
            return missing_tweets_periods
        else:
            return [(begin_date, end_date)]

    def _split_periods(self, periods):
        # Split the periods into parts with a maximal length of 'TIME_DELTA' days
        td = self.timedelta
        splitted_periods = []
        for b, e in periods:
            if e - b <= timedelta(days=td):
                splitted_periods.append((b, e))
            else:
                while e - b >= timedelta(days=self.timedelta):
                    splitted_periods.append((b, b + timedelta(days=td - 1)))
                    b = b + timedelta(days=td)
                    # The last part of the splitting
                    if e - b < timedelta(td):
                        splitted_periods.append((b, e))
        return splitted_periods

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def _populate_proxy_queue(self):
        proxy_df = get_proxies(max_delay=self.max_proxy_delay)
        # Shuffle the proxies otherwise always same order
        proxy_df = proxy_df.sample(frac=1., replace=False)
        for _, proxy in proxy_df.iterrows():
            # self.proxy_queue.put((proxy['ip'], proxy['port']))
            self.proxy_queue.put({'ip': proxy['ip'], 'port': proxy['port']})
        logger.warning(f'Proxy queue poulates. Contains {self.proxy_queue.qsize()} servers')


# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


####################################################################################################################################################################################
def scrape_proxies():
    ps = ProxyScraper()
    logger.info('=' * 100)
    logger.info('Start scrapping Proxies')
    logger.info('=' * 100)
    if scraping_cfg.proxies_download_sites['free_proxy_list']:
        logger.info(f'Start scraping proxies from free_proxy_list.net')
        proxies_df = ps.scrape_free_proxy_list()
        save_proxies(proxies_df)
    if scraping_cfg.proxies_download_sites['hide_my_name']:
        logger.info(f'Start scraping proxies from hidemy.name')
        proxies_df = ps.scrape_hide_my_name()
        save_proxies(proxies_df)
    ps.test_proxies()


def reset_proxy_servers():
    reset_proxies_scrape_success_flag()


def reset_scrape_flag():
    reset_all_scrape_flags()


####################################################################################################################################################################################

if __name__ == '__main__':
    pass

    scrape = TwitterScrapingSession()
    # _ = start_scraping.users_list(['FRanckentheo', 'xsdsdaads', 'smienos'], only_new=False).start_scraping
    _ = scrape.tweets.users_list(['FranckenTheo', 'xxxxxx', 'smienos'], False).start_scraping()
