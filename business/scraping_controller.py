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
from database.proxy_facade import get_proxies, update_proxy_stats, reset_proxies_scrape_success_flag
from database.proxy_facade import save_proxies
from database.twitter_facade import get_join_date, get_nr_tweets_per_day, save_tweets, save_a_profile, get_a_profile
from database.log_facade import log_scraping_profile, log_scraping_tweets, get_max_sesion_id, get_failed_periods
from database.twitter_facade import get_usernames, reset_all_scrape_flags
from tools.logger import logger

"""
A collection of functions to control scraping and saving proxy servers, Twitter tweets and profiles
"""

####################################################################################################################################################################################
system_cfg = SystemCfg()
scraping_cfg = Scraping_cfg()


# Todo: Refactor: Now: multiprocessing inside instance. Better oudside and eah process creates instance? What about proxy queue shqring ?
#                 Rethink the architecture. TwitterScrapingSession object with interface, mp,...
#                 More generic: Base class: TwitterScraping, clields: TweetsScraping, ProfileScraping, ProxyScraping,

class TwitterScrapingSession:
    def __init__(self):
        self.usersnames_df = pd.DataFrame()
        self.scrape_profiles = False
        self.scrape_tweets = False
        manager = mp.Manager()
        self.proxy_queue = manager.Queue()

        self.n_processes = scraping_cfg.n_processes
        self.rescrape = False
        self.session_begin_date = scraping_cfg.session_begin_date
        self.session_end_date = scraping_cfg.session_end_date
        self.timedelta = scraping_cfg.time_delta
        self.max_proxy_delay = scraping_cfg.max_proxy_delay
        self.max_fails = scraping_cfg.max_fails
        self.missing_dates = scraping_cfg.missing_dates
        self.min_tweets = scraping_cfg.min_tweets
        self.session_id = get_max_sesion_id() + 1
        logger.info(
            f'Start Twitter Scraping. | n_processes={self.n_processes}, session_id={self.session_id}, '
            f'session_begin_date={self.session_begin_date}, session_end_date={self.session_end_date}, timedelta={self.timedelta}, missing_dates={self.missing_dates}')

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # INTERFACE
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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

    def rescrape_failed_periods(self, session_id):
        self.rescrape = True
        self.usersnames_df = get_failed_periods(session_id=session_id)
        self.session_id *= -1
        self.scrape_tweets = True
        return self

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ENGINE
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    # @property
    def start_scraping(self):
        # session_begin_date=session_begin_date if session_begin_date else scraping_cfg.session_begin_date
        # session_end_date = session_end_date if session_end_date else scraping_cfg.session_end_date

        if not (self.scrape_profiles or self.scrape_tweets):
            logger.warning(f'Nothing to do. Did you forget "profiles" or "tweets" instruction?')
            return None
        if self.usersnames_df.empty:
            logger.warning(f'Nothing to do. Did you forget to set "all_users" or "users_list"? Or all users already exist?')
            return None
        processes = min(len(self.usersnames_df), self.n_processes)
        if self.scrape_profiles:
            self._populate_proxy_queue()
            print(self.usersnames_df)
            # mp_iterable = [(username,) for _, (_, username) in self.usersnames_df.iterrows()]
            mp_iterable = [(username,) for username in self.usersnames_df['username']]
            with mp.Pool(processes=processes) as pool:
                pool.starmap(self.scrape_a_user_profile, mp_iterable)
        if self.scrape_tweets:
            self._populate_proxy_queue()
            if self.rescrape:
                mp_iterable = [(username, begin_date, end_date) for _, (username, begin_date, end_date) in self.usersnames_df.iterrows()]

            else:
                mp_iterable = [(username, scraping_cfg.session_begin_date, scraping_cfg.session_end_date) for username in self.usersnames_df['username']]
            with mp.Pool(processes=processes) as pool:
                pool.starmap(self.scrape_a_user_tweets, mp_iterable)

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # PROFILES
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def scrape_a_user_profile(self, username):  # Todo: implement Exception trapping + proxy stats
        self._check_proxy_queue()
        proxy = self.proxy_queue.get()
        profile_scraper = ProfileScraper(username)
        profile_scraper.proxy_server = proxy

        logger.info(f'Start scraping profile | {username}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}')
        log_scraping_profile(self.session_id, 'begin', 'profile', username, proxy=proxy)

        profile_df = profile_scraper.execute_scraping()
        if not profile_df.empty:
            logger.info(f'Saving profile | {username}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}')
            save_a_profile(profile_df)

        log_scraping_profile(self.session_id, 'end', 'profile', username)
        self._release_proxy_server(proxy)

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # TWEETS
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def scrape_a_user_tweets(self, username, session_begin_date, session_end_date):
        log_scraping_tweets(self.session_id, 'begin', 'session', username, self.session_begin_date, self.session_end_date)
        self._check_proxy_queue()
        periods_to_scrape = self._calculate_scrape_periods(username, session_begin_date, session_end_date)
        for period_begin_date, period_end_date in periods_to_scrape:
            fail_counter = 0
            while fail_counter < self.max_fails:
                proxy = self.proxy_queue.get()
                logger.info(
                    f'Start scraping tweets | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
                tweet_scraper = TweetScraper(username, period_begin_date, period_end_date)
                tweet_scraper.proxy_server = proxy
                try:
                    tweets_df = tweet_scraper.execute_scraping()
                except ValueError as e:
                    fail_counter += 1
                    self.handle_error('ValueError', e, username, period_begin_date, period_end_date, proxy, fail_counter)
                except ServerDisconnectedError as e:
                    fail_counter += 1
                    self.handle_error('ServerDisconnectedError', e, username, period_begin_date, period_end_date, proxy, fail_counter)
                except ClientOSError as e:
                    fail_counter += 1
                    self.handle_error('ClientOSError', e, username, period_begin_date, period_end_date, proxy, fail_counter)
                except TimeoutError as e:
                    fail_counter += 1
                    self.handle_error('TimeoutError', e, username, period_begin_date, period_end_date, proxy, fail_counter)
                except ClientHttpProxyError as e:
                    fail_counter += 1
                    self.handle_error('ClientHttpProxyError', e, username, period_begin_date, period_end_date, proxy, fail_counter)
                except IndexError as e:
                    fail_counter += 1
                    self.handle_error('IndexError', e, username, period_begin_date, period_end_date, proxy, fail_counter)
                except Empty as e:  # Queue emprt
                    logger.error(
                        f'Empty Error | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}')
                    self._populate_proxy_queue()
                except:
                    print('x' * 3000)
                    print(sys.exc_info()[0])
                    print(sys.exc_info())
                else:
                    logger.info(
                        f'Saving {len(tweets_df)} tweets | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}')
                    if not tweets_df.empty: save_tweets(tweets_df)
                    log_scraping_tweets(self.session_id, 'ok', 'period', username, period_begin_date, end_date=period_end_date, n_tweets=len(tweets_df), proxy=proxy)
                    update_proxy_stats('ok', proxy)
                    break  # the wile-loop
                finally:
                    self._release_proxy_server(proxy)
                    if fail_counter >= self.max_fails:
                        txt = f'FAIL | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}'
                        logger.error(txt)
                        log_scraping_tweets(self.session_id, 'fail', 'period', username, period_begin_date, period_end_date, proxy=proxy)

        # All periods scraped.
        log_scraping_tweets(self.session_id, 'end', 'session', username, self.session_begin_date, self.session_end_date)

    def handle_error(self, flag, e, username, period_begin_date, period_end_date, proxy, fail_counter):
        txt = f'{flag} | {username}, {period_begin_date} | {period_end_date}, {proxy["ip"]}:{proxy["port"]}, queue={self.proxy_queue.qsize()}, fail={fail_counter}'
        logger.warning(txt)
        logger.warning(e)
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

    def _calculate_scrape_periods(self, username, session_begin_date, session_end_date):
        # no need to start_scraping before join_date
        join_date = get_join_date(username)
        session_begin_date, session_end_date = max(session_begin_date, join_date.date()), min(session_end_date, datetime.today().date())

        # no need to start_scraping same dates again
        if self.missing_dates:
            scrape_periods = self._get_periods_without_min_tweets(username, session_begin_date=session_begin_date, session_end_date=session_end_date)
        else:
            scrape_periods = [(session_begin_date, session_end_date)]
        scrape_periods = self._split_periods(scrape_periods)
        return scrape_periods

    def _get_periods_without_min_tweets(self, username, session_begin_date, session_end_date):
        """
        Gets all the dates when 'username' has 'min_tweets' nr of tweets stored in the database.
        Returns a list of tuples with session_begin_date and session_end_date date of the period when the 'username' has less or equal amounts of tweets as 'min_tweets' stored in the database.
        We split the periods that are longer than TIME_DELTA.
        """
        # A bit hacky !!! but it works
        # Todo: Refactor: use df.query() here?
        days_with_tweets = get_nr_tweets_per_day(username, session_begin_date, session_end_date)  # Can return empty ex elsampe sd=datetime(2011,12,31) ed=datetime(2012,1,1)
        if not days_with_tweets.empty:  # Can return empty ex elsampe sd=datetime(2011,12,31) ed=datetime(2012,1,1)
            days_with_tweets = days_with_tweets[days_with_tweets['nr_tweets'] >= self.min_tweets]
            days_with_tweets = [d.to_pydatetime() for d in days_with_tweets['date'] if session_begin_date < d.to_pydatetime() < session_end_date]
            # add session_begin_date at the beginning en session_end_date + 1 day at the session_end_date
            days_with_tweets.insert(0, session_begin_date - timedelta(days=1))  # session_begin_date - 1 day because we'loging_level add a day when creating the dateranges
            days_with_tweets.append((session_end_date + timedelta(days=1)))

            # Create the periods without min_tweets amount of saved tweets
            missing_tweets_periods = [(b + timedelta(days=1), e - timedelta(days=1))
                                      for b, e in zip(days_with_tweets[:-1], days_with_tweets[1:])  # construct the periods
                                      if e - b > timedelta(days=1)]
            return missing_tweets_periods
        else:
            return [(session_begin_date, session_end_date)]

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

    def _populate_proxy_queue(self): # Todo: Trow out every proxy with problems. Reload fast besed on ratio ok/fail
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
    scrape.tweets.users_list(['FranckenTheo', 'xxxxxx', 'smienos'], False).start_scraping()
