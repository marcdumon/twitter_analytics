# --------------------------------------------------------------------------------------------------------
# 2020/07/06
# src - proxy_manager.py
# md
# --------------------------------------------------------------------------------------------------------
import sys
import time
import multiprocessing as mp
from datetime import datetime
from asyncio import TimeoutError, CancelledError
from pathlib import Path

from aiohttp import ServerDisconnectedError, ClientHttpProxyError, ClientProxyConnectionError, ClientOSError

from business.twitter_scraper import TweetScraper
from database.proxy_facade import get_proxies, save_a_proxy_test
from tools.logger import logger

"""
Collection of functions to manage testing and allocations of proxy servers
"""





def get_a_proxy_server(max_delay=100):
    # return {'ip': '03.9.34.151', 'port': '3128'}
    # return {'ip': '51.158.119.88', 'port': '8761'}
    return {'ip': '212.129.3.201', 'port': '5836'}


def test_proxies(only_blacklisted=False, processes=150):
    logger.info('=' * 100)
    logger.info(f"Start testing {'blacklisted' if only_blacklisted else 'all'} proxy servers")
    logger.info('=' * 100)
    proxies = get_proxies(only_blacklisted)
    proxy_list = []
    for _, row in proxies.iterrows():
        proxy = (row['ip'], row['port'])
        proxy_list.append(proxy)
    with mp.Pool(processes=processes) as pool:
        results = pool.starmap(_test_and_save_proxy, proxy_list)


def _test_and_save_proxy(ip, port):
    logger.info('-' * 100)
    logger.info(f'Start testing proxy server: {ip}:{port}')
    logger.info('-' * 100)
    info = f'{ip}_{port}'
    delay = -1
    blacklisted = True
    error_code = -1
    # Try and time fetching the last 40 tweets from Donald Trump
    username = 'realDonaldTrump'
    limit = 40
    ts = TweetScraper(username)
    ts.proxy_server = {'ip': ip, 'port': port}
    ts.twint_limit = limit
    ts.twint_hide_terminal_output = True

    try:
        start_time = time.time()
        ts.execute_scraping()
        delay = time.time() - start_time
        blacklisted, error_code = False, 0
    except ValueError as e:
        delay, blacklisted, error_code = 0, True, 1
        logger.error(f'Error ValueERROR Server: {ip}:{port}')
        logger.error(f'Error message: {e}')
    except (TimeoutError, CancelledError) as e:
        delay, blacklisted, error_code = 0, True, 2
        logger.error(f'Error: asyncio Server:{ip}:{port}')
        logger.error(f'Error message: {e}')
    except (ServerDisconnectedError, ClientHttpProxyError, ClientProxyConnectionError, ClientOSError) as e:
        delay, blacklisted, error_code = 0, True, 3
        logger.error(f'Error aiohttp Server: {ip}:{port}')
        logger.error(f'Error message: {e}')
    except:
        delay, blacklisted, error_code = 0, True, 9
        raise
    finally:
        proxy_test = {'ip': ip, 'port': port,
                      'delay': delay,
                      'blacklisted': blacklisted, 'error_code': error_code}
        save_a_proxy_test(proxy_test)


if __name__ == '__main__':
    # print(get_a_proxy_server())
    # _test_and_save_proxy('195.88.126.54', '48846')
    test_proxies()
