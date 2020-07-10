# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - proxy_scraper.py
# md
# --------------------------------------------------------------------------------------------------------
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from tools.utils import set_pandas_display_options

set_pandas_display_options()


class ProxyScraper:
    """
    Class to scrape proxy servers from different websites and send that data to the the scraping_controller for further handeling.
    Currenty implemented websites are:
        - https://free-proxy-list.net/
        - https://hidemy.name/en/proxy-list/
    """

    def __init__(self):
        # Todo: Generalise to more proxy websites.
        #       scrape_free_proxy_list uses request and is very fast
        #       scrape_hide_my_name uses selenium
        pass

    @staticmethod
    def scrape_free_proxy_list():
        proxy_url = 'https://free-proxy-list.net/'
        # logger.debug(f'Start downloading proxy servers from {proxy_url}')
        response = requests.get(proxy_url)
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find('table', id='proxylisttable')
        list_tr = table.find_all('tr')
        list_td = [elem.find_all('td') for elem in list_tr]
        list_td = list(filter(None, list_td))
        proxies_df = pd.DataFrame()
        proxies_df['ip'] = [elem[0].text for elem in list_td]
        proxies_df['port'] = [elem[1].text for elem in list_td]
        proxies_df['source'] = 'free-proxy-list.net'
        proxies_df['datetime'] = datetime.now()
        proxies_df = proxies_df[['datetime', 'ip', 'port', 'source']]
        return proxies_df

    @staticmethod
    def scrape_hide_my_name():
        # All countries except AUstralia
        proxy_url = 'https://hidemy.name/en/proxy-list/?country=AFALARAMATAZBDBEBJBOBWBRBGBFKHCMCACLCNCOCGCDCRHRCYCZDJECEGFIFRGEDEGHGRGTHNHKHUINIDIRIQIEILITJ' \
                    'PKZKEKRKGLVLBLTLUMKMWMYMVMXMDMNMEMZNPNLNZNINGNOPKPSPAPYPEPHPLPTPRRORUSARSSLSGSKSISOZAESCHTWTHTRUGUAAEGBUSUYUZVEVN&maxtime=1000&type=hs&start='
        pages = 3
        options = Options()
        options.headless = True
        driver = webdriver.Firefox(options=options, service_log_path='/dev/null')
        proxies_df = pd.DataFrame()
        for start in range(0, 64 * (pages - 1) + 1, 64):
            # logger.debug(f'Downloading proxy servers from https://hidemy.name/ starting at {start}')
            driver.get(f'{proxy_url}{start}')
            time.sleep(10)
            table = driver.find_element_by_class_name('table_block')
            body = table.find_element_by_tag_name('tbody')
            for row in body.find_elements_by_tag_name('tr'):
                r = row.find_elements_by_tag_name('td')
                proxies_df = proxies_df.append({'ip': r[0].text,  # Todo: Use list iso DF. Now dict->df->dict ?
                                                'port': r[1].text,
                                                'source': 'hidemy.name',
                                                'datetime': datetime.now()},
                                               ignore_index=True)
                proxies_df = proxies_df[['datetime', 'ip', 'port', 'source']]
        driver.close()

        return proxies_df


if __name__ == '__main__':
    ps = ProxyScraper()
    ps.scrape_free_proxy_list()
    # ps.scrape_hide_my_name()
