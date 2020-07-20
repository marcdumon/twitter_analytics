# --------------------------------------------------------------------------------------------------------
# 2020/07/18
# src - config_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime, timedelta

"""
Config: Reads the configuration
Control: Sets the configuration
"""
# Todo: Rethink architecture config and control
conf_all = {
    'n_processes': 25,
    # Twitter
    'scrape_profiles': True,  # Todo: obsolete?
    'scrape_tweets': True,  # Todo: obsolete?
    'scrape_with_proxy': True,  # Todo: obsolete?
    'session_end_date': datetime.today().date(),
    'session_begin_date': (datetime.now() - timedelta(days=20)).date(),
    'time_delta': 10,
    'max_fails': 10,
    'max_proxy_delay': 30,
    'scrape_only_missing_dates': False,
    'min_tweets': 1,
    # Proxies
    'scrape_proxies': True,
    'proxies_download_sites': {'free_proxy_list': False, 'hide_my_name': False},

    # System
    'database': 'twitter_database_xxx',
    'logging_level': 'Info',
    # 'logging_level': 'Debug',

}
conf = conf_all


class _Config:

    def __init__(self):
        conf['twint_show_stats'] = True if conf['logging_level'] != 'Debug' else False
        self._config = conf

    def get_property(self, property_name):
        if property_name not in self._config.keys():
            return None
        return self._config[property_name]


class Scraping_cfg(_Config):
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    @property
    def proxies(self):
        return self.get_property('scrape_proxies')

    @property
    def proxies_download_sites(self):
        return self.get_property('proxies_download_sites')

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @property
    def profiles(self):
        return self.get_property('scrape_profiles')

    @property
    def tweets(self):
        return self.get_property('scrape_tweets')

    @property
    def missing_dates(self):
        return self.get_property('scrape_only_missing_dates')

    @property
    def max_proxy_delay(self):
        return self.get_property('max_proxy_delay')

    def min_tweets(selfs):
        return selfs.get_property('min_tweets')

    @property
    def session_begin_date(self):
        return self.get_property('session_begin_date')

    @property
    def session_end_date(self):
        return self.get_property('session_end_date')

    @property
    def time_delta(self):
        return self.get_property('time_delta')

    @property
    def max_fails(self):
        return self.get_property('max_fails')

    @property
    def n_processes(self):
        return self.get_property('n_processes')

    @property
    def session_id(self):
        return self.get_property('session_id')
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


class SystemCfg(_Config):

    @property
    def database(self):
        return self.get_property('database')

    @property
    def logging_level(self):
        return self.get_property('logging_level')


if __name__ == '__main__':
    s_cfg = Scraping_cfg()
    x = s_cfg.session_end_date
    print(x)

    xx = SystemCfg()
    print(xx.logging_level)
