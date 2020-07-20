# --------------------------------------------------------------------------------------------------------
# 2020/07/20
# src - log_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime
from database.config_facade import SystemCfg, Scraping_cfg
from database.log_queries import q_save_log, q_get_max_sesion_id
"""
Group of functions to store and retrieve log data via one or more queries from one or more collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------

"""
system_cfg = SystemCfg()
scraping_cfg = Scraping_cfg()

def log_scraping_profile(session_id,flag, category, username, **kwargs):
    log = {'session_id': session_id,
           'task': 'profile',
           'category': category,
           'username': username,
           'flag': flag,
           'database': system_cfg.database,
           'timestamp': datetime.now()}
    log.update(kwargs)
    q_save_log(log)


def log_scraping_tweets(session_id,flag, category, username, begin_date, end_date, **kwargs):
    log = {'session_id': session_id,
           'task': 'tweets',
           'category': category,
           'username': username,
           'begin_date': datetime.combine(begin_date, datetime.min.time()),
           'end_date': datetime.combine(end_date, datetime.min.time()),
           'flag': flag,
           'database': system_cfg.database,
           'timestamp': datetime.now()}
    log.update(kwargs)
    q_save_log(log)


def get_max_sesion_id():
    return q_get_max_sesion_id()

