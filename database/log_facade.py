# --------------------------------------------------------------------------------------------------------
# 2020/07/20
# src - log_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime
from database.config_facade import SystemCfg, Scraping_cfg
from database.log_queries import q_save_log, q_get_max_sesion_id, q_get_failed_periods_logs
import pandas as pd

"""
Group of functions to store and retrieve log data via one or more queries from one or more collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------

"""
system_cfg = SystemCfg()
scraping_cfg = Scraping_cfg()


def log_scraping_profile(session_id, flag, category, username, **kwargs):
    log = {'session_id': session_id,
           'task': 'profile',
           'category': category,
           'username': username,
           'flag': flag,
           'timestamp': datetime.now()}
    log.update(kwargs)
    q_save_log(log)


def log_scraping_tweets(session_id, flag, category, username, begin_date, end_date, **kwargs):
    log = {'session_id': session_id,
           'task': 'tweets',
           'category': category,
           'username': username,
           'session_begin_date': datetime.combine(begin_date, datetime.min.time()),
           'session_end_date': datetime.combine(end_date, datetime.min.time()),
           'flag': flag,
           'timestamp': datetime.now()}
    log.update(kwargs)
    q_save_log(log)


def get_failed_periods(session_id):
    failed_periods_logs = q_get_failed_periods_logs(session_id)
    if failed_periods_logs:
        usernames_df = pd.DataFrame(failed_periods_logs)
        usernames_df['end_date'] = usernames_df['end_date'].dt.date
        usernames_df['begin_date'] = usernames_df['begin_date'].dt.date
        return usernames_df
    else:
        return pd.DataFrame()


def get_max_sesion_id():
    return q_get_max_sesion_id()
