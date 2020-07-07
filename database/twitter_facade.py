# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - twitter_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime


def get_join_date(username):
    pass
    join_date = '2020-01-03'
    return join_date


def get_dates_without_tweets(username, begin_date, end_date):
    # See: https://stackoverflow.com/questions/2315032/how-do-i-find-missing-dates-in-a-list-of-sorted-dates
    return [datetime.strptime(d, '%Y-%m-%d') for d in ['2020-06-24', '2020-06-21', '2020-06-25']]
