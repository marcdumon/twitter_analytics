# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - twitter_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime
import pandas as pd
from config import TEST_USERNAME
from database.tweet_queries import q_get_nr_tweets_per_day
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrief data via one or more queries from one or more collections.
The functions act as a shield to the database queries for higher layers. 
Functions return dataframes when suitable
"""

"""
IMPLEMENTED FUNCTIONS
- get_nr_tweets_per_day(username, begin_date, end_date)

"""


def get_join_date(username):
    pass
    join_date = datetime.strptime('2020-01-03', '%Y-%m-%d')

    return join_date


def get_nr_tweets_per_day(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    nr_tweets_per_day = q_get_nr_tweets_per_day(username, start_date, end_date)
    # dates=['2020-01-01',
    #        '2020-01-04',
    #        '2020-01-10',
    #        '2020-01-11',
    #        '2020-01-12',
    #        '2020-01-20',
    #        '2020-01-30'
    #        ]
    # import numpy as np
    # nr_tweets_per_day=[{'date': datetime.strptime(d, '%Y-%m-%d'), 'nr_tweets': np.random.randint(5)}  for d in dates]


    return pd.DataFrame(nr_tweets_per_day)


if __name__ == '__main__':
    print(get_nr_tweets_per_day(TEST_USERNAME))
