# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - twitter_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime
from pprint import pprint

import pandas as pd
from config import TEST_USERNAME
from database.profile_queries import q_get_a_profile, q_save_a_profile, q_get_profiles, q_set_a_scrape_flag
from database.tweet_queries import q_get_nr_tweets_per_day, q_save_a_tweet, q_update_a_tweet
from tools.logger import logger
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrieve twitter data via one or more queries from one or more collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept and return dataframes when suitable

IMPLEMENTED FUNCTIONS
---------------------
- get_join_date(username)
- get_profiles()
- get_nr_tweets_per_day(username, begin_date, end_date)
- reset_all_scrape_flags()
- save_profiles(profiles_df)
- save_tweets(tweets_df)
- set_a_scrape_flag(username, flag)
"""


def save_tweets(tweets_df, update=True):
    # Update necessary to have correct likes, replies, etc
    def _format_tweets_df(df):
        df = df.rename(columns={'id': 'tweet_id'})
        # Make user_id string
        df['user_id'] = df['user_id'].apply(str)
        # Make all usernamers lowercase
        df['username'] = df['username'].str.lower()

        def f(lst):  # [{'user_id': '11767', 'username': 'xxx'}, ... ]
            for dct in lst:
                dct['username'] = dct['username'].lower()

        df['reply_to'].apply(lambda x: f(x))
        # Add is_reply
        df['is_reply'] = df['tweet_id'] != df['conversation_id']
        # Add datetime columns
        df['datetime'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S')
        df['date'] = df['datetime'].dt.date.apply(str)
        df['time'] = df['datetime'].dt.time.apply(str)
        return df

    def _reorder_tweets_df_columns(df):
        # Reorder the columns, to have the fields in Mongodb in the right order
        columns = ['tweet_id',
                   'conversation_id',
                   'user_id',
                   'username',
                   'name',
                   'created_at',
                   'datetime',
                   'date',
                   'time',
                   'timezone',
                   'day',
                   'hour',

                   'tweet',
                   'hashtags',
                   'cashtags',
                   'reply_to',
                   'is_reply',
                   'quote_url',
                   'link',

                   'retweet',
                   'nlikes',
                   'nreplies',
                   'nretweets',

                   'search',
                   'source',
                   'near',
                   'geo',
                   'place',

                   'user_rt_id',
                   'user_rt',
                   'retweet_id',

                   'retweet_date',
                   'translate',
                   'trans_src',
                   'trans_dest',
                   ]
        return df[columns]

    tweets_df = _format_tweets_df(tweets_df)
    tweets_df = _reorder_tweets_df_columns(tweets_df)

    for _, row in tweets_df.iterrows():
        q_update_a_tweet(row.to_dict()) if update else q_save_a_tweet(row.to_dict())


def save_profiles(profiles_df):
    def _format_profile_df(profile):
        profile['username'] = profile['username'].lower()
        profile['join_datetime'] = pd.to_datetime(profile['join_datetime'])  # dd-mm-yyyy h:mm AM -> hh:mm:ss
        profile['join_date'] = pd.to_datetime(f"{profile['join_date']}").strftime('%Y-%m-%d')  # dd-mm-yyyy -> yyyy-mm-dd
        profile['join_time'] = pd.to_datetime(f"{profile['join_time']}").strftime('%H:%M:%S')  # h:mm AM -> hh:mm:ss+
        profile['private'] = bool(profile['private'])
        profile['verified'] = bool(profile['verified'])
        return profile

    # Only 1 profile in profile, .copy()  otherwise "A value is trying to be set on a copy of a slice from a DataFrame" errror
    profile = profiles_df.iloc[0].copy()
    profile = _format_profile_df(profile)
    q_save_a_profile(profile)


def get_profiles():
    profiles = q_get_profiles()
    profiles_df = pd.DataFrame(profiles)
    return profiles_df


def get_usernames():
    profiles_df = get_profiles()
    usernames_df = profiles_df[['user_id', 'username', 'scrape_flag']]
    return usernames_df


def get_join_date(username):
    profile = q_get_a_profile(username)
    try:
        join_date = profile['join_date']
    except TypeError as e:
        logger.error(f'Couldn\'t get the join_date for user {username}')
        logger.error(f'Error message:{e}')
        logger.error(f'Returning join_date = 1900-01-01')
        join_date = '1900-01-01'
    join_date = datetime.strptime(join_date, '%Y-%m-%d')
    return join_date


def get_nr_tweets_per_day(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    nr_tweets_per_day = q_get_nr_tweets_per_day(username, start_date, end_date)
    return pd.DataFrame(nr_tweets_per_day)


def reset_all_scrape_flags():
    for profile in q_get_profiles():
        q_set_a_scrape_flag(profile['username'], 0)


def set_a_scrape_flag(username, flag):
    q_set_a_scrape_flag(username, flag)


if __name__ == '__main__':
    from datetime import datetime

    u = 'elsampe'
    sd = datetime(2011, 12, 31)
    ed = datetime(2012, 1, 1)

    print(get_nr_tweets_per_day(u, sd, ed))
    # get_join_date((u))
    # reset_all_scrape_flags()
    # set_a_scrape_flag('smienos', 10)
    # get_profiles()
