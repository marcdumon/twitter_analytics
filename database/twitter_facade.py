# --------------------------------------------------------------------------------------------------------
# 2020/07/04
# src - twitter_facade.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime
from pprint import pprint

import pandas as pd
from config import TEST_USERNAME
from database.profile_queries import q_get_profile
from database.tweet_queries import q_get_nr_tweets_per_day, q_save_a_tweet, q_save_many
from tools.utils import set_pandas_display_options

set_pandas_display_options()
"""
Group of functions to store and retrief data via one or more queries from one or more collections.
The functions act as a shield to the database queries for higher layers. 
Functions accept return dataframes when suitable
"""

"""
IMPLEMENTED FUNCTIONS
- get_join_date(username)
- get_nr_tweets_per_day(username, begin_date, end_date)
- save_tweets(tweets_df
"""


def save_tweets(tweets_df):
    def modify_tweets_df(tweets_df):
        tweets_df = tweets_df.rename(columns={'id': 'tweet_id'})
        # Make user_id string
        tweets_df['user_id'] = tweets_df['user_id'].apply(str)
        # Make all usernamers lowercase
        tweets_df['username'] = tweets_df['username'].str.lower()

        def f(lst):  # [{'user_id': '11767', 'username': 'xxx'}, ... ]
            for dct in lst:
                dct['username'] = dct['username'].lower()

        tweets_df['reply_to'].apply(lambda x: f(x))
        # Add is_reply
        tweets_df['is_reply'] = tweets_df['tweet_id'] != tweets_df['conversation_id']
        # Add datetime columns
        tweets_df['datetime'] = pd.to_datetime(tweets_df['date'], format='%Y-%m-%d %H:%M:%S')
        tweets_df['date'] = tweets_df['datetime'].dt.date.apply(str)
        tweets_df['time'] = tweets_df['datetime'].dt.time.apply(str)
        return tweets_df

    def reorder_tweets_df_columns(tweets_df):
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
        return tweets_df[columns]

    tweets_df = modify_tweets_df(tweets_df)
    tweets_df = reorder_tweets_df_columns(tweets_df)



    for _, row in tweets_df.iterrows():
        q_save_a_tweet(row.to_dict())


def get_join_date(username):
    profile = q_get_profile(username)
    join_date = datetime.strptime(profile['join_date'], '%Y-%m-%d')
    return join_date


def get_nr_tweets_per_day(username, start_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    nr_tweets_per_day = q_get_nr_tweets_per_day(username, start_date, end_date)
    return pd.DataFrame(nr_tweets_per_day)


if __name__ == '__main__':
    u = TEST_USERNAME
    # print(get_nr_tweets_per_day(u))
    # get_join_date((u))
