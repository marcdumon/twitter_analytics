# --------------------------------------------------------------------------------------------------------
# 2020/07/06
# src - scraping_controller.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime, date, timedelta
import pandas as pd
from business.twitter_scraper import TweetScraper
from config import USERS_LIST, SCRAPE_WITH_PROXY, END_DATE, BEGIN_DATE, SCRAPE_ONLY_MISSING_DATES, TEST_USERNAME, TIME_DELTA
from database.twitter_facade import get_join_date, get_nr_tweets_per_day, save_tweets
from tools.logger import logger


def scrape_all_users():
    logger.info('=' * 150)
    logger.info('Start scrapping Twitter')
    logger.info('=' * 150)
    for username in USERS_LIST['xxx']:
        logger.info('-' * 150)
        logger.info(f'Start scraping Twitter for user: {username}')
        logger.info('-' * 150)
        scrape_periods = calculate_scrape_periods(username)
        for (b, e) in scrape_periods:
            tweets_df = scrape_user_tweets(username, b, e)
            if not tweets_df.empty:
                logger.info(f'Saving {len(tweets_df)} tweets')
                save_tweets(tweets_df)


def scrape_user_tweets(username, begin_date=datetime(2000, 1, 1), end_date=datetime(2035, 1, 1)):
    use_proxy = SCRAPE_WITH_PROXY
    logger.info(f'Start scraping tweetsfor user: {username} starting on {begin_date} and ending on {end_date}')
    tweet_scraper = TweetScraper(username, begin_date, end_date)
    tweets_df = tweet_scraper.execute_scraping()
    return tweets_df


def calculate_scrape_periods(username):
    # no need to scrape before join_date
    join_date = get_join_date(username)
    begin_date, end_date = max(BEGIN_DATE, join_date), min(END_DATE, datetime.today())
    # no need to scrape same dates again
    if SCRAPE_ONLY_MISSING_DATES:
        scrape_periods = get_periods_without_min_tweets(username, begin_date=begin_date, end_date=end_date, min_tweets=1)
    else:
        scrape_periods = [(begin_date, end_date)]
    scrape_periods = split_periods(scrape_periods)
    return scrape_periods


def get_periods_without_min_tweets(username, begin_date, end_date, min_tweets=1):
    """
    Gets all the dates when 'username' has 'min_tweets' nr of tweets stored in the database.
    Returns a list of tuples with begin and end date of the period when the 'username' has less or equal amounts of tweets as 'min_tweets' stored in the database.
    We split the periods that are longer than TIME_DELTA.
    """
    # A bit hacky !!! but it works
    days_with_tweets = get_nr_tweets_per_day(username, begin_date, end_date)
    days_with_tweets = days_with_tweets[days_with_tweets['nr_tweets'] >= min_tweets]
    days_with_tweets = [d.to_pydatetime() for d in days_with_tweets['date'] if begin_date < d.to_pydatetime() < end_date]
    # add begin_date at the beginning en end_date + 1 day at the end
    days_with_tweets.insert(0, begin_date - timedelta(days=1))  # begin_date - 1 day because we'll add a day when creating the dateranges
    days_with_tweets.append((end_date + timedelta(days=1)))

    # Create the periods without min_tweets amount of saved tweets
    missing_tweets_periods = [(b + timedelta(days=1), e - timedelta(days=1))
                              for b, e in zip(days_with_tweets[:-1], days_with_tweets[1:])  # construct the periods
                              if e - b > timedelta(days=1)]
    return missing_tweets_periods


def split_periods(periods):
    # Split the periods into parts with a maximal length of 'TIME_DELTA' days
    splitted_periods = []
    for b, e in periods:
        if e - b <= timedelta(days=TIME_DELTA):
            splitted_periods.append((b, e))
        else:
            while e - b >= timedelta(days=TIME_DELTA):
                splitted_periods.append((b, b + timedelta(days=TIME_DELTA - 1)))
                b = b + timedelta(days=TIME_DELTA)
                # The last part of the splitting
                if e - b < timedelta(TIME_DELTA):
                    splitted_periods.append((b, e))
    return splitted_periods


if __name__ == '__main__':
    pass
    u = TEST_USERNAME
    # scrape_user_tweets(u)
    scrape_all_users()
    # calculate_scrape_periods(TEST_USERNAME)
    # BEGIN_DATE = datetime(2019,9,29)

    # get_periods_without_min_tweets(TEST_USERNAME, BEGIN_DATE, END_DATE)
