# --------------------------------------------------------------------------------------------------------
# 2020/07/06
# src - scraping_controller.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime, date, timedelta
import pandas as pd
from business.twitter_scraper import TweetScraper
from config import USERS_LIST, SCRAPE_WITH_PROXY, END_DATE, BEGIN_DATE, SCRAPE_ONLY_MISSING_DATES, TEST_USERNAME, TIME_DELTA
from database.twitter_facade import get_join_date, get_nr_tweets_per_day
from tools.logger import logger


def scrape_all_users():
    logger.info('=' * 150)
    logger.info('Start scrapping Twitter')
    logger.info('=' * 150)
    for username in USERS_LIST['xxx']:
        scrape_periods = calculate_scrape_dates(username)
        for (s, e) in scrape_periods:
            scrape_user_tweets(username, s, e)


def calculate_scrape_dates(username):
    # no need to scrape before join_date
    join_date = get_join_date(username)

    begin_date, end_date = max(BEGIN_DATE, join_date), min(END_DATE, datetime.today())
    # SCRAPE_ONLY_MISSING_DATES =False
    if SCRAPE_ONLY_MISSING_DATES:
        missing_dates = get_periods_without_min_tweets(username, begin_date=begin_date, end_date=end_date, min=1)
        print(missing_dates)
        1/0



        missing_dates.insert(0, begin_date) if begin_date < missing_dates[0] else missing_dates
        missing_dates.append(end_date) if end_date > missing_dates[-1] else missing_dates

    scrape_dates = [[begin_date, end_date]]
    return scrape_dates


def scrape_user_tweets(username, begin_date='2000-01-01', end_date='2035-01-01'):
    use_proxy = SCRAPE_WITH_PROXY

    logger.info('-' * 150)
    logger.info(f'Start scraping Twitter for user: {username}')
    logger.info('-' * 150)
    tweet_scraper = TweetScraper(username, begin_date, end_date)
    tweet_scraper.execute_scraping()


def get_periods_without_min_tweets(username, begin_date, end_date, min=1):
    # Very hacky !!! but it works
    days_with_tweets = get_nr_tweets_per_day(username, begin_date, end_date)
    days_with_tweets = days_with_tweets[days_with_tweets['nr_tweets'] >= min]
    days_with_tweets = [d.to_pydatetime() for d in days_with_tweets['date'] if begin_date < d.to_pydatetime() < end_date]
    # add begin_date at the beginning en end_date + 1 day at the end
    days_with_tweets.insert(0, begin_date - timedelta(days=1))  # begin_date - 1 day because we'll add a day when creating the dateranges
    days_with_tweets.append((end_date + timedelta(days=1)))

    # Create the periods without min amount of saved tweets
    missing_periods = [(b + timedelta(days=1), e - timedelta(days=1)) for b, e in zip(days_with_tweets[:-1], days_with_tweets[1:]) if e - b > timedelta(days=1)]

    # Split the periods to maximal length of TIME_DELTA days
    missing_dates = []
    for b, e in missing_periods:
        if e - b <= timedelta(days=TIME_DELTA):
            missing_dates.append((b, e))
        else:
            while e - b >= timedelta(days=TIME_DELTA):
                missing_dates.append((b, b + timedelta(days=TIME_DELTA - 1)))
                b = b + timedelta(days=TIME_DELTA)
                if e - b < timedelta(TIME_DELTA):
                    missing_dates.append((b, e))
    return missing_dates


if __name__ == '__main__':
    pass
    u = 'daemsgreet'
    # scrape_user_tweets(u)
    # scrape_all_users(u)
    calculate_scrape_dates(TEST_USERNAME)
    # BEGIN_DATE = datetime(2019,9,29)

    # get_periods_without_min_tweets(TEST_USERNAME, BEGIN_DATE, END_DATE)
