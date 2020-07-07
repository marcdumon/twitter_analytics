# --------------------------------------------------------------------------------------------------------
# 2020/07/06
# src - scraping_controller.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime, date

from business.twitter_scraper import TweetScraper
from config import USERS_LIST, SCRAPE_WITH_PROXY, END_DATE, BEGIN_DATE, SCRAPE_ONLY_MISSING_DATES
from database.twitter_facade import get_join_date, get_dates_without_tweets
from tools.logger import logger


def scrape_all_users():
    logger.info('=' * 150)
    logger.info('Start scrapping Twitter')
    logger.info('=' * 150)
    for username in USERS_LIST['xxx']:
        scrape_periods = create_scrape_dates(username)
        for (s, e) in scrape_periods:
            scrape_user_tweets(username, s, e)


def create_scrape_dates(username):
    # no need to scrape before join_date
    join_date = get_join_date(username)
    b, e = max(BEGIN_DATE, join_date), min(END_DATE, str(date.today()))
    missing_dates = []
    if SCRAPE_ONLY_MISSING_DATES:
        missing_dates = get_dates_without_tweets(username, begin_date=b, end_date=e)




    scrape_dates = [(b, e)]
    return scrape_dates


def scrape_user_tweets(username, begin_date='2000-01-01', end_date='2035-01-01'):
    use_proxy = SCRAPE_WITH_PROXY

    logger.info('-' * 150)
    logger.info(f'Start scraping Twitter for user: {username}')
    logger.info('-' * 150)
    tweet_scraper = TweetScraper(username, begin_date, end_date)
    tweet_scraper.execute_scraping()


if __name__ == '__main__':
    pass
    # scrape_user_tweets('daemsgreet')
    scrape_all_users()
