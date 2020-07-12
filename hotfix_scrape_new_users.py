# --------------------------------------------------------------------------------------------------------
# 2020/07/12
# src - hotfix_scrape_new_users.py
# md
# --------------------------------------------------------------------------------------------------------
from business.hotfix_scraping_new_users import scrape_new_users_tweets_profile,manualy_check_already_exists

if __name__ == '__main__':
    manualy_check_already_exists()
    scrape_new_users_tweets_profile(is_tweet=True)
