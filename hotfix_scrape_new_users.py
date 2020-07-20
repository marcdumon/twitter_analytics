# --------------------------------------------------------------------------------------------------------
# 2020/07/12
# src - hotfix_scrape_new_users.py
# md
# --------------------------------------------------------------------------------------------------------

from business.scraping_controller import  manualy_check_already_exists, scrape_users_tweets_profile

if __name__ == '__main__':
    usernames = ['vanranstmarc']
    manualy_check_already_exists(usernames)


    scrape_users_tweets_profile(max_delay=30, resume=False, usernames=usernames)
