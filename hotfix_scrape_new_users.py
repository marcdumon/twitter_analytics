# --------------------------------------------------------------------------------------------------------
# 2020/07/12
# src - hotfix_scrape_new_users.py
# md
# --------------------------------------------------------------------------------------------------------

from business.scraping_controller import scrape_new_users_tweets, manualy_check_already_exists

if __name__ == '__main__':
    usernames = ['vanranstmarc']
    manualy_check_already_exists(usernames)


    scrape_new_users_tweets(processes=len(usernames) + 1, max_delay=30, resume=False, usernames=usernames)
