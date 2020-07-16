# --------------------------------------------------------------------------------------------------------
# 2020/07/09
# src - main.py
# md
# --------------------------------------------------------------------------------------------------------

from business.scraping_controller import scrape_users_tweets_profile, reset_proxy_servers, reset_scrape_flag

# Todo: use arguments to control parameters
# from config import SCRAPE_PROFILES, SCRAPE_TWEETS

reset_proxies = True
reset_flags = True
resume = not reset_flags
processes = 30
max_delay = 20

if __name__ == '__main__':
    pass
    if reset_proxies: reset_proxy_servers()
    if reset_flags: reset_scrape_flag()
    scrape_users_tweets_profile(resume=resume, processes=processes, max_delay=max_delay)
