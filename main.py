# --------------------------------------------------------------------------------------------------------
# 2020/07/09
# src - main.py
# md
# --------------------------------------------------------------------------------------------------------

from business.scraping_controller import reset_users_proxies, scrape_users_tweets_profile

# Todo: use arguments to control parameters

reset = True
resume = True
processes = 25
max_delay = 20


if __name__ == '__main__':
    if reset: reset_users_proxies()
    scrape_users_tweets_profile(resume=resume, processes=processes, max_delay=max_delay)
