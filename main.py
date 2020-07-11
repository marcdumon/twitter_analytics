# --------------------------------------------------------------------------------------------------------
# 2020/07/09
# src - main.py
# md
# --------------------------------------------------------------------------------------------------------

from business.scraping_controller import scrape_proxies, reset_users_proxies, scrape_users_tweets

# Todo: use arguments to control parameters

reset = False
resume = True
processes = 10
max_delay = 15
if __name__ == '__main__':
    if reset: reset_users_proxies()
    scrape_users_tweets(resume=False, processes=processes, max_delay=max_delay)
