# --------------------------------------------------------------------------------------------------------
# 2020/07/09
# src - main.py
# md
# --------------------------------------------------------------------------------------------------------

from business.scraping_controller import scrape_users_tweets_profile, reset_proxy_servers, reset_scrape_flag, scrape_proxies

# Todo: use arguments to control parameters
# from config import SCRAPE_PROFILES, SCRAPE_TWEETS
from database.control_facade import Scraping_cfg

scraping_cfg = Scraping_cfg

reset_proxies = True
reset_flags = True
resume = not reset_flags
processes = 30
max_delay = 20

if reset_proxies:
    reset_proxy_servers()
if reset_flags:
    reset_scrape_flag()
if scraping_cfg.proxies:
    scrape_proxies()
if scraping_cfg.tweets or scraping_cfg.profiles:
    scrape_users_tweets_profile(resume=resume, processes=processes, max_delay=max_delay)
