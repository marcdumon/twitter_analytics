# --------------------------------------------------------------------------------------------------------
# 2020/07/09
# src - main.py
# md
# --------------------------------------------------------------------------------------------------------

from business.scraping_controller import reset_proxy_servers, reset_scrape_flag, scrape_proxies, TwitterScrapingSession

scrape = TwitterScrapingSession()
# _ = start_scraping.users_list(['FRanckentheo', 'xsdsdaads', 'smienos'], only_new=False).start_scraping
# _ = scrape.profiles.tweets.all_users.start_scraping()
# _ = scrape.tweets.all_users.start_scraping()
# _ = scrape.profiles.tweets.rescrape_failed_periods(1).start_scraping()
_ = scrape.profiles.tweets.all_users.start_scraping()
