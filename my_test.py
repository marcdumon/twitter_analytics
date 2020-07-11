# --------------------------------------------------------------------------------------------------------
# 2020/07/09
# src - my_test.py
# md
# --------------------------------------------------------------------------------------------------------
from business.proxy_manager import test_proxies
from business.scraping_controller import scrape_proxies

# scrape_proxies()
test_proxies(only_blacklisted=False, processes=8)
# test_proxies(only_blacklisted=True, processes=15)
# test_proxies(only_blacklisted=True, processes=5)




if __name__ == '__main__':
    pass
