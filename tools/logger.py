# --------------------------------------------------------------------------------------------------------
# 2020/06/12
# Twitter_Scraping - logger.py
# md
# --------------------------------------------------------------------------------------------------------
import logging
import coloredlogs

from database.config_facade import SystemCfg

system_cfg=SystemCfg()

loging_level=system_cfg.logging_level


if loging_level == 'Debug':
    level = logging.DEBUG
elif loging_level == 'Info':
    level = logging.INFO
elif loging_level== 'Warning':
    level = logging.WARNING
elif loging_level== 'Error':
    level = logging.ERROR
else:
    level = logging.NOTSET

# create logger
logger = logging.getLogger('scraper')
logger.setLevel(level)
logger.propagate = False


# create console handler and set level to debug
handler = logging.StreamHandler()
handler.setLevel(level)

# create formatter
format = '%(asctime)s: [%(filename)-22s | %(funcName)22s:%(lineno)-3s]: [%(levelname)-7s]: %(message)s'
formatter = coloredlogs.ColoredFormatter(format)

# add formatter to console handler
handler.setFormatter(formatter)

# add console handler to logger
logger.addHandler(handler)
