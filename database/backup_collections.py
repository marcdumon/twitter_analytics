# --------------------------------------------------------------------------------------------------------
# 2020/07/13
# src - backup_collections.py
# md
# --------------------------------------------------------------------------------------------------------
from datetime import datetime

from pymongo import MongoClient

from tools.logger import logger

collection_names = ['profiles', 'proxies', 'tweets']

source_database = 'twitter_database'
backup_database = f'twitter_database_backup_{datetime.now().date()}'

client = MongoClient()
source_db = client[source_database]
backup_db = client[backup_database]

for collection_name in collection_names:
    source_collection = source_db[collection_name]
    backup_collection = backup_db[collection_name]
    # check if source exists. If so, do nothing
    if collection_name in backup_db.list_collection_names():
        logger.warning(f'{collection_name} already exists in {backup_db}')

    else:
        # Create indices
        for name, index_info in source_collection.index_information().items():
            keys = index_info['key']
            del (index_info['ns'])
            del (index_info['v'])
            del (index_info['key'])
            backup_collection.create_index(keys, name=name, **index_info)
            logger.info(f'Index {name} for {collection_name} created')
        # Copy documents
        i = 0
        for doc in source_collection.find({}):
            backup_collection.insert_one(doc)
            i += 1
            if i % 50000 == 0: logger.info(f'Copied {i} documents from {collection_name} collection')
        logger.info(f'Copied total of {i} documents from {collection_name} collection')


if __name__ == '__main__':
    pass
