import os

# set standard data directory
DEFAULT_DATA_DIR = '/srv/parquet/'
if not os.path.exists(DEFAULT_DATA_DIR):
    os.makedirs(DEFAULT_DATA_DIR)
INCOMING = os.path.join(DEFAULT_DATA_DIR, 'incoming')
if not os.path.exists(INCOMING):
    os.makedirs(INCOMING)

# set the redis standards
REDIS_SET_KEY = 'parqueryd_controllers'
REDIS_TICKET_KEY_PREFIX = 'parqueryd_download_ticket_'
REDIS_DOWNLOAD_LOCK_PREFIX = 'parqueryd_download_lock_'
REDIS_DOWNLOAD_LOCK_DURATION = 60 * 30  # time in seconds to keep a lock
