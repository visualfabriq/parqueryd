import os

# set standard data directory
DEFAULT_DATA_DIR = os.environ.get('PARQUERYD_DATA_DIR', '/srv/parquet/')
if not os.path.exists(DEFAULT_DATA_DIR):
    try:
        os.makedirs(DEFAULT_DATA_DIR)
    except OSError:
        pass

INCOMING = os.path.join(DEFAULT_DATA_DIR, 'incoming')
if not os.path.exists(INCOMING):
    try:
        os.makedirs(INCOMING)
    except OSError:
        pass

# set the redis standards
REDIS_SET_KEY = 'parqueryd_controllers'
REDIS_TICKET_KEY_PREFIX = 'parqueryd_download_ticket_'
REDIS_DOWNLOAD_LOCK_PREFIX = 'parqueryd_download_lock_'
REDIS_DOWNLOAD_LOCK_DURATION = 60 * 30  # time in seconds to keep a lock
