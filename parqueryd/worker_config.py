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