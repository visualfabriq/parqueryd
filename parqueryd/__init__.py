import logging
import os
from version import __version__


logger = logging.getLogger('parqueryd')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s %(asctime)s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

DEFAULT_DATA_DIR = '/srv/parquet/'
INCOMING = os.path.join(DEFAULT_DATA_DIR, 'incoming')
if not os.path.exists(INCOMING):
    os.makedirs(INCOMING)

REDIS_SET_KEY = 'parqueryd_controllers'
REDIS_TICKET_KEY_PREFIX = 'parqueryd_download_ticket_'
REDIS_DOWNLOAD_LOCK_PREFIX = 'parqueryd_download_lock_'
REDIS_DOWNLOAD_LOCK_DURATION = 60 * 30  # time in seconds to keep a lock

from rpc import RPC, RPCError
from controller import ControllerNode
from worker import WorkerNode, DownloaderNode, MoveparquetNode
