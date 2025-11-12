from __future__ import annotations

import contextlib
import os

# set standard data directory
DEFAULT_DATA_DIR: str = os.environ.get("PARQUERYD_DATA_DIR", "/srv/parquet/")
if not os.path.exists(DEFAULT_DATA_DIR):
    with contextlib.suppress(OSError):
        os.makedirs(DEFAULT_DATA_DIR)

INCOMING: str = os.path.join(DEFAULT_DATA_DIR, "incoming")
if not os.path.exists(INCOMING):
    with contextlib.suppress(OSError):
        os.makedirs(INCOMING)

# set the redis standards
REDIS_SET_KEY: str = "parqueryd_controllers"
REDIS_TICKET_KEY_PREFIX: str = "parqueryd_download_ticket_"
REDIS_DOWNLOAD_LOCK_PREFIX: str = "parqueryd_download_lock_"
REDIS_DOWNLOAD_LOCK_DURATION: int = 60 * 30  # time in seconds to keep a lock
