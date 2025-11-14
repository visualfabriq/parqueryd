from __future__ import annotations

import logging
import os

pre_release_version: str = os.getenv("PRE_RELEASE_VERSION", "")
__version__: str = pre_release_version if pre_release_version else "2.0.1"

# initalize logger
logger: logging.Logger = logging.getLogger("parqueryd")
ch: logging.StreamHandler = logging.StreamHandler()
formatter: logging.Formatter = logging.Formatter(
    "%(levelname)s %(asctime)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
ch.setFormatter(formatter)
logger.addHandler(ch)

# import helpers
