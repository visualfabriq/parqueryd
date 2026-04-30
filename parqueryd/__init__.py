from __future__ import annotations

import logging
from importlib.metadata import version as _pkg_version

__version__: str = _pkg_version("parqueryd")

# initalize logger
logger: logging.Logger = logging.getLogger("parqueryd")
ch: logging.StreamHandler = logging.StreamHandler()
formatter: logging.Formatter = logging.Formatter(
    "%(levelname)s %(asctime)s %(name)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
ch.setFormatter(formatter)
logger.addHandler(ch)

# import helpers
