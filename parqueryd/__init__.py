import os
import logging

pre_release_version = os.getenv('PRE_RELEASE_VERSION', '')
__version__ = pre_release_version if pre_release_version else '1.1.0'

# initalize logger
logger = logging.getLogger('parqueryd')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s %(asctime)s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

# import helpers
