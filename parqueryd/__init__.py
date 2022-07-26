import os
import logging
# from version import __version__

pre_release_version = os.getenv('PRE_RELEASE_VERSION', '')
__version__ = '0.2.0{}'.format(pre_release_version)

# initalize logger
logger = logging.getLogger('parqueryd')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s %(asctime)s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

# import helpers
