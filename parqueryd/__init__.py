import logging
# from version import __version__


# initalize logger
logger = logging.getLogger('parqueryd')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s %(asctime)s %(name)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
logger.addHandler(ch)

# import helpers
