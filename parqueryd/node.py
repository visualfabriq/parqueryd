#!/usr/bin/env python
import argparse
import logging

import configobj

import sentry_sdk

import parqueryd.config
from parqueryd.controller import ControllerNode
from parqueryd.worker import WorkerNode, DownloaderNode, MoveparquetNode

NODE_TYPES = ['controller', 'worker', 'downloader', 'moveparquet']

def main():
    parser = argparse.ArgumentParser(description='Start parqueryd worker.')
    parser.add_argument("worker_type", choices=NODE_TYPES,
                        help='Type of node to run')
    parser.add_argument('--data_dir', default=parqueryd.config.DEFAULT_DATA_DIR,
                        help='Local directory worker nodes will read from')
    parser.add_argument("-v", "--verbosity", action="count",
                        help="Increase output verbosity")
    args = parser.parse_args()

    if args.verbosity == 3:
        loglevel = logging.DEBUG
    elif args.verbosity == 2:
        loglevel = logging.INFO
    elif args.verbosity == 1:
        loglevel = logging.WARNING
    else:
        loglevel = logging.ERROR

    if parqueryd.config.SENTRY_DSN is not None:
        sentry_sdk.init(
            dsn=parqueryd.config.SENTRY_DSN,
            traces_sample_rate=1.0
        )

    config = configobj.ConfigObj('/etc/parqueryd.cfg')
    redis_url = config.get('redis_url', 'redis://127.0.0.1:6379/0')
    azure_conn_string = config.get('azure_conn_string', None)

    if args.worker_type == 'controller':
        ControllerNode(redis_url=redis_url, loglevel=loglevel, azure_conn_string=azure_conn_string).go()

    elif args.worker_type == 'worker':
        WorkerNode(redis_url=redis_url, loglevel=loglevel, data_dir=args.data_dir).go()

    elif args.worker_type == 'downloader':
        DownloaderNode(redis_url=redis_url, loglevel=loglevel, azure_conn_string=azure_conn_string).go()

    elif args.worker_type == 'moveparquet':
        MoveparquetNode(redis_url=redis_url, loglevel=loglevel).go()


if __name__ == '__main__':
    main()
