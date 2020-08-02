#!/usr/bin/env python
import logging
import sys

import configobj

import parqueryd

if os.path.exists('/etc/parqueryd.cfg'):
    config = configobj.ConfigObj('/etc/parqueryd.cfg')
else:
    # fallback for migrated bqueryd instances
    config = configobj.ConfigObj('/etc/bqueryd.cfg')

redis_url = config.get('redis_url', 'redis://127.0.0.1:6379/0')
azure_conn_string = config.get('azure_conn_string', None)


def main(argv=sys.argv):
    if '-vvv' in argv:
        loglevel = logging.DEBUG
    elif '-vv' in argv:
        loglevel = logging.INFO
    elif '-v' in argv:
        loglevel = logging.WARNING
    else:
        loglevel = logging.ERROR

    data_dir = parqueryd.DEFAULT_DATA_DIR
    for arg in argv:
        if arg.startswith('--data_dir='):
            data_dir = arg[11:]

    if 'controller' in argv:
        parqueryd.ControllerNode(redis_url=redis_url, loglevel=loglevel, azure_conn_string=azure_conn_string).go()
    elif 'worker' in argv:
        parqueryd.WorkerNode(redis_url=redis_url, loglevel=loglevel, data_dir=data_dir).go()
    elif 'downloader' in argv:
        parqueryd.DownloaderNode(redis_url=redis_url, loglevel=loglevel, azure_conn_string=azure_conn_string).go()
    elif 'moveparquet' in argv:
        parqueryd.MoveparquetNode(redis_url=redis_url, loglevel=loglevel).go()
    else:
        if len(argv) > 1 and argv[1].startswith('tcp:'):
            rpc = parqueryd.RPC(address=argv[1], redis_url=redis_url, loglevel=loglevel)
        else:
            rpc = parqueryd.RPC(redis_url=redis_url, loglevel=loglevel)
        import IPython
        IPython.embed()


if __name__ == '__main__':
    main()
