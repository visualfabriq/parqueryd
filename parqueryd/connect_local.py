import argparse
import logging

import IPython

import configobj

from parqueryd.rpc import RPC

def main():
    parser = argparse.ArgumentParser(description='Start parqueryd worker.')
    parser.add_argument("--url", help='Controller URL to connect to')
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

    config = configobj.ConfigObj('/etc/parqueryd.cfg')
    redis_url = config.get('redis_url', 'redis://127.0.0.1:6379/0')

    if args.url:
        if not args.url.startswith('tcp:'):
            print('URL must start with `tcp:`')
            return
        rpc = RPC(address=args.url, redis_url=redis_url, loglevel=loglevel)

    else:
        rpc = RPC(redis_url=redis_url, loglevel=loglevel)

    IPython.embed()

if __name__ == '__main__':
    main()
