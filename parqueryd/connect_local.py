from __future__ import annotations

import argparse
import logging

import configobj
import IPython

from parqueryd.rpc import RPC


def main() -> None:
    """Main entry point for local parqueryd connection."""
    parser = argparse.ArgumentParser(description="Start parqueryd worker.")
    parser.add_argument("--url", help="Controller URL to connect to")
    parser.add_argument("-v", "--verbosity", action="count", help="Increase output verbosity")
    args = parser.parse_args()

    if args.verbosity == 3:
        loglevel = logging.DEBUG
    elif args.verbosity == 2:
        loglevel = logging.INFO
    elif args.verbosity == 1:
        loglevel = logging.WARNING
    else:
        loglevel = logging.ERROR

    config = configobj.ConfigObj("/etc/parqueryd.cfg")
    redis_url: str = config.get("redis_url", "redis://127.0.0.1:6379/0")

    if args.url:
        if not args.url.startswith("tcp:"):
            print("URL must start with `tcp:`")
            return
        RPC(address=args.url, redis_url=redis_url, loglevel=loglevel)

    else:
        RPC(redis_url=redis_url, loglevel=loglevel)

    IPython.embed()


if __name__ == "__main__":
    main()
