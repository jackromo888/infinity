from argparse import ArgumentParser
from enum import Enum
import logging
import os
import sys
from typing import Any, List, NamedTuple

import gevent

from fix.client import FixClient

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class Exchange(str, Enum):
    FTX = 'FTX'
    FTXUS = 'FTXUS'


class ConnectionConfig(NamedTuple):
    url: str
    target_id: str


def eprint(*args, **kwargs) -> None:
    """Print to stderr."""
    print(*args, file=sys.stderr, **kwargs)


def check_env_vars() -> None:
    required_env_vars = ['FTX_API_SECRET', 'FTX_API_KEY']
    for env_var in required_env_vars:
        if env_var not in os.environ:
            eprint(f"missing env var: {env_var}. make sure you have set all required "
                   f"variables: {required_env_vars!r}.")
            sys.exit(1)


def get_connection_config(exchange: Exchange) -> ConnectionConfig:
    connection_config = {
        Exchange.FTX: ConnectionConfig(url='tcp+ssl://fix.ftx.com:4363',
                                       target_id=Exchange.FTX.value),
        Exchange.FTXUS: ConnectionConfig(url='tcp+ssl://fix.ftx.us:4363',
                                         target_id=Exchange.FTXUS.value),
    }
    return connection_config[exchange]


def parse_args(args: List[str]) -> Any:
    parser = ArgumentParser()
    parser.add_argument('exchange', default='ftx', choices=[Exchange.FTX.value.lower(),
                                                            Exchange.FTXUS.value.lower()])
    return parser.parse_args(args)


def main():
    # Parse command-line arguments.
    opts = parse_args(sys.argv[1:])

    # Get a connection config.
    config = get_connection_config(opts.exchange.upper())

    # Check that we've set our env vars correctly.
    check_env_vars()

    # Pull the credentials from the environment.
    api_key = os.environ['FTX_API_KEY']
    secret = os.environ['FTX_API_SECRET']

    logger.debug("creating FixClient")
    client = FixClient(url=config.url, target_id=config.target_id, client_id=api_key)
    logger.debug("logging in")
    client.login(secret)
    gevent.get_hub().join()
    logger.debug("done")


if __name__ == '__main__':
    main()
