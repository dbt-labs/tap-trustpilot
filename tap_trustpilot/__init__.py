#!/usr/bin/env python3
import os
import json
import singer
from singer import utils

from .http import Client
from .discover import discover
from .sync import sync

REQUIRED_CONFIG_KEYS = [
    "access_key",
    "client_secret",
    "username",
    "password",
    "business_unit_id"
]

LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    state = {}
    if args.state:
        state = args.state

    config = {}
    if args.config:
        config = args.config

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        client = Client(config)
        catalog = discover(client, config)
        catalog.dump()
    # Otherwise run in sync mode
    elif args.catalog:
        sync(config=config,
             catalog=args.catalog,
             state=state)

if __name__ == "__main__":
    main()
