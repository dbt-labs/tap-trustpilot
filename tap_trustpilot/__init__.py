#!/usr/bin/env python3
import os
import json
import singer
from singer import utils

from .http import Client
from .discover import discover
from .sync import sync

REQUIRED_CONFIG_KEYS = [
    "access_key"
]

LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.config.get('business_unit_id') is None and args.config.get('business_units') is None:
        raise Exception("Config is missing key business_units or business_unit_id")

    state = {}
    if args.state:
        state = args.state

    config = {}
    if args.config:
        config = args.config

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    elif args.catalog:
        sync(config=config,
             catalog=args.catalog,
             state=state)

if __name__ == "__main__":
    main()
