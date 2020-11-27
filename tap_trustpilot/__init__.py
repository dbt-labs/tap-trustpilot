#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata

from . import schemas
from .http import Client
from .sync import sync

REQUIRED_CONFIG_KEYS = [
    "access_key",
    "client_secret",
    "username",
    "password",
    "business_unit_id"
]

LOGGER = singer.get_logger()


def check_credentials_are_authorized(client, config):
    client.auth(config)


def discover(client, config):
    check_credentials_are_authorized(client, config)
    streams = []
    for tap_stream_id in schemas.stream_ids:
        raw_schema=schemas.load_schema(tap_stream_id)
        schema = Schema.from_dict(raw_schema)
        streams.append(
            CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=schemas.PK_FIELDS[tap_stream_id],
                schema=schema,
                metadata=metadata.get_standard_metadata(
                    schema=raw_schema,
                    schema_name=tap_stream_id,
                    key_properties=schemas.PK_FIELDS[tap_stream_id])
            )
        )
    return Catalog(streams)


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
