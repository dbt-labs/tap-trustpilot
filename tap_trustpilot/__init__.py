#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_trustpilot import streams as streams_
from tap_trustpilot.context import Context
from tap_trustpilot import schemas

REQUIRED_CONFIG_KEYS = [
    "access_key",
    "client_secret",
    "username",
    "password",
    "business_unit_id"
]

LOGGER = singer.get_logger()


def check_credentials_are_authorized(ctx):
    ctx.client.auth(ctx.config)


def discover(ctx):
    check_credentials_are_authorized(ctx)
    catalog = Catalog([])
    for tap_stream_id in schemas.stream_ids:
        schema = Schema.from_dict(schemas.load_schema(tap_stream_id),
                                  inclusion="automatic")
        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
        ))
    return catalog


def output_schema(stream):
    schema = schemas.load_schema(stream.tap_stream_id)
    pk_fields = schemas.PK_FIELDS[stream.tap_stream_id]
    singer.write_schema(stream.tap_stream_id, schema, pk_fields)


def sync(ctx):
    streams_.business_units.fetch_into_cache(ctx)

    currently_syncing = ctx.state.get("currently_syncing")
    start_idx = streams_.all_stream_ids.index(currently_syncing) \
        if currently_syncing else 0
    stream_ids_to_sync = [cs.tap_stream_id for cs in ctx.catalog.streams
                          if cs.is_selected()]
    streams = [s for s in streams_.all_streams[start_idx:]
               if s.tap_stream_id in stream_ids_to_sync]

    for stream in streams:
        ctx.state["currently_syncing"] = stream.tap_stream_id
        output_schema(stream)
        ctx.write_state()
        stream.sync(ctx)
    ctx.state["currently_syncing"] = None
    ctx.write_state()

@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    if args.discover:
        discover(ctx).dump()
        print()
    else:
        ctx.catalog = Catalog.from_dict(args.catalog.to_dict()) \
            if args.catalog else discover(ctx)
        sync(ctx)

if __name__ == "__main__":
    main()
