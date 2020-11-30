#!/usr/bin/env python3
import os
import json
import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata

from .streams import STREAMS


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    return utils.load_json(get_abs_path(path))


def check_credentials_are_authorized(client, config):
    client.auth(config)


def discover(client, config):
    check_credentials_are_authorized(client, config)
    streams = []
    for stream_name, stream_class in STREAMS.items():
        raw_schema=load_schema(stream_name)
        schema = Schema.from_dict(raw_schema)
        streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=stream_class.key_properties,
                schema=schema,
                metadata=metadata.get_standard_metadata(
                    schema=raw_schema,
                    schema_name=stream_name,
                    key_properties=stream_class.key_properties)
            )
        )
    return Catalog(streams)