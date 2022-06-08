#!/usr/bin/env python3
import os
import json
import singer
from singer import utils,metadata
from tap_trustpilot.streams import STREAMS, PK_FIELDS, IDS


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    return utils.load_json(get_abs_path(path))


def load_and_write_schema(tap_stream_id):
    schema = load_schema(tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])

def get_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():
        path = get_abs_path(f'schemas/{stream_name}.json')
        with open(path, encoding='utf-8') as file:
            schema = json.load(file)
        schemas[stream_name] = schema

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.key_properties,
            replication_method=stream_metadata.replication_method,
            valid_replication_keys=stream_metadata.replication_keys
        )
        field_metadata[stream_name] = mdata
        field_metadata["stream"] = STREAMS

    return schemas, field_metadata
