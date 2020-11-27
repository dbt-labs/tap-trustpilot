import singer

from .context import Context
from . import streams as streams_
from .streams import all_streams


LOGGER = singer.get_logger()


# Main routine: orchestrates pulling data for selected streams.
def sync(config, catalog, state):
    """ Sync data from tap source """

    ctx = Context(config, catalog, state)
    streams_.business_units.fetch_into_cache(ctx)

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: %s', last_stream)
    selected_streams = []
    selected_streams_by_name = {}
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
        selected_streams_by_name[stream.stream] = stream

    LOGGER.info('selected_streams: %s', selected_streams)

    if not selected_streams or selected_streams == []:
        return

    # Loop through endpoints in selected_streams
    for stream_class in all_streams:
        stream_name = stream_class.tap_stream_id
        if stream_name in selected_streams:
            LOGGER.info('START Syncing: {}'.format(stream_name))
            stream = selected_streams_by_name[stream_name]

            # Publish schema to singer.
            stream = catalog.get_stream(stream_name)
            schema = stream.schema.to_dict()
            try:
                singer.write_schema(stream_name, schema, stream.key_properties)
            except OSError as err:
                LOGGER.info('OS Error writing schema for: %s', stream_name)
                raise err

            # execute sync
            stream_class.sync(ctx)

            LOGGER.info('FINISHED Syncing: %s', stream_name)

    LOGGER.info('sync.py: sync complete')
