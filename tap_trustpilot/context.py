from datetime import datetime, date
import pendulum
import singer
from singer import bookmarks as bks_
from .http import Client


class Context:
    """Represents a collection of global objects necessary for performing
    discovery or for running syncs. Notably, it contains

    - config  - The JSON structure from the config.json argument
    - state   - The mutable state dict that is shared among streams
    - client  - An HTTP client object for interacting with the API
    - catalog - A singer.catalog.Catalog.
    """
    def __init__(self, config, catalog, state):
        self.config = config
        self.state = state
        self.client = Client(config)
        self.catalog = catalog
        self.now = datetime.utcnow()

        self.cache = {}

    def get_bookmark(self, path):
        return bks_.get_bookmark(self.state, *path)

    def set_bookmark(self, path, val):
        if isinstance(val, date):
            val = val.isoformat()
        bks_.write_bookmark(self.state, path[0], path[1], val)

    def get_offset(self, path):
        off = bks_.get_offset(self.state, path[0])
        return (off or {}).get(path[1])

    def set_offset(self, path, val):
        bks_.set_offset(self.state, path[0], path[1], val)

    def clear_offsets(self, tap_stream_id):
        bks_.clear_offset(self.state, tap_stream_id)

    def update_start_date_bookmark(self, path):
        val = self.get_bookmark(path)
        if not val:
            val = self.config["start_date"]
            self.set_bookmark(path, val)
        return pendulum.parse(val)

    def write_state(self):
        singer.write_state(self.state)
