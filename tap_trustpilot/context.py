from datetime import datetime
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
