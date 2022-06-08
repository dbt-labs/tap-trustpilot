import json

import singer
# from tap_trustpilot.schemas import IDS
from tap_trustpilot import transform

LOGGER = singer.get_logger()
PAGE_SIZE = 100
CONSUMER_CHUNK_SIZE = 1000

class IDS(object):
    BUSINESS_UNITS = "business_units"
    REVIEWS = "reviews"
    CONSUMERS = "consumers"

stream_ids = [getattr(IDS, x) for x in dir(IDS)
              if not x.startswith("__")]

PK_FIELDS = {
    IDS.BUSINESS_UNITS: ["id"],
    IDS.REVIEWS: ["business_unit_id", "id"],
    IDS.CONSUMERS: ["id"],
}

class Stream(object):
    def __init__(self, tap_stream_id, path,
                 returns_collection=True,
                 collection_key=None,
                 custom_formatter=None):
        self.tap_stream_id = tap_stream_id
        self.path = path
        self.returns_collection = returns_collection
        self.collection_key = collection_key
        self.custom_formatter = custom_formatter or (lambda x: x)

    def metrics(self, records):
        with singer.metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(records))

    def write_records(self, records):
        singer.write_records(self.tap_stream_id, records)
        self.metrics(records)

    def format_response(self, response):
        if self.returns_collection:
            if self.collection_key:
                records = (response or {}).get(self.collection_key, [])
            else:
                records = response or []
        else:
            records = [] if not response else [response]

        return self.custom_formatter(records)

    def transform(self, ctx, records):
        schema = ctx.catalog.get_stream(self.tap_stream_id).schema.to_dict()
        transformed = [transform.transform(item, schema) for item in records]
        return transformed


class BusinessUnits(Stream):

    # tap_stream_id = "business_units"
    key_properties = ['id']
    replication_keys  = None
    replication_method = "FULL_TABLE"
    params = {}
    def raw_fetch(self, ctx):
        return ctx.client.GET({"path": self.path}, self.tap_stream_id)

    def fetch_into_cache(self, ctx):
        business_unit_id = ctx.config['business_unit_id']

        resp = self.raw_fetch(ctx)
        resp['id'] = business_unit_id

        business_units = self.transform(ctx, [resp])

        if len(business_units) == 0:
            raise RuntimeError("Business Unit {} was not found!".format(business_unit_id))

        ctx.cache["business_unit"] = business_units[0]

    def sync(self, ctx):
        self.write_records([ctx.cache["business_unit"]])


class Paginated(Stream):
    @staticmethod
    def get_params(page):
        return {
            "page": page,
            "perPage": PAGE_SIZE,
            "orderBy": "createdat.asc"
        }

    def _sync(self, ctx):
        business_unit_id = ctx.cache['business_unit']['id']

        page = 1
        while True:
            LOGGER.info("Fetching page {} for {}".format(page, self.tap_stream_id))
            params = self.get_params(page)
            resp = ctx.client.GET({"path": self.path, "params": params}, self.tap_stream_id)
            raw_records = self.format_response(resp)

            for raw_record in raw_records:
                raw_record['business_unit_id'] = business_unit_id

            records = self.transform(ctx, raw_records)
            self.write_records(records)
            yield records

            if len(records) < PAGE_SIZE:
                break

            page += 1


class Reviews(Paginated):

    key_properties = ["business_unit_id", "id"]
    replication_keys = None
    replication_method = "FULL_TABLE"
    params = {}

    @staticmethod
    def add_consumers_to_cache(ctx, batch):
        for record in batch:
            consumer_id = record.get('consumer', {}).get('id')
            if consumer_id is not None:
                ctx.cache['consumer_ids'].add(consumer_id)

    def sync(self, ctx):
        ctx.cache['consumer_ids'] = set()
        for batch in self._sync(ctx):
            self.add_consumers_to_cache(ctx, batch)


class Consumers(Stream):

    key_properties = ['id']
    replication_keys = None
    replication_method = "FULL_TABLE"
    params = {}

    def sync(self, ctx):
        business_unit_id = ctx.cache['business_unit']['id']

        total = len(ctx.cache.get('consumer_ids',[]))

        # chunk list of consumer IDs to smaller lists of size 1000
        consumer_ids = list(ctx.cache.get('consumer_ids',[]))
        chunked_consumer_ids = [consumer_ids[i: i+CONSUMER_CHUNK_SIZE] for i in range(0, len(consumer_ids),
                                                                                      CONSUMER_CHUNK_SIZE)]

        for i, consumer_id_list in enumerate(chunked_consumer_ids):
            LOGGER.info("Fetching consumer page {} of {}".format(i + 1, len(chunked_consumer_ids)))
            resp = ctx.client.POST({"path": self.path, "payload": json.dumps({"consumerIds": consumer_id_list})},
                                   self.tap_stream_id)

            raw_records = self.format_response([resp])
            raw_records = list(raw_records[0].get('consumers', {}).values())
            for raw_record in raw_records:
                raw_record['business_unit_id'] = business_unit_id

            records = self.transform(ctx, raw_records)
            self.write_records(records)


business_units = BusinessUnits(IDS.BUSINESS_UNITS, "/business-units/:business_unit_id/profileinfo")
all_streams = [
    business_units,
    Reviews(IDS.REVIEWS, '/business-units/:business_unit_id/reviews', collection_key='reviews'),
    Consumers(IDS.CONSUMERS, '/consumers/profile/bulk')
]
all_stream_ids = [s.tap_stream_id for s in all_streams]

STREAMS = {
    'business_units': BusinessUnits(IDS.BUSINESS_UNITS, "/business-units/:business_unit_id/profileinfo"),
    'reviews': Reviews(IDS.REVIEWS, '/business-units/:business_unit_id/reviews', collection_key='reviews'),
    'consumers': Consumers(IDS.CONSUMERS, '/consumers/{consumerId}/profile')
}
    

