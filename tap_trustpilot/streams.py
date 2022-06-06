import singer
from tap_trustpilot.schemas import IDS
from tap_trustpilot import transform

LOGGER = singer.get_logger()
PAGE_SIZE = 100


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
    def get_params(self, page):
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
    def add_consumers_to_cache(self, ctx, batch):
        for record in batch:
            consumer_id = record.get('consumer', {}).get('id')
            if consumer_id is not None:
                ctx.cache['consumer_ids'].add(consumer_id)

    def sync(self, ctx):
        ctx.cache['consumer_ids'] = set()
        for batch in self._sync(ctx):
            self.add_consumers_to_cache(ctx, batch)


class Consumers(Stream):
    def sync(self, ctx):
        business_unit_id = ctx.cache['business_unit']['id']

        total = len(ctx.cache['consumer_ids'])
        for i, consumer_id in enumerate(ctx.cache['consumer_ids']):
            LOGGER.info("Fetching consumer {} of {} ({})".format(i + 1, total, consumer_id))
            path = self.path.format(consumerId=consumer_id)
            resp = ctx.client.GET({"path": path}, self.tap_stream_id)

            raw_records = self.format_response([resp])

            for raw_record in raw_records:
                raw_record['business_unit_id'] = business_unit_id

            records = self.transform(ctx, raw_records)
            self.write_records(records)


business_units = BusinessUnits(IDS.BUSINESS_UNITS, "/business-units/:business_unit_id/profileinfo")
all_streams = [
    business_units,
    Reviews(IDS.REVIEWS, '/business-units/:business_unit_id/reviews', collection_key='reviews'),
    Consumers(IDS.CONSUMERS, '/consumers/{consumerId}/profile')
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
