from datetime import datetime, timedelta
import singer
from singer import bookmarks, utils
from . import transform

LOGGER = singer.get_logger()
PAGE_SIZE = 100


class Stream:
    key_properties = None
    replication_method = None
    replication_keys = None

    def __init__(self, tap_stream_id, path,
                 returns_collection=True,
                 collection_key=None,
                 custom_formatter=None,
                 requires_authentication=False):
        self.tap_stream_id = tap_stream_id
        self._path = path
        self.returns_collection = returns_collection
        self.collection_key = collection_key
        self.custom_formatter = custom_formatter or (lambda x: x)
        self.requires_authentication = requires_authentication

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

    @property
    def path(self):
        return self._path

    def sync(self, ctx):
        if self.requires_authentication:
            # make sure the client is authenticated
            ctx.client.ensure_auth(ctx.config)


class BusinessUnitStream(Stream):
    business_unit_find_path = '/business-units/find'

    _current_business_unit_id = None

    @property
    def path(self):
        """Builds the final path to be used in the client"""
        return super().path \
            .replace(':business_unit_id', self._current_business_unit_id)

    def sync_business_unit(self, ctx, business_unit_id):
        self._current_business_unit_id = business_unit_id

    def sync(self, ctx):
        super().sync(ctx)

        business_units = ctx.config.get('business_units',[])

        if not business_units and ctx.config.get('business_unit_id'):
            self.sync_business_unit(ctx, business_unit_id=ctx.config['business_unit_id'])
        else:
            # in case the user did not configure an array ...
            if not isinstance(business_units, list):
                business_units = [business_units]

            for business_unit in business_units:
                LOGGER.info("Sync business unit: {}".format(business_unit))
                path = self.business_unit_find_path
                params = {"name": business_unit}
                resp = ctx.client.GET({"path": path, "params": params}, self.tap_stream_id)
                business_unit = self.transform(ctx, [resp])

                if len(business_units) == 0:
                    raise RuntimeError(f"Business Unit with name {business_unit} was not found!")

                business_unit_id = business_unit[0]['id']

                self.sync_business_unit(ctx, business_unit_id)


class BusinessUnits(BusinessUnitStream):
    key_properties = ['id']
    replication_method = 'FULL_TABLE'

    def sync_business_unit(self, ctx, business_unit_id):
        super().sync_business_unit(ctx, business_unit_id)

        resp = ctx.client.GET({"path": self.path}, self.tap_stream_id)

        business_units = self.transform(ctx, [resp])

        if len(business_units) == 0:
            raise RuntimeError("Business Unit {} was not found!".format(business_unit_id))

        self.write_records(business_units)



class Paginated(Stream):
    def get_params(self, page, ctx):
        return {
            "page": page,
            "perPage": PAGE_SIZE,
            "orderBy": "createdat.asc"
        }

    def _modify_record(self, raw_record):
        pass

    def _sync(self, ctx):
        page = 1
        while True:
            LOGGER.info("Fetching page {} for {}".format(page, self.tap_stream_id))
            params = self.get_params(page, ctx)
            resp = ctx.client.GET({"path": self.path, "params": params}, self.tap_stream_id)
            raw_records = self.format_response(resp)

            for raw_record in raw_records:
                self._modify_record(raw_record)

            records = self.transform(ctx, raw_records)
            self.write_records(records)
            yield records

            if len(records) < PAGE_SIZE:
                break

            page += 1


class Reviews(Paginated, BusinessUnitStream):
    key_properties = ['business_unit_id','id']
    replication_method = 'FULL_TABLE'

    def add_consumers_to_cache(self, ctx, batch, business_unit_id):
        for record in batch:
            consumer_id = record.get('consumer', {}).get('id')
            if consumer_id is not None:
                ctx.cache['consumer_ids'][business_unit_id].add(consumer_id)

    def _modify_record(self, raw_record):
        raw_record['business_unit_id'] = self._current_business_unit_id

    def sync(self, ctx):
        ctx.cache['consumer_ids'] = dict()
        super().sync(ctx)

    def sync_business_unit(self, ctx, business_unit_id):
        super().sync_business_unit(ctx, business_unit_id)

        ctx.cache['consumer_ids'][business_unit_id] = set()

        for batch in self._sync(ctx):
            self.add_consumers_to_cache(ctx, batch, business_unit_id)


class Consumers(Stream):
    key_properties = ['id']
    replication_method = 'FULL_TABLE'

    def sync(self, ctx):
        for business_unit_id, customer_ids in ctx.cache['consumer_ids'].items():
            LOGGER.info("Sync business unit id: {}".format(business_unit_id))
            total = len(customer_ids)
            for i, consumer_id in enumerate(customer_ids):
                LOGGER.info("Fetching consumer {} of {} ({})".format(i + 1, total, consumer_id))
                path = self.path.format(consumerId=consumer_id)
                resp = ctx.client.GET({"path": path}, self.tap_stream_id)

                raw_records = self.format_response([resp])

                for raw_record in raw_records:
                    raw_record['business_unit_id'] = business_unit_id

                records = self.transform(ctx, raw_records)
                self.write_records(records)


class PrivateReviews(Paginated, BusinessUnitStream):
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    bookmark_field = 'createdAt'

    def get_params(self, page, ctx):
        params = super().get_params(page, ctx)

        start_date_time = bookmarks.get_bookmark(
            ctx.state, self.tap_stream_id,
            key=f'buid({self._current_business_unit_id})_lastCreatedAt')
        if start_date_time:
            # conver to datetime + add 1 millisecond so that we only get new records
            start_date_time = utils.strptime_to_utc(start_date_time) \
                              + timedelta(milliseconds=1)

        if not start_date_time and ctx.config.get('start_date'):
            start_date_time = utils.strptime_to_utc(ctx.config['start_date'])

        if start_date_time:
            params.update({
                'startDateTime': start_date_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            })

        return params

    def sync_business_unit(self, ctx, business_unit_id):
        super().sync_business_unit(ctx, business_unit_id)
        max_bookmark_value = None

        for batch in self._sync(ctx):
            for record in batch:
                bookmark_value = record.get(self.bookmark_field)
                if bookmark_value:
                    bookmark_dttm = utils.strptime_to_utc(bookmark_value)
                    if bookmark_dttm and (max_bookmark_value is None or
                                          bookmark_dttm > max_bookmark_value):
                        max_bookmark_value = bookmark_dttm

        if max_bookmark_value:
            bookmarks.write_bookmark(ctx.state, self.tap_stream_id,
                                     key=f'buid({business_unit_id})_lastCreatedAt',
                                     val=utils.strftime(max_bookmark_value))
            singer.write_state(ctx.state)


STREAMS = {
    'business_units': BusinessUnits(
        tap_stream_id='business_units',
        path="/business-units/:business_unit_id/profileinfo"),
    'reviews': Reviews(
        tap_stream_id='reviews',
        path='/business-units/:business_unit_id/reviews',
        collection_key='reviews'),
    'consumers': Consumers(
        tap_stream_id='consumers',
        path='/consumers/{consumerId}/profile'),
    'private_reviews': PrivateReviews(
        tap_stream_id='private_reviews',
        path='/private/business-units/:business_unit_id/reviews',
        collection_key='reviews',
        requires_authentication=True),
}
