import datetime
import singer

LOGGER = singer.get_logger()

class NotBrokenDatetimeTransformer(singer.Transformer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _transform_datetime(self, value):
        if value is None:
            return None

        if len(value) > 20:
            # sometimes the date includes milliseconds. Then we don't need a custom transformation
            return value

        dt = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def transform(data, schema):
    transformer = NotBrokenDatetimeTransformer()
    return transformer.transform(data, schema)
