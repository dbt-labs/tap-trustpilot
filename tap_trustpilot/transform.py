
import singer
import datetime

LOGGER = singer.get_logger()

class NotBrokenDatetimeTransformer(singer.Transformer):
    def __init__(self, *args, **kwargs):
        super(NotBrokenDatetimeTransformer, self).__init__(*args, **kwargs)

    def _transform_datetime(self, value):
        if value is None:
            return None

        dt = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def transform(data, schema):
    transformer = NotBrokenDatetimeTransformer()
    return transformer.transform(data, schema)
