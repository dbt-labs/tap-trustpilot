import requests
from singer import metrics
import singer
import backoff
import base64

LOGGER = singer.get_logger()

BASE_URL = "https://api.trustpilot.com/v1"
AUTH_URL = "{}/oauth/oauth-business-users-for-applications/accesstoken".format(BASE_URL)


class RateLimitException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.session = requests.Session()

        self.business_unit_id = config['business_unit_id']
        self.access_key = config['access_key']
        self._token = None

    def get_token(self, config):
        creds = "{}:{}".format(config['access_key'], config['client_secret']).encode()
        encoded_creds = base64.b64encode(creds)
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': encoded_creds
        }

        data = {
            'grant_type': 'password',
            'username': config['username'],
            'password': config['password']
        }

        resp = requests.post(url=AUTH_URL, headers=headers, data=data)
        resp.raise_for_status()
        return resp.json()

    @property
    def token(self):
        if not self._token:
            raise RuntimeError("Client is not yet authorized")
        return self._token

    def auth(self, config):
        resp = self.get_token(config)
        token = resp['access_token']
        self._token = token

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent

        request.headers['Authorization'] = 'Bearer {}'.format(self._token)
        request.headers['apikey'] = self.access_key
        request.headers['Content-Type'] = 'application/json'

        return self.session.send(request.prepare())

    def url(self, path):
        joined = _join(BASE_URL, path)
        return joined.replace(':business_unit_id', self.business_unit_id)

    def create_get_request(self, path, **kwargs):
        return requests.Request(method="GET", url=self.url(path), **kwargs)

    def create_post_request(self, path, payload, **kwargs):
        return requests.Request(method="POST", url=self.url(path), data=payload, **kwargs)

    @backoff.on_exception(backoff.expo, RateLimitException, max_tries=10, factor=2)
    @backoff.on_exception(backoff.expo, requests.Timeout, max_tries=10, factor=2)
    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 503]:
            raise RateLimitException()
        # below exception should handle Pagination limit exceeded error if page value is more than 1000
        # depends on access level of access_token being used in config.json file
        if response.status_code == 400 and response.json().get('details') == "Pagination limit exceeded.":
            LOGGER.warning("400 Bad Request, Pagination limit exceeded.")
            return []
        response.raise_for_status()
        return response.json()

    def GET(self, request_kwargs, *args, **kwargs):
        req = self.create_get_request(**request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)

    def POST(self, request_kwargs, *args, **kwargs):
        req = self.create_post_request(**request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)
