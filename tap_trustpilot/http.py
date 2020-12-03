import base64
import backoff
import requests
from singer import metrics

BASE_URL = "https://api.trustpilot.com/v1"
AUTH_URL = "{}/oauth/oauth-business-users-for-applications/accesstoken".format(BASE_URL)


class RateLimitException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client:
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.session = requests.Session()

        self.business_unit_id = config['business_unit_id']
        self.access_key = config['access_key']
        self._token = None

    def get_token(self, config):
        if 'client_secret' not in config or 'username' not in config or 'password' not in config:
            raise Exception('For authentication the config properties client_secret, username and password are required')
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

    def ensure_auth(self, config):
        """Make sure the client is authenticated"""
        if not self._token:
            self.auth(config)

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent

        if self._token:
            request.headers['Authorization'] = 'Bearer {}'.format(self._token)
        request.headers['apikey'] = self.access_key

        return self.session.send(request.prepare())

    def url(self, path):
        joined = _join(BASE_URL, path)
        return joined.replace(':business_unit_id', self.business_unit_id)

    def create_get_request(self, path, **kwargs):
        return requests.Request(method="GET", url=self.url(path), **kwargs)

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request_with_handling(self, request, tap_stream_id):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 503]:
            raise RateLimitException()
        response.raise_for_status()
        return response.json()

    def GET(self, request_kwargs, *args, **kwargs):
        req = self.create_get_request(**request_kwargs)
        return self.request_with_handling(req, *args, **kwargs)
