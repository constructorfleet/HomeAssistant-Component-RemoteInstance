import logging

from aiohttp.web import HTTPFound
from homeassistant.components.http import HomeAssistantView
from homeassistant.core import callback

_LOGGER = logging.getLogger(__name__)

HEADER_KEY_AUTHORIZATION = 'Authorization'
HEADER_KEY_PASSWORD = 'X-HA-access'

HTTP_METHODS = [
    "get",
    "post",
    "delete",
    "put",
    "patch",
    "head",
    "options"
]
HTTP_METHODS_WITH_PAYLOAD = [
    'post',
    'put',
    'patch'
]


def register_proxy(hass, session, host, port, secure, access_token, password, route, method, auth_required):
    if method == 'get':
        proxy = GetRemoteApiProxy(
            hass,
            session,
            host,
            port,
            secure,
            access_token,
            password,
            route,
            method,
            auth_required
        )
    elif method == 'post':
        proxy = PostRemoteApiProxy(
            hass,
            session,
            host,
            port,
            secure,
            access_token,
            password,
            route,
            method,
            auth_required)
    elif method == 'put':
        proxy = PutRemoteApiProxy(
            hass,
            session,
            host,
            port,
            secure,
            access_token,
            password,
            route,
            method,
            auth_required)
    else:
        return
    _LOGGER.warning("PROXY %s" % proxy.__class__.__name__)
    hass.http.register_view(proxy)
    _LOGGER.warning("ROUTES %s" % str(hass.http.app))


class AbstractRemoteApiProxy(HomeAssistantView):
    """A proxy for remote API calls."""

    cors_allowed = True

    def __init__(self, hass, session, host, port, secure, access_token, password, route, method, auth_required):
        """Initializing the proxy."""
        if method not in HTTP_METHODS:
            return

        self.requires_auth = False
        self.url = route if str(route).startswith('/') else '/%s' % route
        self.name = self.url.replace('/', ':')[1:]

        _LOGGER.warning("PROXY %s %s" % (method, self.url))

        self._session = session
        self._hass = hass
        self._host = host
        self._port = port
        self._secure = secure
        self._access_token = access_token
        self._password = password
        self._auth_required = auth_required
        self._method = method

    async def perform_proxy(self, request):
        headers = {}
        _LOGGER.warning("Handing Proxy")

        if self._auth_required:
            auth_header_key, auth_header_value = self._get_auth_header()
            headers[auth_header_key] = auth_header_value

        request_method = getattr(self._session, self._method, None)
        if not request_method:
            _LOGGER.warning("Couldn't find method %s" % self._method)
            raise HTTPFound('/redirect')

        if self._method in HTTP_METHODS_WITH_PAYLOAD:
            await request_method(
                self._get_url(),
                json=request.json(),
                params=request.query,
                headers=headers
            )
        else:
            await request_method(
                self._get_url(),
                params=request.query,
                headers=headers
            )

    def _get_url(self):
        """Get url to connect to."""
        return '%s://%s:%s%s' % (
            'https' if self._secure else 'http', self._host, self._port, self.url)

    def _get_auth_header(self):
        """Get the authentication header."""
        if self._access_token:
            return HEADER_KEY_AUTHORIZATION, 'Bearer %s' % self._access_token
        else:
            return HEADER_KEY_PASSWORD, self._password


class GetRemoteApiProxy(AbstractRemoteApiProxy):

    def __init__(self, hass, session, host, port, secure, access_token, password, route, method, auth_required):
        super().__init__(hass, session, host, port, secure, access_token, password, route, method, auth_required)

    @callback
    def get(self, request):
        return self.perform_proxy(request)


class PostRemoteApiProxy(AbstractRemoteApiProxy):

    def __init__(self, hass, session, host, port, secure, access_token, password, route, method, auth_required):
        super().__init__(hass, session, host, port, secure, access_token, password, route, method, auth_required)

    @callback
    def post(self, request):
        return self.perform_proxy(request)


class PutRemoteApiProxy(AbstractRemoteApiProxy):

    def __init__(self, hass, session, host, port, secure, access_token, password, route, method, auth_required):
        super().__init__(hass, session, host, port, secure, access_token, password, route, method, auth_required)

    @callback
    def put(self, request):
        return self.perform_proxy(request)
