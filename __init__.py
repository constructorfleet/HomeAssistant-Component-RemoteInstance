"""
Connect two Home Assistant instances via the Websocket API.

For more details about this component, please refer to the documentation at
https://github.com/constructorfleet/HomeAssistant-Component-RemoteInstance
"""

import asyncio
import copy
import json
import logging
from json import JSONDecodeError

import aiohttp
import homeassistant.components.websocket_api.auth as api
import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from aiohttp import ClientError, ClientTimeout, web, hdrs
from aiohttp.web import Response
from homeassistant.components.http import HomeAssistantView
from homeassistant.config import DATA_CUSTOMIZE
from homeassistant.const import (CONF_HOST, CONF_PORT)
from homeassistant.const import (
    EVENT_HOMEASSISTANT_STOP, EVENT_SERVICE_REGISTERED, ATTR_DOMAIN, ATTR_SERVICE,
    EVENT_CALL_SERVICE, ATTR_ENTITY_PICTURE, EVENT_STATE_CHANGED)
from homeassistant.core import EventOrigin, split_entity_id
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_component import DATA_INSTANCES, EntityComponent
from homeassistant.helpers.typing import HomeAssistantType, ConfigType

_LOGGER = logging.getLogger(__name__)

CONF_INSTANCES = 'instances'
CONF_SECURE = 'secure'
CONF_VERIFY_SSL = 'verify_ssl'
CONF_TOKEN = 'access_token'
CONF_PASSWORD = 'api_password'
CONF_SUBSCRIBE_TO = 'subscribe_to'
CONF_ENTITY_PREFIX = 'entity_prefix'
CONF_PROXY_API = 'proxy_api'
CONF_PROXY_COMPONENTS = 'proxy_components'

DOMAIN = 'remote_homeassistant'

DEFAULT_SUBSCRIBED_EVENTS = [EVENT_STATE_CHANGED,
                             EVENT_SERVICE_REGISTERED]
DEFAULT_ENTITY_PREFIX = ''
DEFAULT_PROXY_API = True

EVENT_ROUTE_REGISTERED = 'route_registered'
EVENT_TYPE_REQUEST_ROUTES = 'request_routes'
ATTR_ROUTE = 'route'
ATTR_METHOD = 'method'
ATTR_AUTH_REQUIRED = 'auth_required'
ATTR_PROXY = 'proxy'
ATTR_RESPONSE = 'result'
ATTR_STATUS = 'status'
ATTR_BODY = 'body'

DATA_PROXIES = 'proxies'

HEADER_KEY_PASSWORD = 'X-HA-access'
HEADER_KEY_AUTHORIZATION = 'Authorization'

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

ROUTE_PREFIX_SERVICE_CALL = '/api/services/'

INSTANCES_SCHEMA = vol.Schema({
    vol.Required(CONF_HOST): cv.string,
    vol.Optional(CONF_PORT, default=8123): cv.port,
    vol.Optional(CONF_SECURE, default=False): cv.boolean,
    vol.Optional(CONF_VERIFY_SSL, default=True): cv.boolean,
    vol.Exclusive(CONF_TOKEN, 'auth'): cv.string,
    vol.Exclusive(CONF_PASSWORD, 'auth'): cv.string,
    vol.Optional(CONF_SUBSCRIBE_TO,
                 default=DEFAULT_SUBSCRIBED_EVENTS): vol.All(cv.ensure_list, [cv.string]),
    vol.Optional(CONF_ENTITY_PREFIX, default=DEFAULT_ENTITY_PREFIX): cv.string,
    vol.Optional(CONF_PROXY_API, default=False): cv.boolean,
    vol.Optional(CONF_PROXY_COMPONENTS, default=[]): vol.All(cv.ensure_list, [cv.string])
})

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required(CONF_INSTANCES): vol.All(cv.ensure_list,
                                              [INSTANCES_SCHEMA]),
    }),
}, extra=vol.ALLOW_EXTRA)


async def async_setup(hass: HomeAssistantType, config: ConfigType):
    """Set up the remote_homeassistant component."""
    conf = config.get(DOMAIN)

    hass.data[DOMAIN] = {
        DATA_PROXIES: {}
    }

    for method in HTTP_METHODS:
        hass.data[DOMAIN][method] = {}

    for instance in conf.get(CONF_INSTANCES):
        connection = RemoteInstance(hass, instance)
        asyncio.ensure_future(connection.async_connect())

    return True


class RemoteInstance:
    """A Websocket connection to a remote home-assistant instance."""

    def __init__(self, hass, conf):
        """Initialize the connection."""
        self._hass = hass
        self._host = conf[CONF_HOST]
        self._port = conf[CONF_PORT]
        self._secure = conf[CONF_SECURE]
        self._verify_ssl = conf[CONF_VERIFY_SSL]
        self._token = conf.get(CONF_TOKEN, None)
        self._password = conf.get(CONF_PASSWORD, None)
        self._subscribe_to = conf[CONF_SUBSCRIBE_TO]
        self._entity_prefix = conf[CONF_ENTITY_PREFIX]
        self._proxy_api = conf[CONF_PROXY_API]
        self._proxy_components = conf[CONF_PROXY_COMPONENTS]

        self._connection = None
        self._entities = set()
        self._handlers = {}
        self._remove_listener = None

        self._session = aiohttp.ClientSession(timeout=ClientTimeout(total=0.5 * 60)) \
            if EVENT_ROUTE_REGISTERED in self._subscribe_to and self._proxy_api \
            else None

        self.__id = 1

    @callback
    def _get_url(self, scheme, route):
        """Get url to connect to."""
        return '%s://%s:%s%s' % (
            '%ss' % scheme if self._secure else scheme,
            self._host,
            self._port,
            route if route else ""
        )

    async def async_connect(self):
        """Connect to remote home-assistant websocket..."""
        url = self._get_url("ws", "/api/websocket")

        session = async_get_clientsession(self._hass, self._verify_ssl)

        while True:
            try:
                _LOGGER.info('Connecting to %s', url)
                self._connection = await session.ws_connect(url)
            except ClientError as err:
                _LOGGER.error(
                    'Could not connect to %s, retry in 10 seconds... %s', url, str(err))
                await asyncio.sleep(10)
            else:
                _LOGGER.info(
                    'Connected to home-assistant websocket at %s', url)
                break

        async def stop():
            """Close connection."""
            if self._connection is not None:
                await self._connection.close()

        self._hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, stop)

        asyncio.ensure_future(self._recv())

    def _next_id(self):
        _id = self.__id
        self.__id += 1
        return _id

    async def _call(self, handler, message_type, **extra_args):
        _id = self._next_id()
        self._handlers[_id] = handler
        try:
            await self._connection.send_json(
                {'id': _id, 'type': message_type, **extra_args})
        except ClientError as err:
            _LOGGER.error('remote websocket connection closed: %s', err)
            await self._disconnected()

    async def _disconnected(self):
        # Remove all published entries
        for entity in self._entities:
            self._hass.states.async_remove(entity)
        if self._remove_listener is not None:
            self._remove_listener()
        self._remove_listener = None
        self._entities = set()
        asyncio.ensure_future(self.async_connect())

    async def _recv(self):
        async def _get_message():
            message = None
            try:
                data = await self._connection.receive()
            except ClientError as err:
                _LOGGER.error('remote websocket connection closed: %s', err)
                return None

            if not data or data.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                _LOGGER.error('websocket connection is closing')
                return None

            if not data or data.type == aiohttp.WSMsgType.ERROR:
                _LOGGER.error('websocket connection had an error')
                return None

            try:
                if data:
                    message = data.json()
            except TypeError as err:
                _LOGGER.error('could not decode data (%s) as json: %s', data, err)

            return message

        while not self._connection.closed:
            message = await _get_message()

            if message and message['type'] == api.TYPE_AUTH_OK:
                await self._init()

            elif message and message['type'] == api.TYPE_AUTH_REQUIRED:
                if not (self._token or self._password):
                    _LOGGER.error('Access token or api password required, but not provided')
                    return
                if self._token:
                    data = {'type': api.TYPE_AUTH, 'access_token': self._token}
                else:
                    data = {'type': api.TYPE_AUTH, 'api_password': self._password}
                try:
                    await self._connection.send_json(data)
                except Exception as err:
                    _LOGGER.error('could not send data to remote connection: %s', err)
                    break

            elif message and message['type'] == api.TYPE_AUTH_INVALID:
                _LOGGER.error('Auth invalid, check your access token or API password')
                await self._connection.close()
                return

            elif message:
                handler = self._handlers.get(message['id'])
                if handler is not None:
                    if asyncio.iscoroutine(handler):
                        await handler
                    else:
                        handler(message)

        await self._disconnected()

    async def _init(self):
        self._remove_listener = self._hass.bus.async_listen(EVENT_CALL_SERVICE, self.forward_event)

        for event_type in self._subscribe_to:
            await self._call(self.fire_event, 'subscribe_events', event_type=event_type)

        if EVENT_SERVICE_REGISTERED in self._subscribe_to:
            await self._call(self.got_services, 'get_services')

        if EVENT_ROUTE_REGISTERED in self._subscribe_to:
            await self._call(self.fire_event, EVENT_TYPE_REQUEST_ROUTES)

        await self._call(self.got_states, 'get_states')

    async def forward_event(self, event):
        """Send local event to remote instance.

        The affected entity_id has to origin from that remote instance,
        otherwise the event is dicarded.
        """
        event_data = event.data
        service_data = event_data['service_data']

        if not service_data:
            return

        entity_ids = service_data.get('entity_id', None)

        if not entity_ids:
            return

        if isinstance(entity_ids, str):
            entity_ids = (entity_ids.lower(),)

        entity_ids = self._entities.intersection(entity_ids)

        if not entity_ids:
            return

        if self._entity_prefix:
            def _remove_prefix(entity_id):
                domain, object_id = split_entity_id(entity_id)
                object_id = object_id.replace(self._entity_prefix, '', 1)
                return domain + '.' + object_id

            entity_ids = {_remove_prefix(entity_id)
                          for entity_id in entity_ids}

        event_data = copy.deepcopy(event_data)
        event_data['service_data']['entity_id'] = list(entity_ids)

        # Remove service_call_id parameter - websocket API
        # doesn't accept that one
        event_data.pop('service_call_id', None)

        _id = self._next_id()
        data = {
            'id': _id,
            'type': event.event_type,
            **event_data
        }

        try:
            await self._connection.send_json(data)
        except Exception as err:
            _LOGGER.error('could not send data to remote connection: %s', err)
            await self._disconnected()

    def state_changed(self, entity_id, state, attr):
        """Publish remote state change on local instance."""
        domain, object_id = split_entity_id(entity_id)
        if self._entity_prefix:
            object_id = self._entity_prefix + object_id
            entity_id = domain + '.' + object_id

        route = attr.get(
            ATTR_ENTITY_PICTURE,
            ''
        ).split('?')[0].replace(
            entity_id,
            '{entity_id}'
        )
        if ATTR_ENTITY_PICTURE in attr \
                and self._proxy_api \
                and any([proxy_component for proxy_component in self._proxy_components if
                         proxy_component in route]):
            method = 'get'
            register_proxy(
                self._hass,
                ProxyData(
                    self._session,
                    method,
                    self._host,
                    self._port,
                    self._secure,
                    self._token,
                    self._password,
                    route)
            )

        # Add local customization data
        if DATA_CUSTOMIZE in self._hass.data:
            attr.update(self._hass.data[DATA_CUSTOMIZE].get(entity_id))

        self._entities.add(entity_id)
        self._hass.states.async_set(entity_id, state, attr)

    def fire_event(self, message):
        """Publish remove event on local instance."""
        if message['type'] == 'result':
            return

        if message['type'] != 'event':
            return

        if message['event']['event_type'] == EVENT_STATE_CHANGED:
            entity_id = message['event']['data']['entity_id']
            state = message['event']['data']['new_state']['state']
            attr = message['event']['data']['new_state']['attributes']
            self.state_changed(entity_id, state, attr)
        elif message['event']['event_type'] == EVENT_ROUTE_REGISTERED and self._proxy_api:
            data = message['event']['data']
            route = str(data[ATTR_ROUTE]).split('?')[0]
            if any(
                    [proxy_component for proxy_component in self._proxy_components if
                     proxy_component in route]
            ):
                method = data[ATTR_METHOD]
                register_proxy(
                    self._hass,
                    ProxyData(
                        self._session,
                        method,
                        self._host,
                        self._port,
                        self._secure,
                        self._token,
                        self._password,
                        route
                    )
                )
        else:
            event = message['event']
            self._hass.bus.async_fire(
                event_type=event['event_type'],
                event_data=event['data'],
                origin=EventOrigin.remote
            )

    def got_states(self, message):
        """Called when list of remote states is available."""
        for entity in message['result']:
            entity_id = entity['entity_id']
            state = entity['state']
            attributes = entity['attributes']

            self.state_changed(entity_id, state, attributes)

    async def got_services(self, message):
        """Handle services message."""
        for domain in message['result']:
            if domain not in self._hass.data.get(DATA_INSTANCES, {})[domain]:
                await EntityComponent(
                    logging.getLogger(EntityComponent.__name__),
                    domain,
                    self._hass
                ).async_setup({domain: {}})
            for service in domain:
                self._hass.bus.async_fire(
                    event_type=EVENT_SERVICE_REGISTERED,
                    event_data={ATTR_DOMAIN: domain, ATTR_SERVICE: service},
                    origin=EventOrigin.remote
                )
                if self._proxy_api:
                    register_proxy(
                        self._hass,
                        ProxyData(
                            self._session,
                            'post',
                            self._host,
                            self._port,
                            self._secure,
                            self._token,
                            self._password,
                            '/api/services/%s/%s' % (domain, service)
                        )
                    )


def register_proxy(hass, proxy):
    """Registers a proxy with the http router."""
    if str(proxy.route).startswith("http") or '/local' in proxy.route:
        return

    proxy_route = hass.data[DOMAIN].get(proxy.method, {}).get(proxy.route, None)
    if proxy_route:
        proxy_route.add_proxy(proxy)
    else:
        proxy_class = {
            'get': GetRemoteApiProxy,
            "post": PostRemoteApiProxy,
            "delete": DeleteRemoteApiProxy,
            "put": PutRemoteApiProxy,
            "patch": PatchRemoteApiProxy,
            "head": HeadRemoteApiProxy,
            "options": OptionsRemoteApiProxy
        }.get(proxy.method, None)

        if not proxy_class:
            return

        proxy_route = proxy_class(
            hass,
            proxy
        )

        hass.data[DOMAIN][proxy.method][proxy.route] = proxy_route
        if not proxy.route.startswith(ROUTE_PREFIX_SERVICE_CALL):
            for resource in [resource for resource in hass.http.app.router._resources if
                             resource.canonical == proxy.route]:
                hass.http.app.router._resources.remove(resource)
        hass.http.register_view(proxy_route)


# pylint: disable=too-many-arguments
class ProxyData:
    """Container for proxy data."""

    def __init__(self, session, method, host, port, secure, access_token, password, route):
        self.session = session
        self.method = method
        self.host = host
        self.port = port
        self.secure = secure
        self.access_token = access_token
        self.password = password
        self.auth_required = access_token or password
        self.route = route

    def get_url(self, path):
        """Get route to connect to."""
        return '%s://%s:%s%s' % (
            'https' if self.secure else 'http', self.host, self.port, path)

    def get_auth_header(self):
        """Get the authentication header."""
        if self.access_token:
            return HEADER_KEY_AUTHORIZATION, 'Bearer %s' % self.access_token

        return HEADER_KEY_PASSWORD, self.password

    async def perform_proxy(self, request):
        """Forward request to the remote instance."""
        headers = {}
        proxy_url = self.get_url(request.path)

        if self.auth_required:
            auth_header_key, auth_header_value = self.get_auth_header()
            headers[auth_header_key] = auth_header_value

        request_method = getattr(self.session, self.method, None)
        if not request_method:
            _LOGGER.warning("Couldn't find method %s",
                            self.method)
            return Response(body="Proxy route not found", status=404)

        try:
            if self.method in HTTP_METHODS_WITH_PAYLOAD:
                result = await request_method(
                    proxy_url,
                    json=await request.json(),
                    params=request.query,
                    headers=headers
                )
            else:
                result = await request_method(
                    proxy_url,
                    params=request.query,
                    headers=headers
                )

            if result is not None:
                return await self._convert_response(result)
        except Exception as e:
            _LOGGER.error(
                "Error proxying %s %s to %s: %s",
                self.method,
                request.url,
                proxy_url,
                str(e)
            )
        return Response(body="Unable to proxy request", status=500)

    async def _convert_response(self, client_response):
        if 'json' in client_response.headers.get(hdrs.CONTENT_TYPE, '').lower():
            response_body = await client_response.read()
            try:
                data = json.loads(response_body)
                return {
                    ATTR_PROXY: self,
                    ATTR_RESPONSE: data,
                    ATTR_STATUS: client_response.status
                }
            except JSONDecodeError:
                return {
                    ATTR_PROXY: self,
                    ATTR_RESPONSE: "Unable to parse JSON",
                    ATTR_STATUS: 500
                }
        return {
            ATTR_PROXY: self,
            ATTR_RESPONSE: Response(
                body=client_response.content,
                status=client_response.status,
                headers=client_response.headers),
            ATTR_STATUS: client_response.status
        }

    def copy_with_route(self, route):
        """Creates a new ProxyData with the specified route."""
        return ProxyData(
            self.session,
            self.method,
            self.host,
            self.port,
            self.secure,
            self.access_token,
            self.password,
            route
        )

    def is_exact_match(self, method, route):
        """Checks if the method and route are an exact match."""
        return self.method == method and self.route == route

    def __eq__(self, other):
        if isinstance(other, ProxyData):
            return self.host == other.host \
                   and self.port == other.port \
                   and self.method == other.method
        return False

    def __hash__(self):
        return hash(self.__repr__())

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return "%s %s%s%s" % (self.method, self.host, self.port, self.route)


class AbstractRemoteApiProxy(HomeAssistantView):
    """A proxy for remote API calls."""

    cors_allowed = True

    def __init__(self, hass, proxy):
        """Initializing the proxy."""
        if proxy.method not in HTTP_METHODS:
            return

        self.proxies = set()
        self.requires_auth = False
        self.url = proxy.route if str(proxy.route).startswith('/') else '/%s' % proxy.route
        self.name = self.url.replace('/', ':')[1:]
        self._hass = hass
        self._method = proxy.method
        self._host = proxy.host
        self._port = proxy.port

        self._session = proxy.session
        self.add_proxy(proxy.copy_with_route(self.url))

    def add_proxy(self,
                  proxy):
        """Adds a proxy to the set."""
        self.proxies.add(proxy)

    async def perform_proxy(self, request, **kwargs):
        """Proxies the request to the remote instance."""
        route = request.url.path
        exact_match_proxies = [proxy for proxy in self.proxies if
                               proxy.is_exact_match(self._method, route)]
        if len(exact_match_proxies) != 0:
            _LOGGER.warning("Found %s proxies for %s",
                            str(exact_match_proxies),
                            route)
            results = await asyncio.gather(
                *[proxy.perform_proxy(request) for proxy in exact_match_proxies])
        else:
            _LOGGER.warning("Using %s proxies for %s",
                            str(self.proxies),
                            route)
            results = await asyncio.gather(
                *[proxy.perform_proxy(request) for proxy in self.proxies])

        for result in results:
            if result[ATTR_STATUS] == 200:
                if not route.startswith(ROUTE_PREFIX_SERVICE_CALL):
                    proxy = result[ATTR_PROXY]
                    exact_proxy = proxy.copy_with_route(route)
                    self.proxies.add(exact_proxy)
                if isinstance(result[ATTR_RESPONSE], web.StreamResponse):
                    return result[ATTR_RESPONSE]
                return self.json(result[ATTR_RESPONSE])

        return self.json_message("Unable to proxy request", 500)


class GetRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy GET requests."""

    async def get(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class PostRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy POST requests."""

    async def post(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class PutRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy PUT requests."""

    async def put(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class DeleteRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy DELETE requests."""

    async def delete(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class PatchRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy PATCH requests."""

    async def delete(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class HeadRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy HEAD requests."""

    async def head(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)


class OptionsRemoteApiProxy(AbstractRemoteApiProxy):
    """API proxy OPTIONS requests."""

    async def options(self, request, **kwargs):
        """Perform proxy."""
        return await self.perform_proxy(request, **kwargs)
