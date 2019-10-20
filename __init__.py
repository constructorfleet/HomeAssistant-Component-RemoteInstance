"""
Connect two Home Assistant instances via the Websocket API.

For more details about this component, please refer to the documentation at
https://home-assistant.io/components/remote_homeassistant/
"""

import asyncio
import copy
import logging

import aiohttp
import homeassistant.components.websocket_api.auth as api
import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from aiohttp import ClientError
from aiohttp.web import HTTPFound
from homeassistant.components.http import HomeAssistantView
from homeassistant.config import DATA_CUSTOMIZE
from homeassistant.const import (CONF_HOST, CONF_PORT)
from homeassistant.const import (EVENT_HOMEASSISTANT_STOP, EVENT_SERVICE_REGISTERED, ATTR_DOMAIN, ATTR_SERVICE,
                                 EVENT_CALL_SERVICE, ATTR_ENTITY_PICTURE, EVENT_STATE_CHANGED)
from homeassistant.core import EventOrigin, split_entity_id
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
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

INSTANCES_SCHEMA = vol.Schema({
    vol.Required(CONF_HOST): cv.string,
    vol.Optional(CONF_PORT, default=8123): cv.port,
    vol.Optional(CONF_SECURE, default=False): cv.boolean,
    vol.Optional(CONF_VERIFY_SSL, default=True): cv.boolean,
    vol.Exclusive(CONF_TOKEN, 'auth'): cv.string,
    vol.Exclusive(CONF_PASSWORD, 'auth'): cv.string,
    vol.Optional(CONF_SUBSCRIBE_TO,
                 default=DEFAULT_SUBSCRIBED_EVENTS): cv.ensure_list,
    vol.Optional(CONF_ENTITY_PREFIX, default=DEFAULT_ENTITY_PREFIX): cv.string,
    vol.Optional(CONF_PROXY_API, default=False): cv.boolean
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

    for instance in conf.get(CONF_INSTANCES):
        connection = RemoteInstance(hass, instance)
        asyncio.ensure_future(connection.async_connect())

    return True


class RemoteInstance(object):
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

        self._connection = None
        self._entities = set()
        self._handlers = {}
        self._remove_listener = None

        self._session = aiohttp.ClientSession() if EVENT_ROUTE_REGISTERED in self._subscribe_to else None

        self.__id = 1

    @callback
    def _get_url(self, scheme, route):
        """Get url to connect to."""
        return '%s://%s:%s%s' % (
            '%ss' % scheme if self._secure else scheme, self._host, self._port, route if route else "")

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
                    'Could not connect to %s, retry in 10 seconds...', url)
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

    async def _call(self, callback, message_type, **extra_args):
        _id = self._next_id()
        self._handlers[_id] = callback
        try:
            _LOGGER.warning("Sending %s" % message_type)
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
        while not self._connection.closed:
            try:
                data = await self._connection.receive()
            except ClientError as err:
                _LOGGER.error('remote websocket connection closed: %s', err)
                break

            if not data:
                break

            if data.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                _LOGGER.error('websocket connection is closing')
                break

            if data.type == aiohttp.WSMsgType.ERROR:
                _LOGGER.error('websocket connection had an error')
                break

            try:
                message = data.json()
            except TypeError as err:
                _LOGGER.error('could not decode data (%s) as json: %s', data, err)
                break

            if message is None:
                break

            if message['type'] == api.TYPE_AUTH_OK:
                await self._init()

            elif message['type'] == api.TYPE_AUTH_REQUIRED:
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

            elif message['type'] == api.TYPE_AUTH_INVALID:
                _LOGGER.error('Auth invalid, check your access token or API password')
                await self._connection.close()
                return

            else:
                handler = self._handlers.get(message['id'])
                if handler is not None:
                    handler(message)

        await self._disconnected()

    async def _init(self):
        async def forward_event(event):
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

            _LOGGER.debug('forward event: %s', data)

            try:
                await self._connection.send_json(data)
            except Exception as err:
                _LOGGER.error('could not send data to remote connection: %s', err)
                await self._disconnected()

        def state_changed(entity_id, state, attr):
            """Publish remote state change on local instance."""
            if ATTR_ENTITY_PICTURE in attr:
                route = attr[ATTR_ROUTE].split('?')[0]
                method = 'get'
                auth_required = self._token or self._password
                register_proxy(
                    self._hass,
                    self._session,
                    self._host,
                    self._port,
                    self._secure,
                    self._token,
                    self._password,
                    route,
                    method,
                    auth_required)
            if self._entity_prefix:
                domain, object_id = split_entity_id(entity_id)
                object_id = self._entity_prefix + object_id
                entity_id = domain + '.' + object_id

            # Add local customization data
            if DATA_CUSTOMIZE in self._hass.data:
                attr.update(self._hass.data[DATA_CUSTOMIZE].get(entity_id))

            self._entities.add(entity_id)
            self._hass.states.async_set(entity_id, state, attr)

        def fire_event(message):
            """Publish remove event on local instance."""
            if message['type'] == 'result':
                return

            if message['type'] != 'event':
                return

            _LOGGER.warning("RECEIVED MESSAGE %s" % str(message))

            if message['event']['event_type'] == EVENT_STATE_CHANGED:
                entity_id = message['event']['data']['entity_id']
                state = message['event']['data']['new_state']['state']
                attr = message['event']['data']['new_state']['attributes']
                state_changed(entity_id, state, attr)
            elif message['event']['event_type'] == EVENT_ROUTE_REGISTERED:
                data = message['event']['data']
                route = str(data[ATTR_ROUTE]).split('?')[0]
                method = data[ATTR_METHOD]
                auth_required = data[ATTR_AUTH_REQUIRED]
                register_proxy(
                    self._hass,
                    self._session,
                    self._host,
                    self._port,
                    self._secure,
                    self._token,
                    self._password,
                    route,
                    method,
                    auth_required)
            else:
                event = message['event']
                self._hass.bus.async_fire(
                    event_type=event['event_type'],
                    event_data=event['data'],
                    origin=EventOrigin.remote
                )

        def got_states(message):
            """Called when list of remote states is available."""
            for entity in message['result']:
                entity_id = entity['entity_id']
                state = entity['state']
                attributes = entity['attributes']

                state_changed(entity_id, state, attributes)

        def got_services(message):
            for domain in message['result']:
                for service in domain:
                    self._hass.bus.async_fire(
                        event_type=EVENT_SERVICE_REGISTERED,
                        event_data={ATTR_DOMAIN: domain, ATTR_SERVICE: service},
                        origin=EventOrigin.remote
                    )

        self._remove_listener = self._hass.bus.async_listen(EVENT_CALL_SERVICE, forward_event)

        for event_type in self._subscribe_to:
            await self._call(fire_event, 'subscribe_events', event_type=event_type)

        if EVENT_SERVICE_REGISTERED in self._subscribe_to:
            await self._call(got_services, 'get_services')

        if EVENT_ROUTE_REGISTERED in self._subscribe_to:
            await self._call(fire_event, EVENT_TYPE_REQUEST_ROUTES)

        await self._call(got_states, 'get_states')


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
