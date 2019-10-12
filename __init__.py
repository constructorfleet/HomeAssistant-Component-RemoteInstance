"""
Connect two Home Assistant instances via the Websocket API.

For more details about this component, please refer to the documentation at
https://home-assistant.io/components/remote_homeassistant/
"""

import logging
import copy
import asyncio
from builtins import str, setattr

import aiohttp
from aiohttp.web import (Response)
import urllib.parse as url_parse

import voluptuous as vol

from homeassistant.core import callback
from homeassistant.components.http import HomeAssistantView
import homeassistant.components.websocket_api.auth as api
from homeassistant.components.zwave.const import EVENT_NODE_EVENT
from homeassistant.core import EventOrigin, split_entity_id
from homeassistant.const import (CONF_HOST, CONF_PORT, EVENT_CALL_SERVICE,
                                 EVENT_HOMEASSISTANT_STOP,
                                 EVENT_STATE_CHANGED, EVENT_SERVICE_REGISTERED)
from homeassistant.config import DATA_CUSTOMIZE
from homeassistant.helpers.aiohttp_client import async_get_clientsession
import homeassistant.helpers.config_validation as cv

_LOGGER = logging.getLogger(__name__)

ATTR_ROUTE = 'route'
ATTR_METHOD = 'method'
ATTR_AUTH_REQUIRED = 'auth_required'
ATTR_TYPE = 'type'
ATTR_DATA = 'data'
ATTR_EVENT = 'event'
ATTR_SERVICE_DATA = 'service_data'
ATTR_ENTITY_ID = ''

CONF_NAME = 'name'
CONF_INSTANCES = 'instances'
CONF_SECURE = 'secure'
CONF_ACCESS_TOKEN = 'access_token'
CONF_API_PASSWORD = 'api_password'
CONF_SUBSCRIBE_EVENTS = 'subscribe_events'
CONF_ENTITY_PREFIX = 'entity_prefix'
CONF_VERIFY_SSL = 'verify_ssl'

DOMAIN = 'remote_homeassistant'

EVENT_ROUTE_REGISTERED = 'route_registered'

DEFAULT_SUBSCRIBED_EVENTS = [EVENT_STATE_CHANGED,
                             EVENT_SERVICE_REGISTERED]
DEFAULT_ENTITY_PREFIX = ''
DEFAULT_ROUTE_EVENT_TYPE = 'route_registered'
DEFAULT_ROUTE_ATTR = 'route'

INSTANCES_SCHEMA = vol.Schema({
    vol.Required(CONF_NAME): cv.string,
    vol.Required(CONF_HOST): cv.string,
    vol.Optional(CONF_PORT, default=8123): cv.port,
    vol.Optional(CONF_SECURE, default=False): cv.boolean,
    vol.Exclusive(CONF_ACCESS_TOKEN, 'auth'): cv.string,
    vol.Exclusive(CONF_API_PASSWORD, 'auth'): cv.string,
    vol.Optional(CONF_SUBSCRIBE_EVENTS,
                 default=DEFAULT_SUBSCRIBED_EVENTS): vol.All(
        cv.ensure_list,
        [cv.slug]
    ),
    vol.Optional(CONF_ENTITY_PREFIX, default=DEFAULT_ENTITY_PREFIX): cv.string,
})

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required(CONF_INSTANCES): vol.All(cv.ensure_list,
                                              [INSTANCES_SCHEMA]),
    }),
}, extra=vol.ALLOW_EXTRA)

ATTR_NODE_ID = 'node_id'
PROXY_URI = 'uri'
PROXY_QUERY = 'query'

HEADER_PASSWORD = 'X-HA-access'
HEADER_AUTHORIZATION = 'Authorization'
HEADER_AUTHORIZATION_FORMAT = 'Bearer %S'

FILTER_HEADERS = [
    HEADER_PASSWORD,
    HEADER_PASSWORD.lower(),
    HEADER_AUTHORIZATION,
    HEADER_AUTHORIZATION.lower()
]

HTTP_METHODS_WITH_BODY = [
    'post',
    'put',
    'patch',
]

NOT_FOUND_RESPONSE = Response(status=404)
SERVER_ERROR_RESPONSE = Response(status=500)

SESSION = None


def _build_base_url(schema, secure, host, port):
    """Build url to connect to."""
    return '%s://%s:%s' % (
        '{}s'.format(schema) if secure else schema, host, port)


def _get_remote_endpoint(request, base_url):
    route = request.query.path
    if not route:
        return None
    return '%s/%s' % (
        base_url,
        route
    )


def _get_forward_query_params(query_params):
    forward_params = copy.deepcopy(query_params)
    forward_params.pop(PROXY_URI, None)
    return forward_params


def _get_forward_headers(
        headers,
        access_token,
        password
):
    forward_headers = {}

    for header_key in headers.keys():
        if header_key in FILTER_HEADERS:
            continue
        forward_headers[header_key] = headers.get(header_key, None)

    if access_token:
        forward_headers[HEADER_AUTHORIZATION] = HEADER_AUTHORIZATION_FORMAT % access_token
    else:
        forward_headers[HEADER_PASSWORD] = password
    return forward_headers


async def async_setup(hass, config):
    """Set up the remote_homeassistant component."""
    conf = config.get(DOMAIN)
    for instance in conf.get(CONF_INSTANCES):
        connection = RemoteConnection(hass, instance)
        asyncio.ensure_future(connection.async_connect())

    return True


class RemoteInstanceView(HomeAssistantView):
    """Forward http requests to remote instance."""

    def __init__(self, host, port, secure, access_token, password, route, method, auth_required):
        """Initialize the remote instance proxy view."""
        self.requires_auth = auth_required
        self._host = host
        self._port = port
        self._secure = secure
        self._access_token = access_token
        self._password = password
        self._session = SESSION
        self._remote_url = _build_base_url(
            'http',
            self._secure,
            self._host,
            self._port
        )
        self.url = route
        self.name = str(route).replace('/', ':')

        def get_request_forward_handler(http_method):
            async def forward_request(_, request):
                return await self._forward_request(http_method, request)

            return forward_request

        setattr(self, method, get_request_forward_handler(method))

    async def _forward_request(
            self,
            method,
            request
    ):
        url = _get_remote_endpoint(
            request,
            self._remote_url
        )
        if url is None:
            return NOT_FOUND_RESPONSE

        query = _get_forward_query_params(request.query)
        headers = _get_forward_headers(
            request.headers,
            self._access_token,
            self._password
        )

        try:
            http_func = getattr(self._session, method)
            if method in HTTP_METHODS_WITH_BODY:
                async with http_func(
                        url,
                        params=query,
                        headers=headers
                ) as response:
                    return await response

            else:
                async with http_func(
                        url,
                        params=query,
                        data=request,
                        headers=headers
                ) as response:
                    return await response
        except:
            return SERVER_ERROR_RESPONSE


class RemoteConnection(object):
    """A Websocket connection to a remote home-assistant instance."""

    def __init__(self, hass, conf):
        """Initialize the connection."""
        self._hass = hass
        self._name = conf.get(CONF_NAME)
        self._host = conf.get(CONF_HOST)
        self._port = conf.get(CONF_PORT)
        self._secure = conf.get(CONF_SECURE)
        self._verify_ssl = conf.get(CONF_VERIFY_SSL)
        self._access_token = conf.get(CONF_ACCESS_TOKEN)
        self._password = conf.get(CONF_API_PASSWORD)
        self._subscribe_events = conf.get(CONF_SUBSCRIBE_EVENTS)
        self._entity_prefix = conf.get(CONF_ENTITY_PREFIX)

        self._connection = None
        self._entities = set()
        self._handlers = {}
        self._remove_listener = None

        self.__id = 1

    @callback
    def _get_url(self):
        """Get url to connect to."""
        return '{}/api/websocket'.format(
            _build_base_url(
                'ws',
                self._secure,
                self._host,
                self._port
            )
        )

    async def async_connect(self):
        """Connect to remote home-assistant websocket..."""
        url = self._get_url()

        session = async_get_clientsession(self._hass, self._verify_ssl)

        while True:
            try:
                _LOGGER.info('Connecting to %s', url)
                self._connection = await session.ws_connect(url)
            except aiohttp.ClientError as err:
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

        await asyncio.ensure_future(self._recv())

    def _next_id(self):
        _id = self.__id
        self.__id += 1
        return _id

    async def _call(self, callback, message_type, **extra_args):
        _id = self._next_id()
        self._handlers[_id] = callback
        try:
            await self._connection.send_json(
                {'id': _id, ATTR_TYPE: message_type, **extra_args})
        except aiohttp.ClientError as err:
            _LOGGER.error(
                'remote websocket to %s connection closed: %s',
                self._name,
                err)
            await self._disconnected()

    async def _disconnected(self):
        # Remove all published entries
        for entity in self._entities:
            self._hass.states.async_remove(entity)
        if self._remove_listener is not None:
            self._remove_listener()
        self._remove_listener = None
        self._entities = set()
        await asyncio.ensure_future(self.async_connect())

    async def _recv(self):
        while not self._connection.closed:
            try:
                data = await self._connection.receive()
            except aiohttp.ClientError as err:
                _LOGGER.error(
                'remote websocket to %s connection closed: %s',
                self._name,
                err)
                break

            if not data:
                break

            if data.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                _LOGGER.error('websocket connection is closing')
                break

            if data.type == aiohttp.WSMsgType.ERROR:
                _LOGGER.error('websocket connection had an error %s', self._name)
                break

            try:
                message = data.json()
            except TypeError as err:
                _LOGGER.error('could not decode data (%s) as json: %s', data, err)
                break

            if message is None:
                break

            _LOGGER.debug('received: %s', message)

            if message[ATTR_TYPE] == api.TYPE_AUTH_OK:
                await self._init()




            elif message[ATTR_TYPE] == api.TYPE_AUTH_REQUIRED:
                if not (self._access_token or self._password):
                    _LOGGER.error('Access token or api password required, but not provided')
                    return
                if self._access_token:
                    data = {ATTR_TYPE: api.TYPE_AUTH, 'access_token': self._access_token}
                else:
                    data = {ATTR_TYPE: api.TYPE_AUTH, 'api_password': self._password}
                try:
                    await self._connection.send_json(data)
                except Exception as err:
                    _LOGGER.error('could not send data to remote connection: %s', err)
                    break

            elif message[ATTR_TYPE] == api.TYPE_AUTH_INVALID:
                _LOGGER.error(
                    'Auth invalid for %s, check your access token or API password',
                    self._name)
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
            service_data = event_data[ATTR_SERVICE_DATA]

            if not service_data:
                return

            entity_ids = service_data.get(ATTR_ENTITY_ID, None)

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
            if ATTR_NODE_ID in event_data:
                event_data[ATTR_NODE_ID] = self._unprefix_value(event_data[ATTR_NODE_ID])
            if ATTR_NODE_ID in event_data[ATTR_SERVICE_DATA]:
                event_data[ATTR_SERVICE_DATA][ATTR_NODE_ID] = self._unprefix_value(
                    event_data[ATTR_SERVICE_DATA][ATTR_NODE_ID])
            event_data[ATTR_SERVICE_DATA][ATTR_ENTITY_ID] = list(entity_ids)

            # Remove service_call_id parameter - websocket API
            # doesn't accept that one
            event_data.pop('service_call_id', None)

            _id = self._next_id()
            data = {
                'id': _id,
                ATTR_TYPE: event.event_type,
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
            if self._entity_prefix:
                domain, object_id = split_entity_id(entity_id)
                object_id = self._entity_prefix + object_id
                entity_id = domain + '.' + object_id

            # Add local customization data
            if DATA_CUSTOMIZE in self._hass.data:
                attr.update(self._hass.data[DATA_CUSTOMIZE].get(entity_id))

            if ATTR_NODE_ID in attr:
                attr[ATTR_NODE_ID] = self._prefix_value(attr[ATTR_NODE_ID])

            self._entities.add(entity_id)
            self._hass.states.async_set(entity_id, state, attr)

        def fire_event(message):
            """Publish remove event on local instance."""
            if message[ATTR_TYPE] == 'result':
                return

            if message[ATTR_TYPE] != ATTR_EVENT:
                return

            if message[ATTR_EVENT]['event_type'] == 'state_changed':
                entity_id = message[ATTR_EVENT][ATTR_DATA][ATTR_ENTITY_ID]
                state = message[ATTR_EVENT][ATTR_DATA]['new_state']['state']
                attr = message[ATTR_EVENT][ATTR_DATA]['new_state']['attributes']
                if ATTR_NODE_ID in attr:
                    attr[ATTR_NODE_ID] = self._prefix_value(attr[ATTR_NODE_ID])

                state_changed(entity_id, state, attr)
            elif message[ATTR_EVENT]['event_type'] == EVENT_ROUTE_REGISTERED:
                route = message[ATTR_EVENT][ATTR_DATA][ATTR_ROUTE]
                method = message[ATTR_EVENT][ATTR_DATA][ATTR_METHOD]
                auth_required = message[ATTR_EVENT][ATTR_DATA][ATTR_AUTH_REQUIRED]
                RemoteInstanceView(
                    self._host,
                    self._port,
                    self._secure,
                    self._access_token,
                    self._password,
                    route,
                    method,
                    auth_required
                )
            else:
                event = message[ATTR_EVENT]
                if event['event_type'] == EVENT_NODE_EVENT and ATTR_NODE_ID in event[ATTR_DATA]:
                    event[ATTR_DATA][ATTR_NODE_ID] = self._prefix_value(event[ATTR_DATA][ATTR_NODE_ID])
                self._hass.bus.async_fire(
                    event_type=event['event_type'],
                    event_data=event[ATTR_DATA],
                    origin=EventOrigin.remote
                )

        def got_states(message):
            """Called when list of remote states is available."""
            for entity in message['result']:
                entity_id = entity[ATTR_ENTITY_ID]
                state = entity['state']
                attributes = entity['attributes']

                if ATTR_NODE_ID in attributes:
                    attributes[ATTR_NODE_ID] = self._prefix_value(attributes[ATTR_NODE_ID])

                state_changed(entity_id, state, attributes)

        self._remove_listener = self._hass.bus.async_listen(EVENT_CALL_SERVICE, forward_event)

        for event in self._subscribe_events:
            await self._call(fire_event, 'subscribe_events', event_type=event)

        await self._call(got_states, 'get_states')

    def _prefix_value(self, value):
        if str(value).startswith(self._entity_prefix):
            return value
        return "{}_{}".format(self._entity_prefix, value)

    def _unprefix_value(self, value):
        if isinstance(value, str) and str(value).startswith(self._entity_prefix):
            return str(value).replace("{}_".format(self._entity_prefix), '')

        return value
