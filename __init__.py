"""
Connect two Home Assistant instances via the Websocket API.

For more details about this component, please refer to the documentation at
https://home-assistant.io/components/remote_homeassistant/
"""

import logging
import copy
import asyncio
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

CONF_NAME = 'name'
CONF_INSTANCES = 'instances'
CONF_SECURE = 'secure'
CONF_ACCESS_TOKEN = 'access_token'
CONF_API_PASSWORD = 'api_password'
CONF_SUBSCRIBE_EVENTS = 'subscribe_events'
CONF_ENTITY_PREFIX = 'entity_prefix'

DOMAIN = 'remote_homeassistant'

DEFAULT_SUBSCRIBED_EVENTS = [EVENT_STATE_CHANGED,
                             EVENT_SERVICE_REGISTERED]
DEFAULT_ENTITY_PREFIX = ''

INSTANCES_SCHEMA = vol.Schema({
    vol.Required(CONF_NAME): cv.string,
    vol.Required(CONF_HOST): cv.string,
    vol.Optional(CONF_PORT, default=8123): cv.port,
    vol.Optional(CONF_SECURE, default=False): cv.boolean,
    vol.Exclusive(CONF_ACCESS_TOKEN, 'auth'): cv.string,
    vol.Exclusive(CONF_API_PASSWORD, 'auth'): cv.string,
    vol.Optional(CONF_SUBSCRIBE_EVENTS,
                 default=DEFAULT_SUBSCRIBED_EVENTS): cv.ensure_list,
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


def _build_base_url(schema, secure, host, port):
    """Build url to connect to."""
    return '%s://%s:%s/' % (
        '{}s'.format(schema) if secure else schema, host, port)


def _get_remote_endpoint(request, base_url):
    encoded_proxy_uri = request.query.get
    if not encoded_proxy_uri:
        return None
    return '%s%s' % (
        base_url,
        url_parse.unquote(
            encoded_proxy_uri,
            encoding='utf-8'
        )
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
    session = aiohttp.ClientSession()

    for instance in conf.get(CONF_INSTANCES):
        connection = RemoteConnection(hass, instance)
        try:
            await asyncio.ensure_future(connection.async_connect())
            hass.http.register_view(RemoteInstanceView(instance, session))
        except:
            continue

    return True


class RemoteInstanceView(HomeAssistantView):
    """Forward http requests to remote instance."""

    requires_auth = False

    def __init__(self, conf, session):
        """Initialize the remote instance proxy view."""
        self._instance_name = conf.get(CONF_NAME)
        self._host = conf.get(CONF_HOST)
        self._port = conf.get(CONF_PORT)
        self._secure = conf.get(CONF_SECURE)
        self._access_token = conf.get(CONF_ACCESS_TOKEN)
        self._password = conf.get(CONF_API_PASSWORD)
        self._session = session
        self._remote_url = _build_base_url(
            'http',
            self._secure,
            self._host,
            self._port
        )
        self.url = '/api/remote_instance_proxy/%s' % self._instance_name
        self.name = 'api:remote_instance_proxy:%s' % self._instance_name

    async def get(self, request):
        """Forward the GET request to remote instance."""
        return await self._forward_request('get', request)

    async def post(self, request):
        """Forward the POST request to remote instance."""
        return await self._forward_request('post', request)

    async def delete(self, request):
        """Forward the DELETE request to remote instance."""
        return await self._forward_request('delete', request)

    async def put(self, request):
        """Forward the PUT request to remote instance."""
        return await self._forward_request('put', request)

    async def patch(self, request):
        """Forward the PATCH request to remote instance."""
        return await self._forward_request('patch', request)

    async def head(self, request):
        """Forward the HEAD request to remote instance."""
        return await self._forward_request('head', request)

    async def options(self, request):
        """Forward the OPTIONS request to remote instance."""
        return await self._forward_request('options', request)

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
        self._host = conf.get(CONF_HOST)
        self._port = conf.get(CONF_PORT)
        self._secure = conf.get(CONF_SECURE)
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

        session = async_get_clientsession(self._hass)

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
                {'id': _id, 'type': message_type, **extra_args})
        except aiohttp.ClientError as err:
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
        await asyncio.ensure_future(self.async_connect())

    async def _recv(self):
        while not self._connection.closed:
            try:
                data = await self._connection.receive()
            except aiohttp.ClientError as err:
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

            _LOGGER.debug('received: %s', message)

            if message['type'] == api.TYPE_AUTH_OK:
                await self._init()

            elif message['type'] == api.TYPE_AUTH_REQUIRED:
                if not (self._access_token or self._password):
                    _LOGGER.error('Access token or api password required, but not provided')
                    return
                if self._access_token:
                    data = {'type': api.TYPE_AUTH, 'access_token': self._access_token}
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
            if ATTR_NODE_ID in event_data:
                event_data[ATTR_NODE_ID] = self._unprefix_value(event_data[ATTR_NODE_ID])
            if ATTR_NODE_ID in event_data['service_data']:
                event_data['service_data'][ATTR_NODE_ID] = self._unprefix_value(
                    event_data['service_data'][ATTR_NODE_ID])
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
            if message['type'] == 'result':
                return

            if message['type'] != 'event':
                return

            if message['event']['event_type'] == 'state_changed':
                entity_id = message['event']['data']['entity_id']
                state = message['event']['data']['new_state']['state']
                attr = message['event']['data']['new_state']['attributes']
                if ATTR_NODE_ID in attr:
                    attr[ATTR_NODE_ID] = self._prefix_value(attr[ATTR_NODE_ID])

                state_changed(entity_id, state, attr)
            else:
                event = message['event']
                if event['event_type'] == EVENT_NODE_EVENT and ATTR_NODE_ID in event['data']:
                    event['data'][ATTR_NODE_ID] = self._prefix_value(event['data'][ATTR_NODE_ID])
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
