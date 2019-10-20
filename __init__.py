"""
Connect two Home Assistant instances via the Websocket API.

For more details about this component, please refer to the documentation at
https://home-assistant.io/components/remote_homeassistant/
"""

import asyncio
import logging

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.const import (CONF_HOST, CONF_PORT, EVENT_STATE_CHANGED, EVENT_SERVICE_REGISTERED)
from homeassistant.helpers.typing import HomeAssistantType, ConfigType

from remote_instance import RemoteInstance

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
DEFAULT_PROXY_API = False

EVENT_ROUTE_REGISTERED = 'route_registered'
EVENT_TYPE_REQUEST_ROUTES = 'request_routes'
ATTR_ROUTE = 'route'
ATTR_METHOD = 'method'
ATTR_AUTH_REQUIRED = 'auth_required'

HEADER_KEY_PASSWORD = 'X-HA-access'
HEADER_KEY_AUTHORIZATION = 'Authorization'

METHODS_WITH_PAYLOAD = ('post', 'put', 'patch')

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
