import asyncio
import logging

from asynctest import CoroutineMock, Mock

from middlewared.utils import filter_list
from middlewared.schema import Schemas, resolve_methods


class Middleware(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self['failover.licensed'] = Mock(return_value=False)
        self['failover.hardware'] = Mock(return_value='MANUAL')
        self['failover.node'] = Mock(return_value='MANUAL')
        self['failover.status'] = Mock(return_value='SINGLE')
        self['failover.internal_interfaces'] = Mock(return_value=[])
        self['failover.is_single_master_node'] = Mock(return_value=False)
        self['failover.disabled_reasons'] = Mock(return_value=[])
        self['failover.call_remote'] = Mock()
        self['failover.config'] = Mock(return_value={'disabled': True, 'master_node': 'A', 'timeout': 0})
        self['truenas.get_chassis_hardware'] = Mock(return_value='TRUENAS-UNKNOWN')
        self['system.is_freenas'] = Mock(return_value=True)
        self['system.is_enterprise'] = Mock(return_value=False)
        self['system.product_type'] = Mock(return_value='CORE')
        self.__schemas = Schemas()

        self.call_hook = CoroutineMock()
        self.call_hook_inline = Mock()
        self.event_register = Mock()
        self.send_event = Mock()

        self.logger = logging.getLogger("middlewared")

    async def _call(self, name, serviceobj, method, args, app=None):
        to_resolve = [getattr(serviceobj, attr) for attr in dir(serviceobj) if attr != 'query']
        resolve_methods(self.__schemas, to_resolve)
        return await method(*args)

    async def call(self, name, *args):
        result = self[name](*args)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    def call_sync(self, name, *args):
        return self[name](*args)

    async def run_in_executor(self, executor, method, *args, **kwargs):
        return method(*args, **kwargs)

    async def run_in_thread(self, method, *args, **kwargs):
        return method(*args, **kwargs)

    def _query_filter(self, l):
        def query(filters=None, options=None):
            return filter_list(l, filters, options)
        return query
