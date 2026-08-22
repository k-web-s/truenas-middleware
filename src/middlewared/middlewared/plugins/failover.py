# This is a stub of the proprietary `failover` service which lived in the
# TrueNAS Enterprise middleware tree. This build assumes a FreeBSD,
# unlicensed, non-HA system, so every failover-related call returns the
# neutral/disabled value that a standalone (CORE) node would report.

from middlewared.service import CallError, Service, private


class FailoverService(Service):

    class Config:
        namespace = 'failover'
        private = True

    @private
    async def licensed(self):
        return False

    @private
    async def hardware(self):
        return 'MANUAL'

    @private
    async def node(self):
        return 'MANUAL'

    @private
    async def status(self):
        return 'SINGLE'

    @private
    async def internal_interfaces(self):
        return []

    @private
    async def in_progress(self):
        return False

    @private
    async def disabled_reasons(self):
        return []

    @private
    async def is_single_master_node(self):
        return False

    @private
    async def remote_ip(self):
        return None

    @private
    async def config(self):
        return {
            'disabled': True,
            'master_node': 'A',
            'timeout': 0,
        }

    @private
    async def send_small_file(self, path):
        return

    @private
    async def send_database(self):
        return

    @private
    async def call_remote(self, *args, **kwargs):
        raise CallError('HA/failover is not supported on this system')


class FailoverVipService(Service):

    class Config:
        namespace = 'failover.vip'
        private = True

    @private
    async def get_states(self):
        return [False]
