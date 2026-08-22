# This is a stub of the proprietary `fcport` service which lived in the
# TrueNAS Enterprise middleware tree. This build assumes a FreeBSD,
# unlicensed, non-HA system with no Fibre Channel ports, so no port
# usages can exist.

from middlewared.service import Service, private


class FcportService(Service):

    class Config:
        namespace = 'fcport'
        private = True

    @private
    async def query(self, *args, **kwargs):
        return []
