# This is a stub of the proprietary `enclosure` service which lived in the
# TrueNAS Enterprise middleware tree. This build assumes a FreeBSD,
# unlicensed, non-HA system with no SES enclosure management, so the
# enclosure sync operations are no-ops.

from middlewared.service import Service, private


class EnclosureService(Service):

    class Config:
        namespace = 'enclosure'
        private = True

    @private
    async def sync_disk(self, disk_identifier):
        return

    @private
    async def sync_disks(self, *args, **kwargs):
        return

    @private
    async def sync_zpool(self, pool_name):
        return
