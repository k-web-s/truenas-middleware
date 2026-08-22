# This is a stub of the proprietary `truenas` service which lived in the
# TrueNAS Enterprise middleware tree. This build assumes a FreeBSD,
# unlicensed, non-HA system, so chassis hardware detection reports the
# generic "unknown" value.

from middlewared.service import Service, private


class TruenasService(Service):

    class Config:
        namespace = 'truenas'
        private = True

    @private
    async def get_chassis_hardware(self):
        return 'TRUENAS-UNKNOWN'
