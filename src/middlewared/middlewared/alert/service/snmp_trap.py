from pysnmp.hlapi import asyncio as hlapi
from pysnmp.smi import builder, view

from middlewared.alert.base import AlertService
from middlewared.schema import Bool, Dict, Int, Str


class SNMPTrapAlertService(AlertService):
    title = "SNMP Trap"

    schema = Dict(
        "snmp_attributes",
        Str("host", required=True),
        Int("port", required=True),
        Bool("v3", required=True),
        # v1/v2
        Str("community", null=True, default=None, empty=False),
        # v3
        Str("v3_username", null=True, default=None, empty=False),
        Str("v3_authkey", null=True, default=None),
        Str("v3_privkey", null=True, default=None),
        Str("v3_authprotocol", enum=[None, "MD5", "SHA", "128SHA224", "192SHA256", "256SHA384", "384SHA512"],
            null=True, default=None),
        Str("v3_privprotocol", enum=[None, "DES", "3DESEDE", "AESCFB128", "AESCFB192", "AESCFB256",
                                     "AESBLUMENTHALCFB192", "AESBLUMENTHALCFB256"],
            null=True, default=None),
        strict=True,
    )

    def __init__(self, middleware, attributes):
        super().__init__(middleware, attributes)

        self.initialized = False

    def _init_sync(self):
        if self.attributes["v3"]:
            self.auth_data = hlapi.UsmUserData(
                self.attributes["v3_username"] or "",
                self.attributes["v3_authkey"],
                self.attributes["v3_privkey"],
                {
                    None: hlapi.USM_AUTH_NONE,
                    "MD5": hlapi.USM_AUTH_HMAC96_MD5,
                    "SHA": hlapi.USM_AUTH_HMAC96_SHA,
                    "128SHA224": hlapi.USM_AUTH_HMAC128_SHA224,
                    "192SHA256": hlapi.USM_AUTH_HMAC192_SHA256,
                    "256SHA384": hlapi.USM_AUTH_HMAC256_SHA384,
                    "384SHA512": hlapi.USM_AUTH_HMAC384_SHA512,
                }[self.attributes["v3_authprotocol"]],
                {
                    None: hlapi.USM_PRIV_NONE,
                    "DES": hlapi.USM_PRIV_CBC56_DES,
                    "3DESEDE": hlapi.USM_PRIV_CBC168_3DES,
                    "AESCFB128": hlapi.USM_PRIV_CFB128_AES,
                    "AESCFB192": hlapi.USM_PRIV_CFB192_AES,
                    "AESCFB256": hlapi.USM_PRIV_CFB256_AES,
                    "AESBLUMENTHALCFB192": hlapi.USM_PRIV_CFB192_AES_BLUMENTHAL,
                    "AESBLUMENTHALCFB256": hlapi.USM_PRIV_CFB256_AES_BLUMENTHAL,
                }[self.attributes["v3_privprotocol"]],
            )
        else:
            self.auth_data = hlapi.CommunityData(self.attributes["community"])
        self.context_data = hlapi.ContextData()

        mib_builder = builder.MibBuilder()
        mib_sources = mib_builder.get_mib_sources() + (
            builder.DirMibSource("/usr/local/share/pysnmp/mibs"),)
        mib_builder.set_mib_sources(*mib_sources)
        mib_builder.load_modules("FREENAS-MIB")
        self.snmp_alert_level_type = mib_builder.import_symbols("FREENAS-MIB", "AlertLevelType")[0]
        self.mib_view_controller = view.MibViewController(mib_builder)
        self.snmp_alert = hlapi.ObjectIdentity("FREENAS-MIB", "alert"). \
            resolve_with_mib(self.mib_view_controller)
        self.snmp_alert_id = hlapi.ObjectIdentity("FREENAS-MIB", "alertId"). \
            resolve_with_mib(self.mib_view_controller)
        self.snmp_alert_level = hlapi.ObjectIdentity("FREENAS-MIB", "alertLevel"). \
            resolve_with_mib(self.mib_view_controller)
        self.snmp_alert_message = hlapi.ObjectIdentity("FREENAS-MIB", "alertMessage"). \
            resolve_with_mib(self.mib_view_controller)
        self.snmp_alert_cancellation = hlapi.ObjectIdentity("FREENAS-MIB", "alertCancellation"). \
            resolve_with_mib(self.mib_view_controller)

    async def send(self, alerts, gone_alerts, new_alerts):
        if self.attributes["host"] in ("localhost", "127.0.0.1", "::1"):
            if not await self.middleware.call("service.started", "snmp"):
                self.logger.trace("Local SNMP service not started, not sending traps")
                return

        if not self.initialized:
            await self.middleware.run_in_thread(self._init_sync)
            self.initialized = True

        classes = (await self.middleware.call("alertclasses.config"))["classes"]

        with hlapi.SnmpEngine() as snmp_engine:
            transport_target = await hlapi.UdpTransportTarget.create(
                (self.attributes["host"], self.attributes["port"]))

            for alert in gone_alerts:
                error_indication, error_status, error_index, var_binds = await hlapi.send_notification(
                    snmp_engine,
                    self.auth_data,
                    transport_target,
                    self.context_data,
                    "trap",
                    hlapi.NotificationType(self.snmp_alert_cancellation).add_varbinds(
                        (hlapi.ObjectIdentifier(self.snmp_alert_id),
                         hlapi.OctetString(alert.uuid))
                    ).resolve_with_mib(self.mib_view_controller)
                )

                if error_indication:
                    self.logger.error("Failed to send SNMP trap: %s", error_indication)

            for alert in new_alerts:
                error_indication, error_status, error_index, var_binds = await hlapi.send_notification(
                    snmp_engine,
                    self.auth_data,
                    transport_target,
                    self.context_data,
                    "trap",
                    hlapi.NotificationType(self.snmp_alert).add_varbinds(
                        (hlapi.ObjectIdentifier(self.snmp_alert_id),
                         hlapi.OctetString(alert.uuid)),
                        (hlapi.ObjectIdentifier(self.snmp_alert_level),
                         self.snmp_alert_level_type(
                             self.snmp_alert_level_type.namedValues.getValue(
                                 classes.get(alert.klass.name, {}).get("level", alert.klass.level.name).lower()))),
                        (hlapi.ObjectIdentifier(self.snmp_alert_message),
                         hlapi.OctetString(alert.formatted))
                    ).resolve_with_mib(self.mib_view_controller)
                )

                if error_indication:
                    self.logger.warning("Failed to send SNMP trap: %s", error_indication)
