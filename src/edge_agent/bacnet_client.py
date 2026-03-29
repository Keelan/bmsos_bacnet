"""BACpypes3 BACnet/IP client (Pass 2)."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any, Optional

from bacpypes3.apdu import AbortPDU, AbortReason, ErrorRejectAbortNack
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier

from edge_agent.models import EffectiveBacnetConfig, utc_now_iso
from edge_agent.settings import Settings

_log = logging.getLogger(__name__)


def format_bacpypes_device_address(bind_ip: str, bind_prefix: int, udp_port: int) -> str:
    """
    BACpypes3 parses bare ip:port as /32; then addrBroadcastTuple == addrTuple and
    Who-Is (LocalBroadcast) raises RuntimeError('no broadcast'). Use ip/prefix:port.
    If bind_ip already contains '/' (e.g. 192.168.1.5/24), only append :port.
    """
    ip = bind_ip.strip()
    if not ip:
        return ""
    if "/" in ip:
        return f"{ip}:{udp_port}"
    return f"{ip}/{int(bind_prefix)}:{udp_port}"


def _camel_to_kebab(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1-\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1-\2", s1).lower()


def _object_id_string(object_type: str, object_instance: int) -> str:
    return f"{_camel_to_kebab(object_type)} {object_instance}"


async def _object_identifiers(app: Application, device_address: Address, device_identifier: ObjectIdentifier):
    try:
        object_list = await app.read_property(device_address, device_identifier, "object-list")
        return list(object_list)
    except AbortPDU as err:
        if err.apduAbortRejectReason != AbortReason.segmentationNotSupported:
            _log.debug("object-list abort: %s", err)
            return []
    except ErrorRejectAbortNack as err:
        _log.debug("object-list err: %s", err)
        return []

    object_list: list[Any] = []
    try:
        object_list_length = await app.read_property(
            device_address,
            device_identifier,
            "object-list",
            array_index=0,
        )
        for i in range(int(object_list_length)):
            oid = await app.read_property(
                device_address,
                device_identifier,
                "object-list",
                array_index=i + 1,
            )
            object_list.append(oid)
    except ErrorRejectAbortNack as err:
        _log.debug("object-list indexed err: %s", err)

    return object_list


class BacnetPypesClient:
    """Wraps BACpypes3 Application; recreate via manager on config change."""

    def __init__(self, settings: Settings, effective: EffectiveBacnetConfig) -> None:
        self._settings = settings
        self._effective = effective
        self._app: Optional[Application] = None

    def _build_application(self) -> Application:
        os.environ["BACPYPES_DEVICE_INSTANCE"] = str(self._effective.device_instance)
        os.environ["BACPYPES_VENDOR_IDENTIFIER"] = "999"
        if self._effective.bind_ip:
            os.environ["BACPYPES_DEVICE_ADDRESS"] = format_bacpypes_device_address(
                self._effective.bind_ip,
                self._settings.bacnet_bind_prefix,
                self._effective.udp_port,
            )
        elif "BACPYPES_DEVICE_ADDRESS" in os.environ:
            del os.environ["BACPYPES_DEVICE_ADDRESS"]

        parser = SimpleArgumentParser()
        args = parser.parse_args([])
        app = Application.from_args(args)
        return app

    async def start(self) -> None:
        if self._app is not None:
            return
        self._app = self._build_application()
        _log.info("bacnet_stack_started device_instance=%s", self._effective.device_instance)

    async def stop(self) -> None:
        if self._app is not None:
            self._app.close()
            self._app = None
            _log.info("bacnet_stack_stopped")

    async def restart(self, effective: EffectiveBacnetConfig) -> None:
        await self.stop()
        self._effective = effective
        await self.start()

    def _require_app(self) -> Application:
        if not self._app:
            raise RuntimeError("BACnet stack not started")
        return self._app

    async def discover_network(self, who_is_timeout: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        app = self._require_app()
        errors: list[dict[str, Any]] = []
        devices: list[dict[str, Any]] = []
        now = utc_now_iso()
        try:
            fut = app.who_is(0, 4194303, timeout=who_is_timeout)
            i_ams = await asyncio.wait_for(fut, timeout=who_is_timeout + 2.0)
        except Exception as e:
            errors.append({"message": f"who_is failed: {e}"})
            return devices, errors

        for i_am in i_ams:
            try:
                di = i_am.iAmDeviceIdentifier[1]
                seg = getattr(i_am.segmentationSupported, "name", None) or str(
                    i_am.segmentationSupported
                )
                devices.append(
                    {
                        "device_instance": di,
                        "address": str(i_am.pduSource),
                        "vendor_id": int(i_am.vendorID),
                        "max_apdu": int(i_am.maxAPDULengthAccepted),
                        "segmentation": seg,
                        "last_seen_at": now,
                    }
                )
            except Exception as e:
                errors.append({"message": str(e), "raw": "i_am_parse"})
        return devices, errors

    async def snapshot_network(self, who_is_timeout: float, read_timeout: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        devices, derr = await self.discover_network(who_is_timeout)
        errors: list[dict[str, Any]] = list(derr)
        out_devices: list[dict[str, Any]] = []
        app = self._require_app()

        for d in devices:
            di = int(d["device_instance"])
            addr = Address(d["address"])
            dev_obj_id = ObjectIdentifier(("device", di))
            try:
                oids = await asyncio.wait_for(
                    _object_identifiers(app, addr, dev_obj_id),
                    timeout=read_timeout,
                )
            except Exception as e:
                errors.append({"device_instance": di, "message": f"object-list: {e}"})
                continue

            objects: list[dict[str, Any]] = []
            for oid in oids:
                if oid[0] == "device":
                    continue
                entry: dict[str, Any] = {
                    "object_type": str(oid[0]),
                    "object_instance": int(oid[1]),
                }
                for prop, key in (
                    ("object-name", "object_name"),
                    ("description", "description"),
                    ("units", "units"),
                    ("present-value", "present_value"),
                    ("status-flags", "status_flags"),
                    ("out-of-service", "out_of_service"),
                    ("reliability", "reliability"),
                ):
                    try:
                        val = await asyncio.wait_for(
                            app.read_property(addr, oid, prop),
                            timeout=read_timeout,
                        )
                        if isinstance(val, ErrorRejectAbortNack):
                            errors.append(
                                {
                                    "device_instance": di,
                                    "object_type": str(oid[0]),
                                    "object_instance": oid[1],
                                    "property": prop,
                                    "message": str(val),
                                }
                            )
                            entry[key] = None
                        else:
                            entry[key] = val
                    except Exception as e:
                        errors.append(
                            {
                                "device_instance": di,
                                "object_type": str(oid[0]),
                                "object_instance": oid[1],
                                "property": prop,
                                "message": str(e),
                            }
                        )
                        entry[key] = None
                objects.append(entry)
            out_devices.append({"device_instance": di, "objects": objects})

        return {"snapshot_at": utc_now_iso(), "devices": out_devices}, errors

    async def read_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        prop: str,
        read_timeout: float,
    ) -> dict[str, Any]:
        app = self._require_app()
        i_ams_fut = app.who_is(device_instance, device_instance, timeout=self._settings.who_is_timeout_seconds)
        try:
            i_ams = await asyncio.wait_for(
                i_ams_fut,
                timeout=self._settings.who_is_timeout_seconds + 2.0,
            )
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "error": str(e),
            }
        if not i_ams:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "error": "device not found (I-Am)",
            }
        addr = Address(i_ams[0].pduSource)
        ois = _object_id_string(object_type, object_instance)
        prop_s = prop.replace("presentValue", "present-value")
        try:
            val = await asyncio.wait_for(
                app.read_property(addr, ois, prop_s),
                timeout=read_timeout,
            )
            if isinstance(val, ErrorRejectAbortNack):
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "error": str(val),
                }
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "value": val,
                "datatype": type(val).__name__,
                "read_at": utc_now_iso(),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "error": str(e),
            }

    async def write_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        value: Any,
        priority: Optional[int],
        write_timeout: float,
    ) -> dict[str, Any]:
        app = self._require_app()
        i_ams_fut = app.who_is(device_instance, device_instance, timeout=self._settings.who_is_timeout_seconds)
        try:
            i_ams = await asyncio.wait_for(
                i_ams_fut,
                timeout=self._settings.who_is_timeout_seconds + 2.0,
            )
        except Exception as e:
            return {"error": str(e)}
        if not i_ams:
            return {"error": "device not found (I-Am)"}
        addr = Address(i_ams[0].pduSource)
        ois = _object_id_string(object_type, object_instance)
        try:
            resp = await asyncio.wait_for(
                app.write_property(addr, ois, "present-value", value, priority=priority),
                timeout=write_timeout,
            )
            if isinstance(resp, ErrorRejectAbortNack):
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": "presentValue",
                    "value": value,
                    "priority": priority,
                    "error": str(resp),
                }
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": "presentValue",
                "value": value,
                "priority": priority,
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": "presentValue",
                "value": value,
                "priority": priority,
                "error": str(e),
            }
