"""BACpypes3 BACnet/IP client (Pass 2)."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from typing import Any, Optional, Union

# ErrorRejectAbortNack subclasses BaseException, not Exception — BACnet errors
# are not caught by `except Exception`.
from bacpypes3.apdu import AbortPDU, AbortReason, ErrorRejectAbortNack
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import ObjectIdentifier

from edge_agent.json_safe import to_json_safe
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
    # BACpypes3 ObjectIdentifier string parsing requires "type,instance" or "type:instance"
    # (a space separator is rejected and breaks every read_property on objects).
    return f"{_camel_to_kebab(object_type)},{object_instance}"


def _bacnet_property_identifier(prop: str) -> str:
    """Normalize SaaS property names to BACnet property id (kebab-case)."""
    p = str(prop).strip()
    if not p:
        return p
    if not any(c.isupper() for c in p):
        return p.lower().replace("_", "-")
    return _camel_to_kebab(p)


def _json_key_for_bacnet_property(prop_kebab: str) -> str:
    """Stable snake_case key for readback JSON (e.g. present-value -> present_value)."""
    return str(prop_kebab).replace("-", "_")


async def _object_identifiers(app: Application, device_address: Address, device_identifier: ObjectIdentifier):
    try:
        object_list = await app.read_property(device_address, device_identifier, "object-list")
        if isinstance(object_list, ErrorRejectAbortNack):
            _log.debug("object-list error response: %s", object_list)
            return []
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
        if isinstance(object_list_length, ErrorRejectAbortNack):
            _log.debug("object-list length error: %s", object_list_length)
            return []
        for i in range(int(object_list_length)):
            oid = await app.read_property(
                device_address,
                device_identifier,
                "object-list",
                array_index=i + 1,
            )
            if isinstance(oid, ErrorRejectAbortNack):
                _log.debug("object-list element error: %s", oid)
                break
            object_list.append(oid)
    except ErrorRejectAbortNack as err:
        _log.debug("object-list indexed err: %s", err)

    return object_list


def _object_type_kind_key(object_type: str) -> str:
    """CamelCase, kebab-case, and snake_case names from BACnet stacks (e.g. multi-state-value)."""
    return str(object_type).lower().replace("-", "").replace("_", "")


def _is_device_object_type(object_type: Any) -> bool:
    """True for BACnet device object (object-list entry); works with str or BACpypes enum."""
    label = getattr(object_type, "name", object_type)
    if not isinstance(label, str):
        label = str(label)
    return _object_type_kind_key(label) == "device"


def _is_binary_object_type(object_type: str) -> bool:
    return _object_type_kind_key(object_type).startswith("binary")


def _is_multistate_object_type(object_type: str) -> bool:
    return _object_type_kind_key(object_type).startswith("multistate")


def _snapshot_property_plan(object_type: str) -> tuple[list[tuple[str, str]], bool]:
    """
    BACnet properties to read per object type (property id, JSON key).
    The bool is True when we should try an extra optional (silent) reliability read
    for stacks that expose it on BV/AV/etc.
    """
    k = _object_type_kind_key(object_type)
    base: list[tuple[str, str]] = [
        ("object-name", "object_name"),
        ("description", "description"),
    ]
    if k.isdigit():
        return base, False
    meta_only = frozenset(
        {
            "file",
            "notificationclass",
            "eventenrollment",
            "program",
            "trendlog",
            "trendlogmultiple",
        }
    )
    if k in meta_only:
        return base, False
    if k == "schedule":
        # present-value is often a constructed schedule; skip bulk read (avoids repr leaks).
        return base, False
    tail_pv = [
        ("present-value", "present_value"),
        ("status-flags", "status_flags"),
        ("out-of-service", "out_of_service"),
    ]
    rel: tuple[str, str] = ("reliability", "reliability")
    # Objects with Priority_Array normally expose Relinquish_Default (not analog/binary inputs).
    rd: tuple[str, str] = ("relinquish-default", "relinquish_default")
    pa: tuple[str, str] = ("priority-array", "priority_array")
    if k == "calendar":
        return base + [("present-value", "present_value")], False
    if k in ("analoginput", "analogoutput"):
        ao = base + [("units", "units")] + tail_pv + [rel]
        if k == "analogoutput":
            ao.append(rd)
            ao.append(pa)
        return ao, False
    if k == "analogvalue":
        return base + [("units", "units")] + tail_pv + [rd, pa], True
    if k.startswith("binary") or k.startswith("multistate") or k == "characterstringvalue":
        row = base + list(tail_pv)
        if k in (
            "binaryoutput",
            "binaryvalue",
            "multistateoutput",
            "multistatevalue",
            "characterstringvalue",
        ):
            row.append(rd)
        if k in ("binaryoutput", "binaryvalue"):
            row.append(pa)
        return row, True
    if k == "loop":
        return base + tail_pv, True
    return base + tail_pv, True


def _coerce_present_value_active(pv: Any) -> Optional[bool]:
    """Map BACnet binary present-value (enum / int / str) to True=active, False=inactive."""
    if pv is None:
        return None
    if isinstance(pv, bool):
        return pv
    name = getattr(pv, "name", None)
    if isinstance(name, str):
        n = name.lower()
        if n == "active":
            return True
        if n == "inactive":
            return False
    s = str(pv).lower()
    if s in ("active", "1", "true"):
        return True
    if s in ("inactive", "0", "false"):
        return False
    try:
        i = int(pv)
        if i == 1:
            return True
        if i == 0:
            return False
    except (TypeError, ValueError):
        pass
    return None


def _present_value_label(
    pv: Any,
    object_type: str,
    active_text: Optional[str],
    inactive_text: Optional[str],
    state_text: Optional[list[str]],
) -> Optional[str]:
    if _is_binary_object_type(object_type):
        side = _coerce_present_value_active(pv)
        if side is True:
            return active_text or "active"
        if side is False:
            return inactive_text or "inactive"
    if _is_multistate_object_type(object_type) and state_text:
        try:
            idx = int(pv) - 1
            if 0 <= idx < len(state_text):
                return state_text[idx]
        except (TypeError, ValueError):
            pass
    return None


async def _snap_read_property(
    app: Application,
    addr: Address,
    oid: Union[ObjectIdentifier, str],
    prop: str,
    read_timeout: float,
    errors: list[dict[str, Any]],
    err_extra: dict[str, Any],
    array_index: Optional[int] = None,
    *,
    record_error: bool = True,
) -> Any:
    try:
        if array_index is not None:
            val = await asyncio.wait_for(
                app.read_property(addr, oid, prop, array_index=array_index),
                timeout=read_timeout,
            )
        else:
            val = await asyncio.wait_for(
                app.read_property(addr, oid, prop),
                timeout=read_timeout,
            )
        if isinstance(val, ErrorRejectAbortNack):
            if record_error:
                errors.append({**err_extra, "property": prop, "message": str(val)})
            return None
        return val
    except ErrorRejectAbortNack as err:
        if record_error:
            errors.append({**err_extra, "property": prop, "message": str(err)})
        return None
    except Exception as e:
        if record_error:
            errors.append({**err_extra, "property": prop, "message": str(e)})
        return None


def _iter_state_text_sequence(raw: Any) -> Optional[list[str]]:
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return None
    if isinstance(raw, (list, tuple)):
        if not raw:
            return None
        return ["" if x is None else str(x) for x in raw]
    try:
        it = iter(raw)
    except TypeError:
        return None
    items = list(it)
    if not items:
        return None
    return ["" if x is None else str(x) for x in items]


async def _read_multistate_state_text(
    app: Application,
    addr: Address,
    oid: ObjectIdentifier,
    read_timeout: float,
    errors: list[dict[str, Any]],
    err_extra: dict[str, Any],
) -> tuple[Optional[int], list[str]]:
    # Some devices return the full array in one read; element-wise reads fail or are slow.
    whole = await _snap_read_property(
        app,
        addr,
        oid,
        "state-text",
        read_timeout,
        errors,
        err_extra,
        record_error=False,
    )
    texts = _iter_state_text_sequence(whole)
    if texts:
        return len(texts), texts

    nraw = await _snap_read_property(
        app, addr, oid, "number-of-states", read_timeout, errors, err_extra
    )
    if nraw is None:
        return None, []
    try:
        n = int(nraw)
    except (TypeError, ValueError):
        return None, []
    if n < 1:
        return n, []
    out: list[str] = []
    for i in range(1, n + 1):
        part = await _snap_read_property(
            app,
            addr,
            oid,
            "state-text",
            read_timeout,
            errors,
            err_extra,
            array_index=i,
        )
        out.append("" if part is None else str(part))
    return n, out


def _is_present_value_property(prop: str) -> bool:
    p = prop.replace("present-value", "presentValue").strip().lower()
    return p == "presentvalue"


async def _build_snapshot_style_object_entry(
    app: Application,
    addr: Address,
    device_instance: int,
    object_type: str,
    object_instance: int,
    read_timeout: float,
    errors: list[dict[str, Any]],
    *,
    present_value_precooked: Optional[Any] = None,
) -> dict[str, Any]:
    """One BACnet object's snapshot-shaped row (same keys as snapshot_network objects[])."""
    ot = str(object_type)
    oi = int(object_instance)
    oid = _object_id_string(ot, oi)
    err_obj: dict[str, Any] = {
        "device_instance": device_instance,
        "object_type": ot,
        "object_instance": oi,
    }
    entry: dict[str, Any] = {
        "object_type": ot,
        "object_instance": oi,
    }
    plan, try_optional_reliability = _snapshot_property_plan(ot)
    for prop, key in plan:
        if present_value_precooked is not None and key == "present_value":
            entry[key] = present_value_precooked
            continue
        val = await _snap_read_property(
            app, addr, oid, prop, read_timeout, errors, err_obj
        )
        if val is not None:
            entry[key] = val
    if try_optional_reliability and "reliability" not in entry:
        r = await _snap_read_property(
            app,
            addr,
            oid,
            "reliability",
            read_timeout,
            errors,
            err_obj,
            record_error=False,
        )
        if r is not None:
            entry["reliability"] = r

    active_text: Optional[str] = None
    inactive_text: Optional[str] = None
    state_text: Optional[list[str]] = None
    if _is_binary_object_type(ot):
        at = await _snap_read_property(
            app, addr, oid, "active-text", read_timeout, errors, err_obj
        )
        it = await _snap_read_property(
            app, addr, oid, "inactive-text", read_timeout, errors, err_obj
        )
        if at is not None:
            active_text = str(at)
            entry["active_text"] = active_text
        if it is not None:
            inactive_text = str(it)
            entry["inactive_text"] = inactive_text
    elif _is_multistate_object_type(ot):
        n_states, texts = await _read_multistate_state_text(
            app, addr, oid, read_timeout, errors, err_obj
        )
        if n_states is not None:
            entry["number_of_states"] = n_states
        if texts:
            state_text = texts
            entry["state_text"] = texts

    label = _present_value_label(
        entry.get("present_value"),
        ot,
        active_text,
        inactive_text,
        state_text,
    )
    if label is not None:
        entry["present_value_label"] = label

    return entry


class BacnetPypesClient:
    """Wraps BACpypes3 Application; recreate via manager on config change."""

    def __init__(self, settings: Settings, effective: EffectiveBacnetConfig) -> None:
        self._settings = settings
        self._effective = effective
        self._app: Optional[Application] = None

    def _build_application(self) -> Application:
        # BACpypes3 snapshots BACPYPES_* from os.environ when bacpypes3.argparse is
        # imported; later os.environ changes are NOT used as argparse defaults.
        # Always pass bind/instance/vendor on the CLI so .env values apply.
        parser = SimpleArgumentParser()
        cli = [
            "--name",
            self._effective.device_name,
            "--instance",
            str(self._effective.device_instance),
            "--vendoridentifier",
            str(self._effective.vendor_identifier),
        ]
        if self._effective.bind_ip.strip():
            addr = format_bacpypes_device_address(
                self._effective.bind_ip,
                self._effective.bind_prefix,
                self._effective.udp_port,
            )
            cli.extend(["--address", addr])
        args = parser.parse_args(cli)
        app = Application.from_args(args)
        return app

    async def start(self) -> None:
        if self._app is not None:
            return
        self._app = self._build_application()
        addr_log = (
            format_bacpypes_device_address(
                self._effective.bind_ip,
                self._effective.bind_prefix,
                self._effective.udp_port,
            )
            if self._effective.bind_ip.strip()
            else "(default host)"
        )
        _log.info(
            "bacnet_stack_started name=%s device_instance=%s address=%s",
            self._effective.device_name,
            self._effective.device_instance,
            addr_log,
        )

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
        except ErrorRejectAbortNack as e:
            errors.append({"message": f"who_is failed: {e}"})
            return devices, errors
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
            out_entry: dict[str, Any] = dict(d)

            try:
                oids = await asyncio.wait_for(
                    _object_identifiers(app, addr, dev_obj_id),
                    timeout=read_timeout,
                )
            except ErrorRejectAbortNack as e:
                errors.append({"device_instance": di, "message": f"object-list: {e}"})
                continue
            except Exception as e:
                errors.append({"device_instance": di, "message": f"object-list: {e}"})
                continue

            err_dev: dict[str, Any] = {"device_instance": di}
            dev_oname = await _snap_read_property(
                app, addr, dev_obj_id, "object-name", read_timeout, errors, err_dev
            )
            if dev_oname is not None:
                nm = str(dev_oname)
                out_entry["object_name"] = nm
                out_entry["name"] = nm
            for prop, key in (
                ("description", "description"),
                ("location", "location"),
                ("vendor-name", "vendor_name"),
                ("model-name", "model_name"),
                ("firmware-revision", "firmware_revision"),
                ("application-software-version", "application_software_version"),
                ("protocol-version", "protocol_version"),
            ):
                v = await _snap_read_property(
                    app, addr, dev_obj_id, prop, read_timeout, errors, err_dev
                )
                if v is not None:
                    out_entry[key] = v

            objects: list[dict[str, Any]] = []
            for oid in oids:
                if _is_device_object_type(oid[0]):
                    continue
                ot = str(oid[0])
                oi = int(oid[1])
                entry = await _build_snapshot_style_object_entry(
                    app, addr, di, ot, oi, read_timeout, errors
                )
                objects.append(entry)

            out_entry["objects"] = objects
            out_devices.append(out_entry)

        return {
            "snapshot_format_version": 2,
            "snapshot_at": utc_now_iso(),
            "devices": out_devices,
        }, errors

    async def read_device_live(
        self,
        device_instance: int,
        read_timeout: float,
        max_objects: int,
        deadline_monotonic: Optional[float] = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Read snapshot-shaped object rows for one device (Explorer live panel)."""
        app = self._require_app()
        errors: list[dict[str, Any]] = []
        read_at = utc_now_iso()
        empty_data: dict[str, Any] = {
            "device_instance": device_instance,
            "read_at": read_at,
            "objects": [],
        }

        i_ams_fut = app.who_is(
            device_instance, device_instance, timeout=self._settings.who_is_timeout_seconds
        )
        try:
            i_ams = await asyncio.wait_for(
                i_ams_fut,
                timeout=self._settings.who_is_timeout_seconds + 2.0,
            )
        except ErrorRejectAbortNack as e:
            errors.append(
                {"device_instance": device_instance, "message": f"who_is: {e}"}
            )
            return empty_data, errors
        except Exception as e:
            errors.append(
                {"device_instance": device_instance, "message": f"who_is: {e}"}
            )
            return empty_data, errors

        if not i_ams:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": "device not found (I-Am)",
                }
            )
            return empty_data, errors

        addr = Address(i_ams[0].pduSource)
        dev_obj_id = ObjectIdentifier(("device", device_instance))

        try:
            oids = await asyncio.wait_for(
                _object_identifiers(app, addr, dev_obj_id),
                timeout=read_timeout,
            )
        except ErrorRejectAbortNack as e:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": f"object-list: {e}",
                }
            )
            return empty_data, errors
        except Exception as e:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": f"object-list: {e}",
                }
            )
            return empty_data, errors

        if not oids:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": "object-list empty or unreadable",
                }
            )
            return empty_data, errors

        non_dev = [o for o in oids if not _is_device_object_type(o[0])]
        total_object_count = len(non_dev)
        if total_object_count == 0:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": "no non-device objects in object-list",
                }
            )
            return empty_data, errors

        if max_objects and max_objects > 0:
            to_process = non_dev[:max_objects]
        else:
            to_process = non_dev

        truncated_by_count = len(to_process) < total_object_count
        objects_out: list[dict[str, Any]] = []
        truncated_by_time = False

        for oid in to_process:
            if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
                truncated_by_time = True
                break
            ot = str(oid[0])
            oi = int(oid[1])
            entry = await _build_snapshot_style_object_entry(
                app, addr, device_instance, ot, oi, read_timeout, errors
            )
            objects_out.append(entry)

        returned_object_count = len(objects_out)
        data: dict[str, Any] = {
            "device_instance": device_instance,
            "read_at": read_at,
            "objects": objects_out,
        }
        truncated = truncated_by_count or truncated_by_time
        if truncated:
            data["truncated"] = True
            data["total_object_count"] = total_object_count
            data["returned_object_count"] = returned_object_count

        return data, errors

    async def read_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        prop: str,
        read_timeout: float,
        array_index: Optional[int] = None,
    ) -> dict[str, Any]:
        app = self._require_app()
        arr_idx: Optional[int] = (
            int(array_index) if array_index is not None else None
        )
        i_ams_fut = app.who_is(device_instance, device_instance, timeout=self._settings.who_is_timeout_seconds)
        try:
            i_ams = await asyncio.wait_for(
                i_ams_fut,
                timeout=self._settings.who_is_timeout_seconds + 2.0,
            )
        except ErrorRejectAbortNack as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": str(e),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": str(e),
            }
        if not i_ams:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": "device not found (I-Am)",
            }
        addr = Address(i_ams[0].pduSource)
        ois = _object_id_string(object_type, object_instance)

        if _is_present_value_property(prop) and arr_idx is None:
            try:
                val = await asyncio.wait_for(
                    app.read_property(addr, ois, "present-value"),
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
            except ErrorRejectAbortNack as err:
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "error": str(err),
                }
            except Exception as e:
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "error": str(e),
                }

            enrich_errors: list[dict[str, Any]] = []
            entry = await _build_snapshot_style_object_entry(
                app,
                addr,
                device_instance,
                object_type,
                object_instance,
                read_timeout,
                enrich_errors,
                present_value_precooked=val,
            )
            read_ts = utc_now_iso()
            out: dict[str, Any] = dict(entry)
            out["device_instance"] = device_instance
            out["object_type"] = object_type
            out["object_instance"] = object_instance
            out["property"] = prop
            out["present_value"] = entry.get("present_value")
            out["value"] = entry.get("present_value")
            out["read_at"] = read_ts
            out["datatype"] = type(val).__name__
            if enrich_errors:
                out["_property_errors"] = enrich_errors
            return out

        pid = (
            "present-value"
            if _is_present_value_property(prop)
            else _bacnet_property_identifier(str(prop))
        )
        if not pid:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": "empty property id",
            }
        try:
            if arr_idx is not None:
                val = await asyncio.wait_for(
                    app.read_property(
                        addr, ois, pid, array_index=int(arr_idx)
                    ),
                    timeout=read_timeout,
                )
            else:
                val = await asyncio.wait_for(
                    app.read_property(addr, ois, pid),
                    timeout=read_timeout,
                )
            if isinstance(val, ErrorRejectAbortNack):
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "bacnet_property": pid,
                    "array_index": arr_idx,
                    "error": str(val),
                }
            safe = to_json_safe(val)
            out: dict[str, Any] = {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "bacnet_property": pid,
                "value": safe,
                "datatype": type(val).__name__,
                "read_at": utc_now_iso(),
            }
            if arr_idx is not None:
                out["array_index"] = arr_idx
            return out
        except ErrorRejectAbortNack as err:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "bacnet_property": pid,
                "array_index": arr_idx,
                "error": str(err),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "bacnet_property": pid,
                "array_index": arr_idx,
                "error": str(e),
            }

    async def _resolve_device_address(
        self, device_instance: int
    ) -> tuple[Optional[Address], Optional[str]]:
        app = self._require_app()
        i_ams_fut = app.who_is(
            device_instance, device_instance, timeout=self._settings.who_is_timeout_seconds
        )
        try:
            i_ams = await asyncio.wait_for(
                i_ams_fut,
                timeout=self._settings.who_is_timeout_seconds + 2.0,
            )
        except ErrorRejectAbortNack as e:
            return None, str(e)
        except Exception as e:
            return None, str(e)
        if not i_ams:
            return None, "device not found (I-Am)"
        return Address(i_ams[0].pduSource), None

    async def _write_property_dispatch(
        self,
        app: Application,
        addr: Address,
        ois: str,
        pid: str,
        val: Any,
        write_timeout: float,
        priority: Optional[int],
        array_index: Optional[int],
    ) -> Union[Any, ErrorRejectAbortNack]:
        """Single BACnet WriteProperty; priority only for present-value; array_index for arrays."""
        if pid == "present-value":
            if array_index is not None and priority is None:
                raise ValueError(
                    "present-value uses BACnet priority (1-16), not array_index; "
                    "omit array_index, set priority for that slot, or use property "
                    "priority-array with array_index"
                )
            if priority is not None and array_index is not None:
                return await asyncio.wait_for(
                    app.write_property(
                        addr,
                        ois,
                        pid,
                        val,
                        priority=int(priority),
                        array_index=int(array_index),
                    ),
                    timeout=write_timeout,
                )
            if priority is not None:
                return await asyncio.wait_for(
                    app.write_property(
                        addr, ois, pid, val, priority=int(priority)
                    ),
                    timeout=write_timeout,
                )
        if array_index is not None:
            return await asyncio.wait_for(
                app.write_property(
                    addr, ois, pid, val, array_index=int(array_index)
                ),
                timeout=write_timeout,
            )
        return await asyncio.wait_for(
            app.write_property(addr, ois, pid, val),
            timeout=write_timeout,
        )

    async def write_point_multi(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        writes: list[dict[str, Any]],
        write_timeout: float,
        include_readback: bool = False,
        readback_properties: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """
        Apply multiple WriteProperty operations in order. Per-write ok/error in write_results.
        Manufacturers may reject some properties; use result status partial_success on SaaS.
        """
        addr, addr_err = await self._resolve_device_address(device_instance)
        if addr_err:
            return {
                "error": addr_err,
                "device_instance": device_instance,
                "write_results": [],
            }
        app = self._require_app()
        ois = _object_id_string(object_type, object_instance)
        write_results: list[dict[str, Any]] = []

        for i, spec in enumerate(writes):
            if not isinstance(spec, dict):
                write_results.append(
                    {
                        "index": i,
                        "property": None,
                        "bacnet_property": None,
                        "ok": False,
                        "error": "write entry must be an object",
                    }
                )
                continue
            prop_raw = spec.get("property")
            if prop_raw is None or str(prop_raw).strip() == "":
                write_results.append(
                    {
                        "index": i,
                        "property": None,
                        "bacnet_property": None,
                        "ok": False,
                        "error": "missing property",
                    }
                )
                continue
            if "value" not in spec:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": None,
                        "ok": False,
                        "error": "missing value (use null for BACnet null when applicable)",
                    }
                )
                continue

            pid = _bacnet_property_identifier(str(prop_raw))
            if not pid:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": None,
                        "ok": False,
                        "error": "empty property id",
                    }
                )
                continue

            val = spec["value"]
            pri = spec.get("priority")
            if pri is not None:
                pri = int(pri)
            arr_idx = spec.get("array_index")
            if arr_idx is not None:
                arr_idx = int(arr_idx)

            if pid == "present-value":
                if val is None and pri is None:
                    write_results.append(
                        {
                            "index": i,
                            "property": str(prop_raw),
                            "bacnet_property": pid,
                            "ok": False,
                            "error": (
                                "present-value null (relinquish) requires priority 1-16, "
                                "or use property priority-array with array_index and value null"
                            ),
                        }
                    )
                    continue
                if pri is not None and (pri < 1 or pri > 16):
                    write_results.append(
                        {
                            "index": i,
                            "property": str(prop_raw),
                            "bacnet_property": pid,
                            "ok": False,
                            "error": "priority must be 1-16 for present-value",
                        }
                    )
                    continue
            if pid == "priority-array" and arr_idx is None:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": "priority-array write requires array_index (1-16)",
                    }
                )
                continue
            if (
                pid == "priority-array"
                and arr_idx is not None
                and (arr_idx < 1 or arr_idx > 16)
            ):
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": "priority-array array_index must be 1-16",
                    }
                )
                continue

            try:
                resp = await self._write_property_dispatch(
                    app, addr, ois, pid, val, write_timeout, pri, arr_idx
                )
                if isinstance(resp, ErrorRejectAbortNack):
                    write_results.append(
                        {
                            "index": i,
                            "property": str(prop_raw),
                            "bacnet_property": pid,
                            "ok": False,
                            "error": str(resp),
                        }
                    )
                else:
                    write_results.append(
                        {
                            "index": i,
                            "property": str(prop_raw),
                            "bacnet_property": pid,
                            "ok": True,
                        }
                    )
            except ErrorRejectAbortNack as err:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": str(err),
                    }
                )
            except Exception as e:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": str(e),
                    }
                )

        result: dict[str, Any] = {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "write_mode": "multi",
            "write_results": write_results,
        }

        props_to_read: Optional[list[str]] = None
        if include_readback:
            props_to_read = readback_properties if readback_properties else ["present-value"]

        if props_to_read:
            rb_at = utc_now_iso()
            rb_obj: dict[str, Any] = {}
            for rb in props_to_read:
                rpid = _bacnet_property_identifier(str(rb))
                jkey = _json_key_for_bacnet_property(rpid)
                try:
                    pv = await asyncio.wait_for(
                        app.read_property(addr, ois, rpid),
                        timeout=write_timeout,
                    )
                    if isinstance(pv, ErrorRejectAbortNack):
                        rb_obj[jkey] = None
                        rb_obj[f"{jkey}_error"] = str(pv)
                    else:
                        rb_obj[jkey] = to_json_safe(pv)
                except (ErrorRejectAbortNack, Exception) as e:
                    rb_obj[jkey] = None
                    rb_obj[f"{jkey}_error"] = str(e)
            result["readback"] = rb_obj
            result["read_at"] = rb_at

        return result

    async def write_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        value: Any,
        priority: Optional[int],
        write_timeout: float,
        include_readback: bool = False,
    ) -> dict[str, Any]:
        addr, addr_err = await self._resolve_device_address(device_instance)
        if addr_err:
            return {"error": addr_err}
        app = self._require_app()
        ois = _object_id_string(object_type, object_instance)
        try:
            resp = await self._write_property_dispatch(
                app,
                addr,
                ois,
                "present-value",
                value,
                write_timeout,
                priority,
                None,
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
            result: dict[str, Any] = {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": "presentValue",
                "value": value,
                "priority": priority,
            }
            if include_readback:
                rb_at = utc_now_iso()
                try:
                    pv = await asyncio.wait_for(
                        app.read_property(addr, ois, "present-value"),
                        timeout=write_timeout,
                    )
                    if isinstance(pv, ErrorRejectAbortNack):
                        result["present_value_after"] = None
                    else:
                        result["present_value_after"] = to_json_safe(pv)
                except (ErrorRejectAbortNack, Exception):
                    result["present_value_after"] = None
                result["read_at"] = rb_at
            return result
        except ErrorRejectAbortNack as err:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": "presentValue",
                "value": value,
                "priority": priority,
                "error": str(err),
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
