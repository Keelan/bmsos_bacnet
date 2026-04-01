"""BACpypes3 BACnet/IP client (Pass 2)."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from importlib.metadata import PackageNotFoundError, version as package_version
from typing import Any, Optional, Union

# ErrorRejectAbortNack subclasses BaseException, not Exception — BACnet errors
# are not caught by `except Exception`.
from bacpypes3.apdu import AbortPDU, AbortReason, ErrorRejectAbortNack
from bacpypes3.app import Application
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.basetypes import BinaryPV, EventState, ObjectTypesSupported, Polarity, StatusFlags
from bacpypes3.local.binary import BinaryInputObject
from bacpypes3.pdu import Address
from bacpypes3.primitivedata import Boolean, CharacterString, ObjectIdentifier

from edge_agent.json_safe import failure_message, to_json_safe
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


# Plain normalized type (no separators) -> BACnet object-id token for ReadProperty.
# Without this, "binaryvalue" becomes "binaryvalue,1" and stacks often reject it;
# "binary-value,1" works. Same for analog-value, multi-state-value, etc.
_PLAIN_KIND_TO_OBJECT_ID_TOKEN: dict[str, str] = {
    "analoginput": "analog-input",
    "analogoutput": "analog-output",
    "analogvalue": "analog-value",
    "binaryinput": "binary-input",
    "binaryoutput": "binary-output",
    "binaryvalue": "binary-value",
    "multistateinput": "multi-state-input",
    "multistateoutput": "multi-state-output",
    "multistatevalue": "multi-state-value",
    "characterstringvalue": "character-string-value",
    "notificationclass": "notification-class",
    "trendlog": "trend-log",
    "trendlogmultiple": "trend-log-multiple",
    "eventenrollment": "event-enrollment",
}


def _object_type_label(raw: Any) -> str:
    """
    Stable BACnet object-type label for planning reads and JSON rows.
    Handles BACpypes enums (use .name), 'ObjectType.analogValue' str forms, etc.
    """
    if raw is None:
        return ""
    name = getattr(raw, "name", None)
    if isinstance(name, str) and name.strip():
        base = name.strip()
    else:
        base = str(raw).strip()
    if "." in base:
        base = base.rsplit(".", 1)[-1]
    return base


def _object_type_kind_key(object_type: Any) -> str:
    """Normalize for _snapshot_property_plan (camel, kebab, snake, spaces)."""
    label = _object_type_label(object_type)
    return label.lower().replace("-", "").replace("_", "").replace(" ", "")


def _object_id_string(object_type: str, object_instance: int) -> str:
    # BACpypes3 ObjectIdentifier string parsing requires "type,instance" or "type:instance"
    # (a space separator is rejected and breaks every read_property on objects).
    p = str(object_type).strip()
    pk = _object_type_kind_key(p)
    if pk in _PLAIN_KIND_TO_OBJECT_ID_TOKEN:
        p = _PLAIN_KIND_TO_OBJECT_ID_TOKEN[pk]
    return f"{_camel_to_kebab(p)},{object_instance}"


def _object_type_for_json(ot_label: str) -> str:
    """Kebab-case object_type for API consumers (matches Explorer / SaaS)."""
    pk = _object_type_kind_key(ot_label)
    if pk in _PLAIN_KIND_TO_OBJECT_ID_TOKEN:
        return _PLAIN_KIND_TO_OBJECT_ID_TOKEN[pk]
    return _camel_to_kebab(ot_label)


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


def _is_device_object_type(object_type: Any) -> bool:
    """True for BACnet device object (object-list entry); works with str or BACpypes enum."""
    label = getattr(object_type, "name", object_type)
    if not isinstance(label, str):
        label = str(label)
    return _object_type_kind_key(label) == "device"


def _is_binary_object_type(object_type: Any) -> bool:
    return _object_type_kind_key(object_type).startswith("binary")


def _is_multistate_object_type(object_type: Any) -> bool:
    return _object_type_kind_key(object_type).startswith("multistate")


def _snapshot_property_plan(object_type: Any) -> tuple[list[tuple[str, str]], bool]:
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
        if k in (
            "binaryoutput",
            "binaryvalue",
            "multistateoutput",
            "multistatevalue",
        ):
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


async def _snap_read_property_ex(
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
) -> tuple[Any, bool]:
    """
    Returns (value, success). success is False on NACK/error; value may be None
    on success (e.g. BACnet null) — caller decides whether to set a JSON key.
    """
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
                errors.append(
                    {
                        **err_extra,
                        "property": prop,
                        "message": failure_message(
                            val, default="read property rejected"
                        ),
                    }
                )
            return None, False
        return val, True
    except ErrorRejectAbortNack as err:
        if record_error:
            errors.append(
                {
                    **err_extra,
                    "property": prop,
                    "message": failure_message(
                        err, default="read property rejected"
                    ),
                }
            )
        return None, False
    except Exception as e:
        if record_error:
            errors.append(
                {
                    **err_extra,
                    "property": prop,
                    "message": failure_message(
                        e, default="read property exception"
                    ),
                }
            )
        return None, False


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
    val, _ok = await _snap_read_property_ex(
        app,
        addr,
        oid,
        prop,
        read_timeout,
        errors,
        err_extra,
        array_index,
        record_error=record_error,
    )
    return val


def _priority_array_whole_has_live_slot(whole: Any) -> bool:
    """
    True if at least one priority slot decodes to a non-null JSON value.
    Some devices return a 'successful' full-array read of 16 empty slots while
    indexed reads 1..16 return the real PriorityValues (see priority-array reads).
    """
    try:
        n = len(whole)  # type: ignore[arg-type]
    except TypeError:
        return True
    for i in range(min(n, 16)):
        try:
            slot = whole[i]
        except (IndexError, TypeError):
            return True
        if to_json_safe(slot) is not None:
            return True
    return False


def _priority_array_whole_is_usable(whole: Any) -> bool:
    if whole is None:
        return False
    try:
        n = len(whole)
    except TypeError:
        return False
    if n != 16:
        return False
    # Bulk read may succeed with 16 empty-looking slots; prefer indexed fallback.
    if not _priority_array_whole_has_live_slot(whole):
        return False
    return True


async def _read_priority_array_for_snapshot(
    app: Application,
    addr: Address,
    oid: str,
    read_timeout: float,
    errors: list[dict[str, Any]],
    err_extra: dict[str, Any],
) -> list[Any]:
    """
    Many devices reject or truncate a single ReadProperty on the full
    priority-array; fall back to indexed reads 1..16 (like state-text).
    """
    whole, whole_ok = await _snap_read_property_ex(
        app,
        addr,
        oid,
        "priority-array",
        read_timeout,
        errors,
        err_extra,
        record_error=False,
    )
    if whole_ok and _priority_array_whole_is_usable(whole):
        return list(whole)

    slots: list[Any] = []
    for i in range(1, 17):
        part, _ok = await _snap_read_property_ex(
            app,
            addr,
            oid,
            "priority-array",
            read_timeout,
            errors,
            err_extra,
            array_index=i,
            record_error=False,
        )
        slots.append(part)
    return slots


def _bacnet_relinquish_present_value_as_null() -> Any:
    """
    WriteProperty to present-value with priority: BACnet NULL relinquishes that slot.
    BACpypes3 write_property skips coercion only for primitivedata.Null when priority is set.
    """
    from bacpypes3.primitivedata import Null

    return Null(())


def _bacnet_null_priority_array_element() -> Any:
    """priority-array[index] relinquish — element type is PriorityValue."""
    from bacpypes3.basetypes import PriorityValue

    return PriorityValue(null=())


def _normalize_write_value_for_bacnet(
    pid: str,
    val: Any,
    priority: Optional[int],
    array_index: Optional[int],
) -> Any:
    if val is not None:
        return val
    if pid == "priority-array" and array_index is not None:
        return _bacnet_null_priority_array_element()
    if pid == "present-value" and priority is not None:
        return _bacnet_relinquish_present_value_as_null()
    return val


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
    object_type: Any,
    object_instance: int,
    read_timeout: float,
    errors: list[dict[str, Any]],
    *,
    read_oid: Optional[Any] = None,
    present_value_precooked: Optional[Any] = None,
) -> dict[str, Any]:
    """One BACnet object's snapshot-shaped row (same keys as snapshot_network objects[])."""
    ot = _object_type_label(object_type)
    oi = int(object_instance)
    oid = read_oid if read_oid is not None else _object_id_string(ot, oi)
    ot_json = _object_type_for_json(ot)
    err_obj: dict[str, Any] = {
        "device_instance": device_instance,
        "object_type": ot_json,
        "object_instance": oi,
    }
    entry: dict[str, Any] = {
        "object_type": ot_json,
        "object_instance": oi,
    }
    plan, try_optional_reliability = _snapshot_property_plan(ot)
    for prop, key in plan:
        if present_value_precooked is not None and key == "present_value":
            entry[key] = present_value_precooked
            continue
        if key == "priority_array":
            entry[key] = await _read_priority_array_for_snapshot(
                app, addr, oid, read_timeout, errors, err_obj
            )
            continue
        if key == "relinquish_default":
            val_rd, ok_rd = await _snap_read_property_ex(
                app, addr, oid, prop, read_timeout, errors, err_obj
            )
            if ok_rd:
                entry[key] = val_rd
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


def _create_edge_status_binary_inputs() -> tuple[BinaryInputObject, BinaryInputObject]:
    """
    Two local binary-input objects on the edge device: WAN check and SaaS heartbeat liveness.
    inactive/active texts are Offline/Online (present-value label via BACnet state text).
    """
    def _bi_common() -> dict[str, Any]:
        return {
            "statusFlags": StatusFlags([0, 0, 0, 0]),
            "eventState": EventState.normal,
            "outOfService": Boolean(False),
            "polarity": Polarity.normal,
            "inactiveText": CharacterString("Offline"),
            "activeText": CharacterString("Online"),
        }

    internet = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,1"),
        objectName=CharacterString("Edge-Internet"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("Internet / WAN (HTTP reachability)"),
        **_bi_common(),
    )
    saas = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,2"),
        objectName=CharacterString("Edge-SaaS"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("SaaS API heartbeat within online threshold"),
        **_bi_common(),
    )
    return internet, saas


def _patch_local_device_object_types_supported(app: Application) -> None:
    """
    BACpypes3's local DeviceObject returns an empty protocol-object-types-supported
    bitstring. Many supervisors only expose object types that are marked supported,
    so binary-input points would not appear even though object-list contains them.
    """
    dev = app.device_object
    base = dev.__class__

    class _DeviceWithObjectTypesSupported(base):
        @property
        def protocolObjectTypesSupported(self) -> ObjectTypesSupported:
            ots = ObjectTypesSupported([0] * 63)
            ots[ObjectTypesSupported.binaryInput] = 1
            ots[ObjectTypesSupported.device] = 1
            ots[ObjectTypesSupported.networkPort] = 1
            return ots

    dev.__class__ = _DeviceWithObjectTypesSupported


def _resolved_edge_agent_version(settings: Settings) -> str:
    v = (settings.software_version or "").strip()
    if v:
        return v
    try:
        return package_version("edge-agent")
    except PackageNotFoundError:
        return "unknown"


def _apply_device_metadata(app: Application, settings: Settings) -> None:
    """Set device object model-name and application-software-version (BACnet)."""
    ver = _resolved_edge_agent_version(settings)
    app.device_object.applicationSoftwareVersion = CharacterString(ver)
    model = (settings.bacnet_model_name or "").strip() or "bmOS-edge"
    app.device_object.modelName = CharacterString(model)


class BacnetPypesClient:
    """Wraps BACpypes3 Application; recreate via manager on config change."""

    def __init__(self, settings: Settings, effective: EffectiveBacnetConfig) -> None:
        self._settings = settings
        self._effective = effective
        self._app: Optional[Application] = None
        self._bi_internet: Optional[BinaryInputObject] = None
        self._bi_saas: Optional[BinaryInputObject] = None

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
        _patch_local_device_object_types_supported(app)
        _apply_device_metadata(app, self._settings)
        bi_internet, bi_saas = _create_edge_status_binary_inputs()
        app.add_object(bi_internet)
        app.add_object(bi_saas)
        self._bi_internet = bi_internet
        self._bi_saas = bi_saas
        return app

    def update_edge_status_binary_inputs(self, internet_ok: bool, saas_ok: bool) -> None:
        """Update present-value for Edge-Internet and Edge-SaaS binary-input objects."""
        if self._bi_internet is None or self._bi_saas is None:
            return
        self._bi_internet.presentValue = BinaryPV.active if internet_ok else BinaryPV.inactive
        self._bi_saas.presentValue = BinaryPV.active if saas_ok else BinaryPV.inactive

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
        self._bi_internet = None
        self._bi_saas = None

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
            errors.append(
                {
                    "message": f"who_is failed: {failure_message(e, default='rejected')}",
                }
            )
            return devices, errors
        except Exception as e:
            errors.append(
                {
                    "message": f"who_is failed: {failure_message(e, default='failed')}",
                }
            )
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
                errors.append(
                    {
                        "message": failure_message(e, default="i_am parse failed"),
                        "raw": "i_am_parse",
                    }
                )
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
                errors.append(
                    {
                        "device_instance": di,
                        "message": f"object-list: {failure_message(e, default='rejected')}",
                    }
                )
                continue
            except Exception as e:
                errors.append(
                    {
                        "device_instance": di,
                        "message": f"object-list: {failure_message(e, default='failed')}",
                    }
                )
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
                oi = int(oid[1])
                entry = await _build_snapshot_style_object_entry(
                    app,
                    addr,
                    di,
                    oid[0],
                    oi,
                    read_timeout,
                    errors,
                    read_oid=oid,
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
                {
                    "device_instance": device_instance,
                    "message": f"who_is: {failure_message(e, default='rejected')}",
                }
            )
            return empty_data, errors
        except Exception as e:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": f"who_is: {failure_message(e, default='failed')}",
                }
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
                    "message": f"object-list: {failure_message(e, default='rejected')}",
                }
            )
            return empty_data, errors
        except Exception as e:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": f"object-list: {failure_message(e, default='failed')}",
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
            oi = int(oid[1])
            entry = await _build_snapshot_style_object_entry(
                app,
                addr,
                device_instance,
                oid[0],
                oi,
                read_timeout,
                errors,
                read_oid=oid,
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
                "error": failure_message(e, default="who-is failed"),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": failure_message(e, default="who-is exception"),
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
                        "error": failure_message(
                            val, default="read present-value rejected"
                        ),
                    }
            except ErrorRejectAbortNack as err:
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "error": failure_message(
                        err, default="read present-value rejected"
                    ),
                }
            except Exception as e:
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "error": failure_message(e, default="read present-value failed"),
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
            if pid == "priority-array" and arr_idx is None:
                pe: list[dict[str, Any]] = []
                pa_list = await _read_priority_array_for_snapshot(
                    app,
                    addr,
                    ois,
                    read_timeout,
                    pe,
                    {
                        "device_instance": device_instance,
                        "object_type": object_type,
                        "object_instance": object_instance,
                    },
                )
                safe = to_json_safe(pa_list)
                out_pa: dict[str, Any] = {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "bacnet_property": pid,
                    "value": safe,
                    "datatype": "list",
                    "read_at": utc_now_iso(),
                }
                if pe:
                    out_pa["_property_errors"] = pe
                return out_pa

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
                    "error": failure_message(val, default="read property rejected"),
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
                "error": failure_message(err, default="read property rejected"),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "bacnet_property": pid,
                "array_index": arr_idx,
                "error": failure_message(e, default="read property failed"),
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
            return None, failure_message(
                e, default="who-is / address resolution rejected"
            )
        except Exception as e:
            return None, failure_message(
                e, default="who-is / address resolution failed"
            )
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
        val = _normalize_write_value_for_bacnet(pid, val, priority, array_index)
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
                "error": failure_message(
                    addr_err, default="device address resolution failed"
                ),
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
                            "error": failure_message(
                                resp, default="BACnet write rejected"
                            ),
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
                        "error": failure_message(err, default="BACnet write rejected"),
                    }
                )
            except Exception as e:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": failure_message(e, default="write raised exception"),
                    }
                )

        for row in write_results:
            if row.get("ok") is True:
                continue
            row["error"] = failure_message(
                row.get("error"),
                default=f"write failed (index {row.get('index')})",
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
                    if rpid == "priority-array":
                        pe: list[dict[str, Any]] = []
                        pa_list = await asyncio.wait_for(
                            _read_priority_array_for_snapshot(
                                app,
                                addr,
                                ois,
                                write_timeout,
                                pe,
                                {
                                    "device_instance": device_instance,
                                    "object_type": object_type,
                                    "object_instance": object_instance,
                                },
                            ),
                            timeout=write_timeout + 2.0,
                        )
                        rb_obj[jkey] = to_json_safe(pa_list)
                        if pe:
                            rb_obj[f"{jkey}_errors"] = pe
                    else:
                        pv = await asyncio.wait_for(
                            app.read_property(addr, ois, rpid),
                            timeout=write_timeout,
                        )
                        if isinstance(pv, ErrorRejectAbortNack):
                            rb_obj[jkey] = None
                            rb_obj[f"{jkey}_error"] = failure_message(
                                pv, default="readback rejected"
                            )
                        else:
                            rb_obj[jkey] = to_json_safe(pv)
                except (ErrorRejectAbortNack, Exception) as e:
                    rb_obj[jkey] = None
                    rb_obj[f"{jkey}_error"] = failure_message(
                        e, default="readback failed"
                    )
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
            return {
                "error": failure_message(
                    addr_err, default="device address resolution failed"
                )
            }
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
                    "error": failure_message(resp, default="BACnet write rejected"),
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
                "error": failure_message(err, default="BACnet write rejected"),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": "presentValue",
                "value": value,
                "priority": priority,
                "error": failure_message(e, default="write raised exception"),
            }
