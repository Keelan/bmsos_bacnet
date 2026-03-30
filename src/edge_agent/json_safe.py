"""Convert BACnet / BACpypes values into JSON-serializable plain Python."""

from __future__ import annotations

import re
from typing import Any, Optional, Type


def failure_message(obj: Any, *, default: str = "operation failed") -> str:
    """
    BACnet Error/Reject/Abort and some stack types can stringify to empty text.
    Job results and write_results must always carry a non-empty error string for SaaS/UI.
    """
    if obj is None:
        return default
    if isinstance(obj, str):
        t = obj.strip()
        return t if t else default
    try:
        s = str(obj).strip()
    except Exception:
        s = ""
    if s:
        return s
    try:
        r = repr(obj).strip()
    except Exception:
        r = ""
    if r and r not in ("", "''", '""'):
        return r
    return f"{default} ({type(obj).__name__})"

# BACpypes constructed values often stringify to Python repr, not a BACnet value.
_REPR_LEAK = re.compile(r"^<[\w.]+ object at 0x[0-9a-f]+\>$", re.IGNORECASE)

_priority_value_cls: Any = None


def _priority_value_type() -> Optional[Type[Any]]:
    global _priority_value_cls
    if _priority_value_cls is False:
        return None
    if _priority_value_cls is None:
        try:
            from bacpypes3.basetypes import PriorityValue as _PV

            _priority_value_cls = _PV
        except ImportError:
            _priority_value_cls = False
    return _priority_value_cls if _priority_value_cls is not False else None


def _is_array_of_priority_values(obj: Any) -> bool:
    pv = _priority_value_type()
    if pv is None:
        return False
    sub = getattr(type(obj), "_subtype", None)
    return isinstance(obj, list) and sub is pv


def _priority_value_to_json(obj: Any) -> Any:
    """
    Expand BACpypes3 PriorityValue (CHOICE) to JSON-safe data.
    Empty / null slot -> None. Common atomics map to Python scalars.
    """
    choice = getattr(obj, "_choice", None)
    if choice is None:
        return None
    inner = getattr(obj, choice, None)
    if choice == "null" or inner is None:
        return None
    if choice in ("real", "double"):
        return float(inner)
    if choice in ("integer", "unsigned", "enumerated"):
        try:
            return int(inner)
        except (TypeError, ValueError):
            return str(inner)
    if choice == "boolean":
        return bool(inner)
    if choice == "characterString":
        return str(inner)
    if choice == "octetString":
        if isinstance(inner, (bytes, bytearray)):
            return bytes(inner).hex()
        return str(inner)
    if choice == "bitString":
        raw = getattr(inner, "value", inner)
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw).hex()
        return str(inner)
    if choice == "objectidentifier":
        if isinstance(inner, (list, tuple)) and len(inner) == 2:
            return [str(inner[0]), int(inner[1])]
        return to_json_safe(inner)
    if choice in ("date", "time", "datetime"):
        return str(inner)
    if choice == "constructedValue":
        return to_json_safe(inner)
    return {"bacnet_choice": choice, "value": to_json_safe(inner)}


def to_json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        return int(obj)
    if isinstance(obj, float):
        return float(obj)
    if isinstance(obj, str):
        return obj
    if isinstance(obj, (bytes, bytearray)):
        return bytes(obj).hex()

    pv_cls = _priority_value_type()
    if pv_cls is not None and isinstance(obj, pv_cls):
        return _priority_value_to_json(obj)
    if _is_array_of_priority_values(obj):
        return [_priority_value_to_json(x) for x in obj]

    if isinstance(obj, dict):
        return {str(k): to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        if type(obj).__name__ == "ObjectIdentifier" and len(obj) == 2:
            return [str(obj[0]), int(obj[1])]
        return [to_json_safe(x) for x in obj]
    try:
        return int(obj)
    except (TypeError, ValueError):
        pass
    try:
        return float(obj)
    except (TypeError, ValueError):
        pass
    s = str(obj)
    if _REPR_LEAK.match(s.strip()):
        return None
    return s
