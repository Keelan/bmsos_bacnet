"""Convert BACnet / BACpypes values into JSON-serializable plain Python."""

from __future__ import annotations

import re
from typing import Any, Optional, Type


def _enum_token(value: Any) -> Optional[str]:
    if value is None:
        return None
    for attr in ("attr", "name"):
        token = getattr(value, attr, None)
        if isinstance(token, str) and token.strip():
            return token.strip()
    if isinstance(value, str) and value.strip():
        return value.strip()
    try:
        text = str(value).strip()
    except Exception:
        return None
    if not text or text.startswith("<"):
        return None
    return text


def bacnet_error_class_code(obj: Any) -> tuple[Optional[str], Optional[str]]:
    """Pull errorClass / errorCode off Error, CreateObjectError.errorType, or wrappers."""
    current = obj
    seen: set[int] = set()
    for _ in range(6):
        if current is None:
            break
        ident = id(current)
        if ident in seen:
            break
        seen.add(ident)
        error_class = _enum_token(getattr(current, "errorClass", None))
        error_code = _enum_token(getattr(current, "errorCode", None))
        if error_class or error_code:
            return error_class, error_code
        nested = getattr(current, "errorType", None)
        if nested is not None and nested is not current:
            current = nested
            continue
        inner = getattr(current, "error", None)
        if inner is not None and inner is not current:
            current = inner
            continue
        break
    return None, None


def _norm_error_token(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]", "", value.lower())


_ALREADY_EXISTS_CODES = frozenset(
    {"objectalreadyexists", "objectidentifieralreadyexists"}
)


def is_object_already_exists(obj: Any) -> bool:
    _error_class, error_code = bacnet_error_class_code(obj)
    if _norm_error_token(error_code) in _ALREADY_EXISTS_CODES:
        return True
    token = _norm_error_token(failure_message(obj, default=""))
    return any(code in token for code in _ALREADY_EXISTS_CODES)


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
    error_class, error_code = bacnet_error_class_code(obj)
    if error_class or error_code:
        if error_class and error_code:
            return f"{error_class}: {error_code}"
        return error_code or error_class or default
    try:
        s = str(obj).strip()
    except Exception:
        s = ""
    if s and not s.startswith("<"):
        return s
    try:
        r = repr(obj).strip()
    except Exception:
        r = ""
    if r and r not in ("", "''", '""') and not r.startswith("<"):
        return r
    if error_class or error_code:
        return f"{error_class or 'error'}: {error_code or default}"
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


def _object_identifier_to_json(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (list, tuple)) and len(obj) == 2:
        return [str(obj[0]), int(obj[1])]
    try:
        # bacpypes3 ObjectIdentifier acts like a 2-tuple
        if len(obj) == 2:  # type: ignore[arg-type]
            return [str(obj[0]), int(obj[1])]
    except Exception:
        pass
    return str(obj)


def _object_property_reference_to_json(obj: Any) -> Optional[dict[str, Any]]:
    """Decode BACpypes Object/DeviceObjectPropertyReference for snapshot/import."""
    oid = getattr(obj, "objectIdentifier", None)
    if oid is None:
        return None
    prop = getattr(obj, "propertyIdentifier", None)
    arr = getattr(obj, "propertyArrayIndex", None)
    out: dict[str, Any] = {
        "objectIdentifier": _object_identifier_to_json(oid),
        "propertyIdentifier": str(prop) if prop is not None else "present-value",
    }
    if arr is not None:
        try:
            out["propertyArrayIndex"] = int(arr)
        except (TypeError, ValueError):
            out["propertyArrayIndex"] = arr
    device = getattr(obj, "deviceIdentifier", None)
    if device is not None:
        out["deviceIdentifier"] = _object_identifier_to_json(device)
    return out


def _setpoint_reference_to_json(obj: Any) -> Optional[dict[str, Any]]:
    """Decode BACpypes SetpointReference CHOICE (nested .setpointReference OPR)."""
    inner = getattr(obj, "setpointReference", None)
    if inner is None:
        # Some stacks put the OPR fields on the outer object
        if getattr(obj, "objectIdentifier", None) is not None:
            opr = _object_property_reference_to_json(obj)
            return {"setpointReference": opr} if opr else None
        return None
    opr = _object_property_reference_to_json(inner)
    return {"setpointReference": opr} if opr else None


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

    type_name = type(obj).__name__
    if type_name == "ObjectPropertyReference" or (
        hasattr(obj, "objectIdentifier")
        and hasattr(obj, "propertyIdentifier")
        and not hasattr(obj, "setpointReference")
        and "Reference" in type_name
    ):
        decoded = _object_property_reference_to_json(obj)
        if decoded is not None:
            return decoded
    if type_name == "SetpointReference" or hasattr(obj, "setpointReference"):
        decoded_sp = _setpoint_reference_to_json(obj)
        if decoded_sp is not None:
            return decoded_sp
    if type_name == "ObjectIdentifier":
        return _object_identifier_to_json(obj)

    # BACnet AnyAtomic / constructed Any: attempt common loop-reference casts.
    cast_out = getattr(obj, "cast_out", None)
    if callable(cast_out):
        for cls_name, import_path in (
            ("ObjectPropertyReference", "bacpypes3.basetypes"),
            ("DeviceObjectPropertyReference", "bacpypes3.basetypes"),
            ("SetpointReference", "bacpypes3.basetypes"),
            ("Real", "bacpypes3.primitivedata"),
            ("Unsigned", "bacpypes3.primitivedata"),
            ("Enumerated", "bacpypes3.primitivedata"),
            ("Boolean", "bacpypes3.primitivedata"),
            ("CharacterString", "bacpypes3.primitivedata"),
        ):
            try:
                mod = __import__(import_path, fromlist=[cls_name])
                cls = getattr(mod, cls_name)
                typed = cast_out(cls)
                return to_json_safe(typed)
            except Exception:
                continue

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
