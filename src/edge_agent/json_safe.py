"""Convert BACnet / BACpypes values into JSON-serializable plain Python."""

from __future__ import annotations

import re
from typing import Any

# BACpypes constructed values often stringify to Python repr, not a BACnet value.
_REPR_LEAK = re.compile(r"^<[\w.]+ object at 0x[0-9a-f]+\>$", re.IGNORECASE)


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
