"""In-memory BACnet for Pass 1 / local testing."""

from __future__ import annotations

import hashlib
from typing import Any, Optional

from edge_agent.models import utc_now_iso
from edge_agent.bacnet_schedule import (
    calendar_property_to_json,
    calendar_write_values,
    schedule_property_to_json,
    schedule_write_values,
)


class MockBacnetClient:
    async def discover_network(self, who_is_timeout: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        now = utc_now_iso()
        devices = [
            {
                "device_instance": 2001,
                "address": "192.168.1.100:47808",
                "vendor_id": 42,
                "max_apdu": 1476,
                "segmentation": "segmentedBoth",
                "last_seen_at": now,
                "name": "Mock FCU",
                "object_name": "Mock FCU",
            }
        ]
        return devices, []

    async def snapshot_network(self, who_is_timeout: float, read_timeout: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        devs, errs = await self.discover_network(who_is_timeout)
        errors: list[dict[str, Any]] = list(errs)
        devices_out: list[dict[str, Any]] = []
        for d in devs:
            objects = [
                {
                    "object_type": "analogValue",
                    "object_instance": 1,
                    "object_name": "SAT",
                    "description": "Supply air temp",
                    "units": "degreesCelsius",
                    "present_value": 21.5,
                    "status_flags": None,
                    "out_of_service": False,
                    "reliability": "noFaultDetected",
                    "relinquish_default": 18.0,
                    "priority_array": [None] * 15 + [21.5],
                },
                {
                    "object_type": "binaryValue",
                    "object_instance": 2,
                    "object_name": "FanEnable",
                    "description": None,
                    "units": None,
                    "present_value": 1,
                    "active_text": "RUN",
                    "inactive_text": "OFF",
                    "present_value_label": "RUN",
                    "status_flags": None,
                    "out_of_service": False,
                    "reliability": "noFaultDetected",
                    "relinquish_default": "inactive",
                    "priority_array": [None] * 16,
                },
                {
                    "object_type": "multiStateValue",
                    "object_instance": 3,
                    "object_name": "OccMode",
                    "description": None,
                    "units": None,
                    "present_value": 2,
                    "number_of_states": 3,
                    "state_text": ["Unocc", "Occ", "Bypass"],
                    "present_value_label": "Occ",
                    "status_flags": None,
                    "out_of_service": False,
                    "reliability": "noFaultDetected",
                    "relinquish_default": 1,
                    "priority_array": [None] * 16,
                },
            ]
            row = {
                **d,
                "description": "Mock device",
                "location": "Lab",
                "vendor_name": "MockVendor",
                "model_name": "X-1",
                "firmware_revision": "1.0.0",
                "application_software_version": "1.2.3",
                "protocol_version": 1,
                "objects": objects,
            }
            devices_out.append(row)
        data = {
            "snapshot_format_version": 2,
            "snapshot_at": utc_now_iso(),
            "devices": devices_out,
        }
        return data, errors

    async def read_device_live(
        self,
        device_instance: int,
        read_timeout: float,
        max_objects: int,
        deadline_monotonic: Optional[float] = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if device_instance != 2001:
            return (
                {
                    "device_instance": device_instance,
                    "read_at": utc_now_iso(),
                    "objects": [],
                },
                [
                    {
                        "device_instance": device_instance,
                        "message": "device not found (I-Am)",
                    }
                ],
            )
        objects = [
            {
                "object_type": "analogValue",
                "object_instance": 1,
                "object_name": "SAT",
                "description": "Supply air temp",
                "units": "degreesCelsius",
                "present_value": 21.5,
                "status_flags": None,
                "out_of_service": False,
                "reliability": "noFaultDetected",
                "relinquish_default": 18.0,
                "priority_array": [None] * 15 + [21.5],
            },
            {
                "object_type": "binaryValue",
                "object_instance": 2,
                "object_name": "FanEnable",
                "description": None,
                "units": None,
                "present_value": 1,
                "active_text": "RUN",
                "inactive_text": "OFF",
                "present_value_label": "RUN",
                "status_flags": None,
                "out_of_service": False,
                "reliability": "noFaultDetected",
                "relinquish_default": "inactive",
                "priority_array": [None] * 16,
            },
            {
                "object_type": "multiStateValue",
                "object_instance": 3,
                "object_name": "OccMode",
                "description": None,
                "units": None,
                "present_value": 2,
                "number_of_states": 3,
                "state_text": ["Unocc", "Occ", "Bypass"],
                "present_value_label": "Occ",
                "status_flags": None,
                "out_of_service": False,
                "reliability": "noFaultDetected",
                "relinquish_default": 1,
                "priority_array": [None] * 16,
            },
        ]
        total = len(objects)
        if max_objects and max_objects > 0:
            cut = objects[:max_objects]
        else:
            cut = objects
        data: dict[str, Any] = {
            "device_instance": device_instance,
            "read_at": utc_now_iso(),
            "objects": cut,
        }
        if len(cut) < total:
            data["truncated"] = True
            data["total_object_count"] = total
            data["returned_object_count"] = len(cut)
        return data, []

    async def read_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        prop: str,
        read_timeout: float,
        array_index: Optional[int] = None,
    ) -> dict[str, Any]:
        pl = prop.replace("present-value", "presentValue").lower()
        if pl == "presentvalue" and array_index is None:
            return {
                "object_type": object_type,
                "object_instance": object_instance,
                "object_name": "SAT",
                "description": "Supply air temp",
                "units": "degreesCelsius",
                "present_value": 21.5,
                "status_flags": None,
                "out_of_service": False,
                "reliability": "noFaultDetected",
                "present_value_label": None,
                "device_instance": device_instance,
                "property": prop,
                "value": 21.5,
                "datatype": "float",
                "read_at": utc_now_iso(),
            }
        pa = pl.replace("_", "").replace("-", "")
        if pa == "priorityarray":
            pid = "priority-array"
            if array_index is not None:
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "bacnet_property": pid,
                    "array_index": int(array_index),
                    "value": 21.0,
                    "datatype": "float",
                    "read_at": utc_now_iso(),
                }
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "bacnet_property": pid,
                "value": [None] * 16,
                "datatype": "list",
                "read_at": utc_now_iso(),
            }
        return {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "property": prop,
            "array_index": array_index,
            "value": None,
            "datatype": "NoneType",
            "read_at": utc_now_iso(),
        }

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
        out: dict[str, Any] = {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "property": "presentValue",
            "value": value,
            "priority": priority,
        }
        if include_readback:
            out["present_value_after"] = value
            out["read_at"] = utc_now_iso()
        return out

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
        if device_instance != 2001:
            return {
                "error": "device not found (I-Am)",
                "device_instance": device_instance,
                "write_results": [],
            }
        write_results: list[dict[str, Any]] = []
        for i, spec in enumerate(writes):
            if not isinstance(spec, dict) or "property" not in spec or "value" not in spec:
                write_results.append(
                    {
                        "index": i,
                        "property": spec.get("property") if isinstance(spec, dict) else None,
                        "bacnet_property": None,
                        "ok": False,
                        "error": "invalid write entry",
                    }
                )
                continue
            pr = str(spec["property"])
            bp = pr.lower().replace("_", "-")
            # Simulate a manufacturer that rejects engineering-units writes
            if "unit" in bp:
                write_results.append(
                    {
                        "index": i,
                        "property": pr,
                        "bacnet_property": bp,
                        "ok": False,
                        "error": "optional functionality not supported (mock)",
                    }
                )
            else:
                write_results.append(
                    {
                        "index": i,
                        "property": pr,
                        "bacnet_property": bp,
                        "ok": True,
                    }
                )
        result: dict[str, Any] = {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "write_mode": "multi",
            "write_results": write_results,
        }
        if include_readback:
            props = readback_properties or ["present-value"]
            rb: dict[str, Any] = {"read_at": utc_now_iso()}
            for p in props:
                key = p.lower().replace("-", "_")
                if "present" in key:
                    rb["present_value"] = 21.5
                elif "name" in key:
                    rb["object_name"] = "SAT"
                else:
                    rb[key] = None
            result["readback"] = rb
            result["read_at"] = rb["read_at"]
        return result

    async def atomic_write_file(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        file_data: bytes,
        chunk_size: int,
        write_timeout: float,
        include_readback: bool = False,
        read_chunk_size: Optional[int] = None,
    ) -> dict[str, Any]:
        if device_instance != 2001:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "error": "device not found (I-Am)",
            }
        if object_type.lower() != "file":
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "error": "atomic_write_file object_type must be file",
            }
        chunk_size = max(1, int(chunk_size))
        chunks = 0
        ack_positions: list[int] = []
        for pos in range(0, len(file_data), chunk_size):
            chunks += 1
            ack_positions.append(pos)
        digest = hashlib.sha256(file_data).hexdigest()
        out: dict[str, Any] = {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "file_access": "stream",
            "bytes_written": len(file_data),
            "chunk_size": chunk_size,
            "chunks": chunks,
            "ack_positions": ack_positions,
            "expected_sha256": digest,
        }
        if include_readback:
            out["readback_sha256"] = digest
            out["verified"] = True
            out["read_chunk_size"] = int(read_chunk_size or 200)
        return out

    async def atomic_read_file(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        read_timeout: float,
        chunk_size: int = 200,
        expected_length: Optional[int] = None,
    ) -> dict[str, Any]:
        if device_instance != 2001:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "error": "device not found (I-Am)",
            }
        if object_type.lower() != "file":
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "error": "atomic_read_file object_type must be file",
            }
        data = b"\x00\x81\x00\x06\x00\x00\x00\x16\xff\xff\x00\x00REM mock\r\n"
        if expected_length is not None:
            data = data[: int(expected_length)]
        return {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "file_access": "stream",
            "bytes_read": len(data),
            "chunks": 1,
            "chunk_size": int(chunk_size),
            "end_of_file": True,
            "file_sha256": hashlib.sha256(data).hexdigest(),
            "file_data": data,
        }

    async def write_schedule(
        self,
        device_instance: int,
        object_instance: int,
        schedule: dict[str, Any],
        write_timeout: float,
        include_readback: bool = True,
    ) -> dict[str, Any]:
        base = {
            "device_instance": device_instance,
            "object_type": "schedule",
            "object_instance": object_instance,
        }
        if device_instance != 2001:
            return {**base, "error": "device not found (I-Am)"}
        try:
            values = schedule_write_values(schedule)
        except (TypeError, ValueError) as exc:
            return {**base, "error": str(exc)}
        expected = {
            "object_name": str(values["object-name"]),
            "description": str(values["description"]),
            "effective_period": schedule_property_to_json(
                "effective-period", values["effective-period"]
            ),
            "weekly_schedule": schedule_property_to_json(
                "weekly-schedule", values["weekly-schedule"]
            ),
            "exception_schedule": schedule_property_to_json(
                "exception-schedule", values["exception-schedule"]
            ),
            "schedule_default": schedule_property_to_json(
                "schedule-default", values["schedule-default"]
            ),
            "references": schedule_property_to_json(
                "list-of-object-property-references",
                values["list-of-object-property-references"],
            ),
            "priority_for_writing": int(values["priority-for-writing"]),
            "out_of_service": bool(values["out-of-service"]),
        }
        out: dict[str, Any] = {
            **base,
            "write_results": [{"property": "schedule-config", "ok": True}],
        }
        if include_readback:
            out.update(
                {
                    "expected": expected,
                    "readback": {**base, **expected},
                    "verified": True,
                    "differences": {},
                }
            )
        return out

    async def write_calendar(
        self,
        device_instance: int,
        object_instance: int,
        calendar: dict[str, Any],
        write_timeout: float,
        include_readback: bool = True,
        create_if_missing: bool = True,
    ) -> dict[str, Any]:
        base = {
            "device_instance": device_instance,
            "object_type": "calendar",
            "object_instance": object_instance,
        }
        if device_instance != 2001:
            return {**base, "error": "device not found (I-Am)"}
        try:
            values = calendar_write_values(calendar)
        except (TypeError, ValueError) as exc:
            return {**base, "error": str(exc)}
        expected = {
            "object_name": str(values["object-name"]),
            "description": str(values["description"]),
            "date_list": calendar_property_to_json("date-list", values["date-list"]),
        }
        out: dict[str, Any] = {
            **base,
            "created": bool(create_if_missing),
            "write_results": [{"property": "calendar-config", "ok": True}],
        }
        if include_readback:
            out.update(
                {
                    "expected": expected,
                    "readback": {**base, **expected, "present_value": False},
                    "verified": True,
                    "differences": {},
                }
            )
        return out

    async def create_object(
        self,
        device_instance: int,
        object_type: str,
        object_instance: Optional[int],
        initial_properties: Optional[list[dict[str, Any]]],
        write_timeout: float,
    ) -> dict[str, Any]:
        if device_instance != 2001:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "error": "device not found (I-Am)",
            }
        oi_out = int(object_instance) if object_instance is not None else 9999
        out: dict[str, Any] = {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": oi_out,
        }
        if object_instance is not None:
            out["requested_object_instance"] = int(object_instance)
        return out

    async def delete_object(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        write_timeout: float,
    ) -> dict[str, Any]:
        if device_instance != 2001:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "error": "device not found (I-Am)",
            }
        return {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
        }
