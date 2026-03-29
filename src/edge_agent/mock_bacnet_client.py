"""In-memory BACnet for Pass 1 / local testing."""

from __future__ import annotations

from typing import Any, Optional

from edge_agent.models import utc_now_iso


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
            }
        ]
        return devices, []

    async def snapshot_network(self, who_is_timeout: float, read_timeout: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        devs, errs = await self.discover_network(who_is_timeout)
        errors: list[dict[str, Any]] = list(errs)
        devices_out: list[dict[str, Any]] = []
        for d in devs:
            inst = d["device_instance"]
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
                }
            ]
            devices_out.append({"device_instance": inst, "objects": objects})
        data = {"snapshot_at": utc_now_iso(), "devices": devices_out}
        return data, errors

    async def read_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        prop: str,
        read_timeout: float,
    ) -> dict[str, Any]:
        return {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "property": prop,
            "value": 21.5 if prop.lower() in ("presentvalue", "present-value") else None,
            "datatype": "real",
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
    ) -> dict[str, Any]:
        return {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "property": "presentValue",
            "value": value,
            "priority": priority,
        }
