"""Dispatch SaaS jobs to BACnet handlers."""

from __future__ import annotations

import asyncio
import logging
import traceback
from typing import Any, Optional

from bacpypes3.apdu import ErrorRejectAbortNack

from edge_agent.models import (
    BacnetClient,
    JobModel,
    JobResultEnvelope,
    utc_now_iso,
)
from edge_agent.settings import Settings
from edge_agent.storage import Storage

_log = logging.getLogger(__name__)


async def run_job(
    job: JobModel,
    bacnet: BacnetClient,
    storage: Storage,
    settings: Settings,
) -> JobResultEnvelope:
    started = utc_now_iso()
    errors: list[dict[str, Any]] = []
    data: dict[str, Any] = {}
    summary = ""
    status: str = "success"

    try:
        if job.type == "discover_network":
            devices, derr = await asyncio.wait_for(
                bacnet.discover_network(settings.who_is_timeout_seconds),
                timeout=settings.who_is_timeout_seconds + 5.0,
            )
            errors.extend(derr)
            storage.save_latest_discovery({"discovered_at": utc_now_iso(), "devices": devices})
            data = {"discovered_at": utc_now_iso(), "devices": devices}
            summary = f"Discovered {len(devices)} devices"
            if errors and devices:
                status = "partial_success"
            elif errors and not devices:
                status = "failed"

        elif job.type == "snapshot_network":
            snap, serr = await asyncio.wait_for(
                bacnet.snapshot_network(
                    settings.who_is_timeout_seconds,
                    settings.request_timeout_seconds,
                ),
                timeout=600.0,
            )
            errors.extend(serr)
            storage.save_latest_snapshot(snap)
            data = snap
            nd = len(snap.get("devices", []))
            summary = f"Snapshot {nd} devices"
            if errors:
                status = "partial_success" if nd else "failed"
            if not errors and nd == 0:
                status = "failed"
                summary = "Snapshot empty"
                errors.append({"message": "No devices in snapshot"})

        elif job.type == "read_point":
            p = job.payload
            dev = int(p["device_instance"])
            ot = str(p["object_type"])
            oi = int(p["object_instance"])
            prop = str(p.get("property") or "presentValue")
            try:
                rd = await asyncio.wait_for(
                    bacnet.read_point(dev, ot, oi, prop, settings.request_timeout_seconds),
                    timeout=settings.request_timeout_seconds + 5.0,
                )
                data = rd
                summary = "Read OK"
                if rd.get("error"):
                    status = "failed"
                    errors.append({"message": str(rd["error"])})
            except (ErrorRejectAbortNack, Exception) as e:
                status = "failed"
                summary = "Read failed"
                data = {
                    "device_instance": dev,
                    "object_type": ot,
                    "object_instance": oi,
                    "property": prop,
                }
                errors.append({"message": str(e), "traceback": traceback.format_exc()})

        elif job.type == "write_point":
            p = job.payload
            dev = int(p["device_instance"])
            ot = str(p["object_type"])
            oi = int(p["object_instance"])
            val = p["value"]
            pri: Optional[int] = None
            if p.get("priority") is not None:
                pri = int(p["priority"])
            try:
                wr = await asyncio.wait_for(
                    bacnet.write_point(
                        dev, ot, oi, val, pri, settings.request_timeout_seconds
                    ),
                    timeout=settings.request_timeout_seconds + 5.0,
                )
                if wr.get("error"):
                    status = "failed"
                    summary = "Write failed"
                    errors.append({"message": str(wr["error"])})
                    data = wr
                else:
                    summary = "Write OK"
                    data = wr
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "value": val,
                        "priority": pri,
                        "outcome": status,
                        "detail": wr,
                    },
                )
            except (ErrorRejectAbortNack, Exception) as e:
                status = "failed"
                summary = "Write failed"
                data = {
                    "device_instance": dev,
                    "object_type": ot,
                    "object_instance": oi,
                    "property": "presentValue",
                    "value": val,
                    "priority": pri,
                }
                errors.append({"message": str(e), "traceback": traceback.format_exc()})
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "value": val,
                        "priority": pri,
                        "outcome": "failed",
                        "detail": str(e),
                    },
                )

        else:
            status = "failed"
            summary = f"Unknown job type: {job.type}"
            errors.append({"message": summary})

    except asyncio.TimeoutError as e:
        status = "failed"
        summary = "Job timed out"
        errors.append({"message": str(e)})
    except (ErrorRejectAbortNack, Exception) as e:
        status = "failed"
        summary = f"Job error: {e}"
        errors.append({"message": str(e), "traceback": traceback.format_exc()})
        _log.exception("job_failed job_id=%s", job.job_id)

    finished = utc_now_iso()
    return JobResultEnvelope(
        job_id=job.job_id,
        status=status,  # type: ignore[arg-type]
        started_at=started,
        finished_at=finished,
        summary=summary,
        data=data,
        errors=errors,
    )
