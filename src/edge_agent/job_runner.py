"""Dispatch SaaS jobs to BACnet handlers."""

from __future__ import annotations

import asyncio
import logging
import time
import traceback
from typing import Any, Optional

from bacpypes3.apdu import ErrorRejectAbortNack

from edge_agent.json_safe import to_json_safe
from edge_agent.models import (
    BacnetClient,
    JobModel,
    JobResultEnvelope,
    apply_float_tuning,
    apply_int_tuning,
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

    tuning = storage.get_remote_agent_tuning()
    who_timeout = apply_float_tuning(
        settings.who_is_timeout_seconds, tuning, "who_is_timeout_seconds", 1.0, 120.0
    )
    read_live_max_default = apply_int_tuning(
        settings.read_device_live_max_objects,
        tuning,
        "read_device_live_max_objects",
        1,
        10000,
    )
    read_live_timeout_default = apply_float_tuning(
        settings.read_device_live_timeout_seconds,
        tuning,
        "read_device_live_timeout_seconds",
        10.0,
        600.0,
    )

    try:
        if job.type == "discover_network":
            devices, derr = await asyncio.wait_for(
                bacnet.discover_network(who_timeout),
                timeout=who_timeout + 5.0,
            )
            errors.extend(derr)
            data = to_json_safe({"discovered_at": utc_now_iso(), "devices": devices})
            storage.save_latest_discovery(data)
            summary = f"Discovered {len(devices)} devices"
            if errors and devices:
                status = "partial_success"
            elif errors and not devices:
                status = "failed"

        elif job.type == "snapshot_network":
            snap, serr = await asyncio.wait_for(
                bacnet.snapshot_network(
                    who_timeout,
                    settings.request_timeout_seconds,
                ),
                timeout=600.0,
            )
            errors.extend(serr)
            snap = to_json_safe(snap)
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

        elif job.type == "read_device_live":
            p = job.payload
            dev = int(p["device_instance"])
            max_obj = (
                int(p["max_objects"])
                if p.get("max_objects") is not None
                else read_live_max_default
            )
            to_sec = (
                float(p["timeout_seconds"])
                if p.get("timeout_seconds") is not None
                else read_live_timeout_default
            )
            deadline = time.monotonic() + max(1.0, to_sec)
            try:
                live, derr = await asyncio.wait_for(
                    bacnet.read_device_live(
                        dev,
                        settings.request_timeout_seconds,
                        max_obj,
                        deadline,
                    ),
                    timeout=max(to_sec + 5.0, 10.0),
                )
                errors.extend(derr)
                for err in derr:
                    if err.get("object_type") is not None:
                        _log.warning(
                            "read_device_live_object_issue job_id=%s device_instance=%s "
                            "object_type=%s object_instance=%s message=%s",
                            job.job_id,
                            err.get("device_instance"),
                            err.get("object_type"),
                            err.get("object_instance"),
                            err.get("message"),
                        )
                data = live
                nob = len(live.get("objects", []))
                if live.get("truncated"):
                    summary = (
                        f"Read {live.get('returned_object_count', nob)}/"
                        f"{live.get('total_object_count', '?')} objects (truncated)"
                    )
                else:
                    summary = f"Read {nob} objects"
                if nob == 0:
                    status = "failed"
                    if derr and derr[0].get("message"):
                        summary = str(derr[0]["message"])
                    else:
                        summary = "read_device_live: no objects"
                elif derr:
                    status = "partial_success"
                else:
                    status = "success"
            except asyncio.TimeoutError as e:
                status = "failed"
                summary = "read_device_live timed out"
                errors.append({"message": str(e), "device_instance": dev})
                data = {
                    "device_instance": dev,
                    "read_at": utc_now_iso(),
                    "objects": [],
                }
            except (ErrorRejectAbortNack, Exception) as e:
                status = "failed"
                summary = "read_device_live failed"
                errors.append(
                    {
                        "message": str(e),
                        "device_instance": dev,
                        "traceback": traceback.format_exc(),
                    }
                )
                _log.exception("read_device_live job_id=%s", job.job_id)
                data = {
                    "device_instance": dev,
                    "read_at": utc_now_iso(),
                    "objects": [],
                }

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
                pe = rd.pop("_property_errors", None)
                if pe:
                    errors.extend(pe)
                data = rd
                summary = "Read OK"
                if rd.get("error"):
                    status = "failed"
                    errors.append({"message": str(rd["error"])})
                elif pe:
                    status = "partial_success"
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
            include_readback = bool(p.get("include_readback"))
            try:
                wr = await asyncio.wait_for(
                    bacnet.write_point(
                        dev,
                        ot,
                        oi,
                        val,
                        pri,
                        settings.request_timeout_seconds,
                        include_readback=include_readback,
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
    data = to_json_safe(data)
    return JobResultEnvelope(
        job_id=job.job_id,
        status=status,  # type: ignore[arg-type]
        started_at=started,
        finished_at=finished,
        summary=summary,
        data=data,
        errors=errors,
    )
