"""Dispatch SaaS jobs to BACnet handlers."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import time
import traceback
from typing import Any, Optional

from bacpypes3.apdu import ErrorRejectAbortNack

from edge_agent.json_safe import failure_message, to_json_safe
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


def _sanitize_job_result_messages(data: Any, errors: list[dict[str, Any]]) -> None:
    """Force non-empty strings so JSON never carries null error/message (SaaS/UI)."""
    if isinstance(data, dict):
        for row in data.get("write_results") or []:
            if isinstance(row, dict) and row.get("ok") is not True:
                row["error"] = failure_message(
                    row.get("error"),
                    default=f"write failed (index {row.get('index')})",
                )
    for err in errors:
        if isinstance(err, dict) and "message" in err:
            err["message"] = failure_message(
                err.get("message"),
                default="error (no message text)",
            )


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
            arr_ix = p.get("array_index")
            arr_ix_i: Optional[int] = None
            if arr_ix is not None:
                arr_ix_i = int(arr_ix)
            try:
                rd = await asyncio.wait_for(
                    bacnet.read_point(
                        dev,
                        ot,
                        oi,
                        prop,
                        settings.request_timeout_seconds,
                        array_index=arr_ix_i,
                    ),
                    timeout=settings.request_timeout_seconds + 5.0,
                )
                pe = rd.pop("_property_errors", None)
                if pe:
                    errors.extend(pe)
                data = rd
                summary = "Read OK"
                if rd.get("error"):
                    status = "failed"
                    errors.append(
                        {
                            "message": failure_message(
                                rd["error"], default="read_point failed"
                            ),
                        }
                    )
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
            include_readback = bool(p.get("include_readback"))
            rb_props = p.get("readback_properties")
            if rb_props is not None and not isinstance(rb_props, list):
                rb_props = None
            writes_list = p.get("writes")

            if isinstance(writes_list, list) and len(writes_list) > 0:
                try:
                    wr = await asyncio.wait_for(
                        bacnet.write_point_multi(
                            dev,
                            ot,
                            oi,
                            writes_list,
                            settings.request_timeout_seconds,
                            include_readback=include_readback,
                            readback_properties=rb_props,
                        ),
                        timeout=settings.request_timeout_seconds + 5.0,
                    )
                    data = wr
                    if wr.get("error") and not wr.get("write_results"):
                        status = "failed"
                        summary = failure_message(
                            wr["error"], default="write_point failed"
                        )
                        errors.append({"message": summary, "device_instance": dev})
                    else:
                        results = wr.get("write_results", [])
                        ok_c = sum(1 for r in results if r.get("ok"))
                        fail_c = len(results) - ok_c
                        for r in results:
                            if not r.get("ok"):
                                werr = failure_message(
                                    r.get("error"),
                                    default=f"write failed (index {r.get('index')})",
                                )
                                errors.append(
                                    {
                                        "device_instance": dev,
                                        "object_type": ot,
                                        "object_instance": oi,
                                        "write_index": r.get("index"),
                                        "property": r.get("property"),
                                        "bacnet_property": r.get("bacnet_property"),
                                        "message": werr,
                                    }
                                )
                        if fail_c == 0:
                            status = "success"
                            summary = f"Write OK ({ok_c} properties)"
                        elif ok_c == 0:
                            status = "failed"
                            summary = f"All {fail_c} writes failed"
                        else:
                            status = "partial_success"
                            summary = f"Partial write: {ok_c} ok, {fail_c} failed"
                    storage.append_write_audit(
                        job.job_id,
                        {
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                            "writes": writes_list,
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
                        "write_mode": "multi",
                        "write_results": [],
                    }
                    errors.append({"message": str(e), "traceback": traceback.format_exc()})
                    storage.append_write_audit(
                        job.job_id,
                        {
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                            "writes": writes_list,
                            "outcome": "failed",
                            "detail": str(e),
                        },
                    )
            else:
                val = p["value"]
                pri: Optional[int] = None
                if p.get("priority") is not None:
                    pri = int(p["priority"])
                if val is None and pri is None:
                    status = "failed"
                    summary = "present-value relinquish requires priority 1-16"
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                        }
                    )
                    data = {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "property": "presentValue",
                        "value": val,
                        "priority": pri,
                        "error": summary,
                    }
                elif pri is not None and (pri < 1 or pri > 16):
                    status = "failed"
                    summary = "priority must be 1-16"
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                        }
                    )
                    data = {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "property": "presentValue",
                        "value": val,
                        "priority": pri,
                        "error": summary,
                    }
                else:
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
                            wmsg = failure_message(
                                wr.get("error"), default="Write failed"
                            )
                            errors.append({"message": wmsg})
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
                        errors.append(
                            {"message": str(e), "traceback": traceback.format_exc()}
                        )
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

        elif job.type == "atomic_read_file":
            p = job.payload
            dev = int(p["device_instance"])
            ot = str(p.get("object_type") or "file")
            oi = int(p["object_instance"])
            chunk_size = int(p.get("read_chunk_size") or p.get("chunk_size") or 200)
            expected_len = p.get("byte_length")
            max_len = int(p.get("max_byte_length") or (2 * 1024 * 1024))

            try:
                if ot.lower() != "file":
                    raise ValueError("atomic_read_file object_type must be file")
                if max_len < 1 or max_len > 2 * 1024 * 1024:
                    raise ValueError("max_byte_length must be between 1 and 2097152")

                read_timeout = max(settings.request_timeout_seconds, 60.0)
                arf = await asyncio.wait_for(
                    bacnet.atomic_read_file(
                        dev,
                        ot,
                        oi,
                        settings.request_timeout_seconds,
                        chunk_size=chunk_size,
                        expected_length=int(expected_len) if expected_len is not None else None,
                    ),
                    timeout=read_timeout,
                )
                raw = arf.pop("file_data", b"")
                if isinstance(raw, bytearray):
                    raw = bytes(raw)
                if not isinstance(raw, bytes):
                    raise ValueError("AtomicReadFile returned non-bytes file_data")
                if len(raw) > max_len:
                    raise ValueError("AtomicReadFile result exceeds max_byte_length")

                data = {
                    **arf,
                    "file_b64": base64.b64encode(raw).decode("ascii"),
                    "byte_length": len(raw),
                    "file_sha256": hashlib.sha256(raw).hexdigest(),
                }
                if arf.get("error"):
                    status = "failed"
                    summary = failure_message(
                        arf.get("error"), default="atomic_read_file failed"
                    )
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                        }
                    )
                else:
                    status = "success"
                    summary = f"AtomicReadFile OK ({len(raw)} bytes)"
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "byte_length": len(raw),
                        "file_sha256": data["file_sha256"],
                        "outcome": status,
                        "detail": {k: v for k, v in data.items() if k != "file_b64"},
                    },
                )
            except (ErrorRejectAbortNack, Exception) as e:
                status = "failed"
                summary = "atomic_read_file failed"
                data = {
                    "device_instance": dev,
                    "object_type": ot,
                    "object_instance": oi,
                }
                errors.append({"message": str(e), "traceback": traceback.format_exc()})
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "outcome": "failed",
                        "detail": str(e),
                    },
                )

        elif job.type == "atomic_write_file":
            p = job.payload
            dev = int(p["device_instance"])
            ot = str(p.get("object_type") or "file")
            oi = int(p["object_instance"])
            include_readback = bool(p.get("include_readback", True))
            chunk_size = int(p.get("chunk_size") or 100)
            read_chunk_size = int(p.get("read_chunk_size") or 200)
            expected_len = p.get("byte_length")
            expected_sha = str(p.get("file_sha256") or "").strip().lower()
            b64 = p.get("file_b64", p.get("file_data_base64"))

            try:
                if ot.lower() != "file":
                    raise ValueError("atomic_write_file object_type must be file")
                if not isinstance(b64, str) or b64.strip() == "":
                    raise ValueError("file_b64 is required")
                file_data = base64.b64decode(b64, validate=True)
                if len(file_data) > 2 * 1024 * 1024:
                    raise ValueError("file_b64 exceeds first-cut 2 MiB limit")
                if expected_len is not None and len(file_data) != int(expected_len):
                    raise ValueError(
                        f"byte_length mismatch: payload={int(expected_len)} decoded={len(file_data)}"
                    )
                actual_sha = hashlib.sha256(file_data).hexdigest()
                if expected_sha and actual_sha != expected_sha:
                    raise ValueError("file_sha256 mismatch")

                chunk_count = max(1, (len(file_data) + max(1, chunk_size) - 1) // max(1, chunk_size))
                read_count = (
                    max(1, (len(file_data) + max(1, read_chunk_size) - 1) // max(1, read_chunk_size))
                    if include_readback
                    else 0
                )
                awf_timeout = max(
                    settings.request_timeout_seconds * float(chunk_count + read_count) + 10.0,
                    60.0,
                )
                awf = await asyncio.wait_for(
                    bacnet.atomic_write_file(
                        dev,
                        ot,
                        oi,
                        file_data,
                        chunk_size,
                        settings.request_timeout_seconds,
                        include_readback=include_readback,
                        read_chunk_size=read_chunk_size,
                    ),
                    timeout=awf_timeout,
                )
                data = awf
                if awf.get("error"):
                    status = "failed"
                    summary = failure_message(
                        awf.get("error"), default="atomic_write_file failed"
                    )
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                        }
                    )
                elif include_readback and awf.get("verified") is not True:
                    status = "failed"
                    summary = "AtomicWriteFile readback verification failed"
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                        }
                    )
                else:
                    status = "success"
                    summary = f"AtomicWriteFile OK ({awf.get('bytes_written', len(file_data))} bytes)"
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "byte_length": len(file_data),
                        "file_sha256": actual_sha,
                        "outcome": status,
                        "detail": awf,
                    },
                )
            except (ErrorRejectAbortNack, Exception) as e:
                status = "failed"
                summary = "atomic_write_file failed"
                data = {
                    "device_instance": dev,
                    "object_type": ot,
                    "object_instance": oi,
                }
                errors.append({"message": str(e), "traceback": traceback.format_exc()})
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "outcome": "failed",
                        "detail": str(e),
                    },
                )

        elif job.type == "write_schedule":
            p = job.payload
            dev = int(p["device_instance"])
            ot = str(p.get("object_type") or "schedule")
            oi = int(p["object_instance"])
            schedule = p.get("schedule")
            include_readback = bool(p.get("include_readback", True))
            try:
                if ot.lower().replace("_", "-") != "schedule":
                    raise ValueError("write_schedule object_type must be schedule")
                if not isinstance(schedule, dict):
                    raise ValueError("write_schedule schedule must be an object")
                result = await asyncio.wait_for(
                    bacnet.write_schedule(
                        dev,
                        oi,
                        schedule,
                        settings.request_timeout_seconds,
                        include_readback=include_readback,
                    ),
                    timeout=max(settings.request_timeout_seconds * 20.0, 120.0),
                )
                data = result
                if result.get("error"):
                    status = "failed"
                    summary = failure_message(result["error"], default="write_schedule failed")
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                        }
                    )
                elif include_readback and result.get("verified") is not True:
                    status = "failed"
                    summary = "Schedule readback verification failed"
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                            "differences": result.get("differences", {}),
                        }
                    )
                else:
                    status = "success"
                    summary = (
                        "BACnet schedule written and verified"
                        if include_readback
                        else "BACnet schedule written"
                    )
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "outcome": status,
                        "detail": result,
                    },
                )
            except (ErrorRejectAbortNack, Exception) as exc:
                status = "failed"
                summary = "write_schedule failed"
                data = {
                    "device_instance": dev,
                    "object_type": ot,
                    "object_instance": oi,
                }
                errors.append({"message": str(exc), "traceback": traceback.format_exc()})
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "outcome": "failed",
                        "detail": str(exc),
                    },
                )

        elif job.type == "write_calendar":
            p = job.payload
            dev = int(p["device_instance"])
            ot = str(p.get("object_type") or "calendar")
            oi = int(p["object_instance"])
            calendar = p.get("calendar")
            include_readback = bool(p.get("include_readback", True))
            create_if_missing = bool(p.get("create_if_missing", True))
            try:
                if ot.lower().replace("_", "-") != "calendar":
                    raise ValueError("write_calendar object_type must be calendar")
                if not isinstance(calendar, dict):
                    raise ValueError("write_calendar calendar must be an object")
                result = await asyncio.wait_for(
                    bacnet.write_calendar(
                        dev,
                        oi,
                        calendar,
                        settings.request_timeout_seconds,
                        include_readback=include_readback,
                        create_if_missing=create_if_missing,
                    ),
                    timeout=max(settings.request_timeout_seconds * 8.0, 60.0),
                )
                data = result
                if result.get("error"):
                    status = "failed"
                    summary = failure_message(result["error"], default="write_calendar failed")
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                        }
                    )
                elif include_readback and result.get("verified") is not True:
                    status = "failed"
                    summary = "Calendar readback verification failed"
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                            "differences": result.get("differences", {}),
                        }
                    )
                else:
                    status = "success"
                    summary = (
                        "BACnet calendar written and verified"
                        if include_readback
                        else "BACnet calendar written"
                    )
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "outcome": status,
                        "detail": result,
                    },
                )
            except (ErrorRejectAbortNack, Exception) as exc:
                status = "failed"
                summary = "write_calendar failed"
                data = {
                    "device_instance": dev,
                    "object_type": ot,
                    "object_instance": oi,
                }
                errors.append({"message": str(exc), "traceback": traceback.format_exc()})
                storage.append_write_audit(
                    job.job_id,
                    {
                        "device_instance": dev,
                        "object_type": ot,
                        "object_instance": oi,
                        "outcome": "failed",
                        "detail": str(exc),
                    },
                )

        elif job.type == "create_object":
            p = job.payload
            dev = int(p["device_instance"])
            ot = str(p["object_type"])
            oi_raw = p.get("object_instance")
            oi: Optional[int] = int(oi_raw) if oi_raw is not None else None
            init = p.get("initial_properties")
            if init is not None and not isinstance(init, list):
                status = "failed"
                summary = "initial_properties must be a list or omitted"
                errors.append({"message": summary, "device_instance": dev})
                data = {
                    "device_instance": dev,
                    "object_type": ot,
                    "error": summary,
                }
            else:
                init_list: list[dict[str, Any]] = init if isinstance(init, list) else []
                try:
                    cr = await asyncio.wait_for(
                        bacnet.create_object(
                            dev,
                            ot,
                            oi,
                            init_list,
                            settings.request_timeout_seconds,
                        ),
                        timeout=settings.request_timeout_seconds + 5.0,
                    )
                    data = cr
                    if cr.get("error"):
                        status = "failed"
                        summary = failure_message(
                            cr.get("error"), default="create_object failed"
                        )
                        errors.append({"message": summary, "device_instance": dev})
                    elif cr.get("already_exists"):
                        status = "success"
                        summary = "Object already exists"
                    else:
                        status = "success"
                        summary = "Object created"
                except (ErrorRejectAbortNack, Exception) as e:
                    status = "failed"
                    summary = "create_object failed"
                    data = {
                        "device_instance": dev,
                        "object_type": ot,
                    }
                    if oi is not None:
                        data["object_instance"] = oi
                    errors.append(
                        {"message": str(e), "traceback": traceback.format_exc()}
                    )
                    _log.exception("create_object job_id=%s", job.job_id)

        elif job.type == "delete_object":
            p = job.payload
            dev = int(p["device_instance"])
            ot = str(p["object_type"])
            oi = int(p["object_instance"])
            try:
                dr = await asyncio.wait_for(
                    bacnet.delete_object(
                        dev,
                        ot,
                        oi,
                        settings.request_timeout_seconds,
                    ),
                    timeout=settings.request_timeout_seconds + 5.0,
                )
                data = dr
                if dr.get("error"):
                    status = "failed"
                    summary = failure_message(
                        dr.get("error"), default="delete_object failed"
                    )
                    errors.append(
                        {
                            "message": summary,
                            "device_instance": dev,
                            "object_type": ot,
                            "object_instance": oi,
                        }
                    )
                else:
                    status = "success"
                    summary = "Object deleted"
            except (ErrorRejectAbortNack, Exception) as e:
                status = "failed"
                summary = "delete_object failed"
                data = {
                    "device_instance": dev,
                    "object_type": ot,
                    "object_instance": oi,
                }
                errors.append(
                    {"message": str(e), "traceback": traceback.format_exc()}
                )
                _log.exception("delete_object job_id=%s", job.job_id)

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
    _sanitize_job_result_messages(data, errors)
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
