"""Async agent: heartbeat, config poll, job poll (one job at a time)."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
import traceback
from typing import Any, Optional, Union

import httpx
from bacpypes3.apdu import ErrorRejectAbortNack

from edge_agent.bacnet_client import BacnetPypesClient
from edge_agent.job_runner import run_job
from edge_agent.mock_bacnet_client import MockBacnetClient
from edge_agent.models import (
    BacnetClient,
    ConfigPullResponse,
    JobResultEnvelope,
    apply_float_tuning,
    merge_bacnet,
    utc_now_iso,
)
from edge_agent.saas_client import SaasClient
from edge_agent.settings import Settings
from edge_agent.storage import Storage

_log = logging.getLogger(__name__)


class _HeartbeatState:
    __slots__ = ("last_ok_at",)

    def __init__(self) -> None:
        self.last_ok_at: Optional[float] = None


def _local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return ""


def _load_effective(settings: Settings, storage: Storage):
    _rev, remote = storage.get_remote_config_state()
    return merge_bacnet(
        settings.bacnet_device_instance,
        settings.bacnet_bind_ip,
        settings.bacnet_udp_port,
        settings.bacnet_device_name,
        settings.bacnet_bind_prefix,
        settings.bacnet_vendor_identifier,
        settings.bacnet_iam_response_mode,
        remote,
    )


def _make_bacnet(settings: Settings, storage: Storage) -> Union[MockBacnetClient, BacnetPypesClient]:
    if settings.bacnet_mock:
        return MockBacnetClient()
    eff = _load_effective(settings, storage)
    return BacnetPypesClient(settings, eff, storage)


async def _ensure_bacnet_started(bacnet: Union[MockBacnetClient, BacnetPypesClient]) -> None:
    if isinstance(bacnet, BacnetPypesClient):
        await bacnet.start()


async def _stop_bacnet(bacnet: Union[MockBacnetClient, BacnetPypesClient]) -> None:
    if isinstance(bacnet, BacnetPypesClient):
        await bacnet.stop()


async def _apply_remote_config(
    settings: Settings,
    storage: Storage,
    bacnet: Union[MockBacnetClient, BacnetPypesClient],
    cfg: ConfigPullResponse,
) -> None:
    if cfg.unchanged or cfg.revision is None:
        return
    existing = storage.get_stored_remote_config_dict() or {}

    if cfg.bacnet is not None:
        bacnet_payload = cfg.bacnet.model_dump(exclude_none=True)
    else:
        prev_b = existing.get("bacnet")
        bacnet_payload = dict(prev_b) if isinstance(prev_b, dict) else {}

    if cfg.agent is not None:
        agent_payload = cfg.agent.model_dump(exclude_none=True)
    else:
        prev_a = existing.get("agent")
        agent_payload = dict(prev_a) if isinstance(prev_a, dict) else {}

    storage.save_remote_config(
        cfg.revision,
        cfg.updated_at or utc_now_iso(),
        bacnet_payload,
        agent_payload,
    )
    if isinstance(bacnet, BacnetPypesClient):
        eff = _load_effective(settings, storage)
        await bacnet.restart(eff)


async def _heartbeat_body(settings: Settings, storage: Storage) -> dict[str, Any]:
    rev, _ = storage.get_remote_config_state()
    return {
        "box_id": settings.box_id,
        "software_version": settings.software_version,
        "hostname": socket.gethostname(),
        "local_ip": _local_ip(),
        "uptime_seconds": int(time.monotonic()),
        "bacnet_config_revision": rev,
    }


async def _check_internet(settings: Settings) -> bool:
    """True when an HTTP(S) GET to `internet_check_url` returns 2xx (captures proxy/WAN)."""
    url = (settings.internet_check_url or "").strip()
    if not url:
        return False
    timeout = httpx.Timeout(settings.internet_check_timeout_seconds)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(url)
            return 200 <= r.status_code < 300
    except Exception as e:
        _log.debug("internet_check_failed err=%s", e)
        return False


async def _run_forever(settings: Settings) -> None:
    storage = Storage(settings.local_db_path)
    saas = SaasClient(settings)
    bacnet = _make_bacnet(settings, storage)
    await _ensure_bacnet_started(bacnet)
    if isinstance(bacnet, BacnetPypesClient):
        bacnet.set_agent_identity_csv(
            socket.gethostname(),
            settings.box_id,
            settings.saas_base,
        )

    job_lock = asyncio.Lock()
    hb_state = _HeartbeatState()

    async def heartbeat_task() -> None:
        while True:
            await asyncio.sleep(
                apply_float_tuning(
                    settings.heartbeat_interval_seconds,
                    storage.get_remote_agent_tuning(),
                    "heartbeat_interval_seconds",
                    10.0,
                    600.0,
                )
            )
            if await saas.heartbeat(await _heartbeat_body(settings, storage)):
                hb_state.last_ok_at = time.time()

    async def edge_status_task() -> None:
        while True:
            try:
                if isinstance(bacnet, BacnetPypesClient):
                    internet_ok = await _check_internet(settings)
                    now = time.time()
                    saas_ok = hb_state.last_ok_at is not None and (
                        now - hb_state.last_ok_at
                    ) < settings.saas_online_threshold_seconds
                    bacnet.update_edge_status_binary_inputs(internet_ok, saas_ok)
                    bacnet.update_agent_uptime_seconds(time.monotonic())
            except Exception as e:
                _log.warning("edge_status_update_failed err=%s", e)
            await asyncio.sleep(
                apply_float_tuning(
                    settings.edge_status_check_interval_seconds,
                    storage.get_remote_agent_tuning(),
                    "edge_status_check_interval_seconds",
                    5.0,
                    600.0,
                )
            )

    async def config_task() -> None:
        while True:
            await asyncio.sleep(
                apply_float_tuning(
                    settings.config_poll_interval_seconds,
                    storage.get_remote_agent_tuning(),
                    "config_poll_interval_seconds",
                    15.0,
                    3600.0,
                )
            )
            async with job_lock:
                rev, _ = storage.get_remote_config_state()
                cfg = await saas.fetch_config(rev)
                try:
                    await _apply_remote_config(settings, storage, bacnet, cfg)
                except Exception as e:
                    _log.warning("apply_config_failed err=%s", e)

    async def job_task() -> None:
        while True:
            await asyncio.sleep(
                apply_float_tuning(
                    settings.poll_interval_seconds,
                    storage.get_remote_agent_tuning(),
                    "poll_interval_seconds",
                    1.0,
                    120.0,
                )
            )
            async with job_lock:
                nj = await saas.next_job()
                job = nj.job
                if job is None:
                    continue
                started = utc_now_iso()
                if isinstance(bacnet, BacnetPypesClient):
                    bacnet.set_last_job_running(job.job_id, job.type)
                try:
                    envelope = await run_job(job, bacnet, storage, settings)
                except (ErrorRejectAbortNack, Exception) as e:
                    # BACnet Error* types subclass BaseException, not Exception.
                    _log.exception("run_job_crashed job_id=%s", job.job_id)
                    envelope = JobResultEnvelope(
                        job_id=job.job_id,
                        status="failed",
                        started_at=started,
                        finished_at=utc_now_iso(),
                        summary=f"Unhandled: {e}",
                        data={},
                        errors=[{"message": str(e), "traceback": traceback.format_exc()}],
                    )
                if isinstance(bacnet, BacnetPypesClient):
                    bacnet.set_last_job_finished(envelope)
                try:
                    await saas.post_result_idempotent(
                        job.job_id,
                        envelope.model_dump(),
                    )
                except Exception as e:
                    _log.error("post_result_failed job_id=%s err=%s", job.job_id, e)

    try:
        if await saas.heartbeat(await _heartbeat_body(settings, storage)):
            hb_state.last_ok_at = time.time()
        await asyncio.gather(heartbeat_task(), config_task(), job_task(), edge_status_task())
    finally:
        await _stop_bacnet(bacnet)
        await saas.aclose()
        storage.close()


def run(settings: Optional[Settings] = None) -> None:
    s = settings or Settings()
    from edge_agent.logging_setup import setup_logging

    setup_logging(s.log_level)
    _log.info(
        "edge_agent_config bacnet_mock=%s bacnet_iam_response_mode=%s bacnet_bind_ip=%r",
        s.bacnet_mock,
        s.bacnet_iam_response_mode,
        s.bacnet_bind_ip,
    )
    if s.bacnet_mock:
        _log.warning(
            "BACNET_MOCK=true: no real BACnet/IP stack — I-Am and discovery will not run on the LAN"
        )
    try:
        asyncio.run(_run_forever(s))
    except KeyboardInterrupt:
        _log.info("shutdown")
