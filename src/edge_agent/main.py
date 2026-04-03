"""Async agent: heartbeat, config poll, job poll (one job at a time)."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
import traceback
from datetime import date
from typing import Any, Optional, Union

import httpx
from bacpypes3.apdu import ErrorRejectAbortNack

from edge_agent.bacnet_client import BacnetPypesClient
from edge_agent.job_runner import run_job
from edge_agent.mock_bacnet_client import MockBacnetClient
from edge_agent.holidays import HolidayEval, evaluate_holidays_for_local_date, load_public_holidays_year
from edge_agent.models import (
    BacnetClient,
    ConfigPullResponse,
    JobResultEnvelope,
    apply_float_tuning,
    merge_bacnet,
    remote_weather_master_enabled,
    use_fahrenheit_from_tuning,
    utc_now_iso,
    weather_coords_valid,
)
from edge_agent.open_meteo import SunTimesResult, fetch_current_weather, fetch_daily_sunrise_sunset
from edge_agent.open_meteo_air_quality import fetch_current_air_quality
from edge_agent.saas_client import SaasClient
from edge_agent.settings import Settings
from edge_agent.site_time import get_local_time_info
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

    _, remote_before = storage.get_remote_config_state()
    eff_before = merge_bacnet(
        settings.bacnet_device_instance,
        settings.bacnet_bind_ip,
        settings.bacnet_udp_port,
        settings.bacnet_device_name,
        settings.bacnet_bind_prefix,
        settings.bacnet_vendor_identifier,
        settings.bacnet_iam_response_mode,
        remote_before,
    )

    storage.save_remote_config(
        cfg.revision,
        cfg.updated_at or utc_now_iso(),
        bacnet_payload,
        agent_payload,
    )

    eff_after = _load_effective(settings, storage)
    _log.info(
        "bacnet_config_applied revision=%s iam_response_mode=%s",
        cfg.revision,
        eff_after.iam_response_mode,
    )

    if isinstance(bacnet, BacnetPypesClient):
        if eff_before.iam_response_mode != eff_after.iam_response_mode:
            _log.info(
                "bacnet_restarting_due_to_config_change old_iam_response_mode=%s new_iam_response_mode=%s",
                eff_before.iam_response_mode,
                eff_after.iam_response_mode,
            )
        _log.info(
            "bacnet_stack_restarting reason=config_revision revision=%s",
            cfg.revision,
        )
        await bacnet.restart(eff_after)
        bacnet.set_agent_identity_csv(
            socket.gethostname(),
            settings.box_id,
            settings.saas_base,
        )
        bacnet.set_weather_polling_enabled_from_config(storage.get_remote_agent_tuning())


async def _heartbeat_body(settings: Settings, storage: Storage) -> dict[str, Any]:
    rev, _ = storage.get_remote_config_state()
    eff = _load_effective(settings, storage)
    return {
        "box_id": settings.box_id,
        "software_version": settings.software_version,
        "hostname": socket.gethostname(),
        "local_ip": _local_ip(),
        "uptime_seconds": int(time.monotonic()),
        "bacnet_config_revision": rev,
        "iam_response_mode": eff.iam_response_mode,
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
        bacnet.set_weather_polling_enabled_from_config(storage.get_remote_agent_tuning())

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

    async def weather_poll_task() -> None:
        while True:
            try:
                tuning = storage.get_remote_agent_tuning()
                interval = apply_float_tuning(
                    settings.weather_poll_interval_seconds,
                    tuning,
                    "weather_poll_interval_seconds",
                    900.0,
                    3600.0,
                )
                if isinstance(bacnet, BacnetPypesClient):
                    if not remote_weather_master_enabled(tuning):
                        await asyncio.sleep(interval)
                        continue
                    if not bacnet.is_weather_polling_bv_active():
                        await asyncio.sleep(interval)
                        continue
                    if tuning is None or tuning.weather_latitude is None or tuning.weather_longitude is None:
                        await asyncio.sleep(interval)
                        continue
                    lat = float(tuning.weather_latitude)
                    lon = float(tuning.weather_longitude)
                    tmo = min(30.0, float(settings.request_timeout_seconds))
                    imperial = use_fahrenheit_from_tuning(tuning)
                    wx_result, aq_result = await asyncio.gather(
                        fetch_current_weather(
                            lat, lon, imperial_bundle=imperial, timeout_seconds=tmo
                        ),
                        fetch_current_air_quality(lat, lon, timeout_seconds=tmo),
                    )
                    # Each updater keeps last good analogs when its fetch fails (partial success).
                    bacnet.update_weather(wx_result, imperial)
                    bacnet.update_air_quality(aq_result)
                    bacnet.update_outdoor_decision_points(wx_result, aq_result, imperial)
            except Exception as e:
                _log.debug("weather_poll_failed err=%s", e)
            await asyncio.sleep(
                apply_float_tuning(
                    settings.weather_poll_interval_seconds,
                    storage.get_remote_agent_tuning(),
                    "weather_poll_interval_seconds",
                    900.0,
                    3600.0,
                )
            )

    async def site_time_task() -> None:
        """Site-local time from system UTC + offline IANA zone (weather_latitude/longitude)."""
        while True:
            tuning = storage.get_remote_agent_tuning()
            interval = apply_float_tuning(
                settings.site_time_poll_interval_seconds,
                tuning,
                "site_time_poll_interval_seconds",
                1.0,
                3600.0,
            )
            try:
                if isinstance(bacnet, BacnetPypesClient):
                    lat = tuning.weather_latitude if tuning else None
                    lon = tuning.weather_longitude if tuning else None
                    info = get_local_time_info(lat, lon)
                    bacnet.update_site_time(info)
            except Exception as e:
                _log.warning("site_time_update_failed err=%s", e)
            await asyncio.sleep(interval)

    async def schedule_context_task() -> None:
        """Public holidays (Nager.Date) + sunrise/sunset (Open-Meteo daily, fallback API)."""
        while True:
            tuning = storage.get_remote_agent_tuning()
            interval = apply_float_tuning(
                settings.schedule_context_poll_interval_seconds,
                tuning,
                "schedule_context_poll_interval_seconds",
                30.0,
                3600.0,
            )
            try:
                if isinstance(bacnet, BacnetPypesClient):
                    lat = tuning.weather_latitude if tuning else None
                    lon = tuning.weather_longitude if tuning else None
                    country = tuning.site_country_code if tuning else None
                    info = get_local_time_info(lat, lon)
                    tmo = min(30.0, float(settings.request_timeout_seconds))
                    if not info.ok:
                        bacnet.update_schedule_context(
                            info,
                            HolidayEval(
                                holiday_today=False,
                                holiday_name="",
                                business_day=False,
                                long_weekend=False,
                                holiday_api_ok=False,
                                error=info.error or "site_time_unavailable",
                            ),
                            SunTimesResult("", "", False, info.error or "site_time_unavailable"),
                        )
                        await asyncio.sleep(interval)
                        continue

                    d = date(info.year, info.month, info.day)
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(tmo), follow_redirects=True
                    ) as http:
                        hlist: Optional[list] = None
                        hok = False
                        herr = ""
                        if country:
                            hlist, hok, herr = await load_public_holidays_year(
                                country,
                                d.year,
                                client=http,
                                timeout_seconds=tmo,
                            )
                        hev = evaluate_holidays_for_local_date(
                            country,
                            d,
                            info.weekday_number,
                            hlist if hok else None,
                            load_ok=hok,
                            load_error=herr,
                        )
                        sun = SunTimesResult("", "", False, "skip")
                        if (
                            weather_coords_valid(lat, lon)
                            and info.timezone_name
                            and isinstance(info.timezone_name, str)
                        ):
                            sun = await fetch_daily_sunrise_sunset(
                                float(lat),
                                float(lon),
                                info.timezone_name,
                                info.local_date_iso,
                                timeout_seconds=tmo,
                            )
                        bacnet.update_schedule_context(info, hev, sun)
            except Exception as e:
                _log.debug("schedule_context_failed err=%s", e)
            await asyncio.sleep(interval)

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
        await asyncio.gather(
            heartbeat_task(),
            config_task(),
            job_task(),
            edge_status_task(),
            weather_poll_task(),
            site_time_task(),
            schedule_context_task(),
        )
    finally:
        await _stop_bacnet(bacnet)
        await saas.aclose()
        storage.close()


def run(settings: Optional[Settings] = None) -> None:
    s = settings or Settings()
    from edge_agent.logging_setup import setup_logging

    setup_logging(s.log_level)
    boot_storage = Storage(s.local_db_path)
    try:
        boot_eff = _load_effective(s, boot_storage)
        _log.info(
            "edge_agent_config bacnet_mock=%s iam_response_mode=%s (effective merged .env+remote) env_bacnet_iam_response_mode=%s bacnet_bind_ip=%r",
            s.bacnet_mock,
            boot_eff.iam_response_mode,
            s.bacnet_iam_response_mode,
            s.bacnet_bind_ip,
        )
    finally:
        boot_storage.close()
    if s.bacnet_mock:
        _log.warning(
            "BACNET_MOCK=true: no real BACnet/IP stack — I-Am and discovery will not run on the LAN"
        )
    try:
        asyncio.run(_run_forever(s))
    except KeyboardInterrupt:
        _log.info("shutdown")
