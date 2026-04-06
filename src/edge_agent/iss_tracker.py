"""ISS position / pass awareness (novelty only; no BAS control).

Uses free public APIs:
- https://api.wheretheiss.at/v1/satellites/25544 — current lat/lon/altitude/velocity
- http://api.open-notify.org/iss-pass.json — next pass risetime + duration (optional)

Distance is great-circle on the WGS84 sphere. ``overhead_now`` uses a fixed ground-track
proximity threshold (not visibility or horizon).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from edge_agent.models import weather_coords_valid

_log = logging.getLogger(__name__)

_WHERE_THE_ISS_URL = "https://api.wheretheiss.at/v1/satellites/25544"
_OPEN_NOTIFY_PASS_URL = "http://api.open-notify.org/iss-pass.json"

# Ground-track "nearby / overhead-ish": great-circle distance from site subsatellite point.
# ~750 km is a coarse band (not naked-eye, not precise pass geometry). Documented constant only.
ISS_OVERHEAD_THRESHOLD_KM = 750.0

_EARTH_RADIUS_KM = 6371.0

# BACnet display conversions when SaaS imperial bundle is active (same flag as weather °F).
_KM_TO_FEET = 3280.839895013123
_KPH_TO_MPH = 0.621371192237334


def iss_distance_for_bacnet(distance_km: float, imperial: bool) -> float:
    """Great-circle distance: km (metric) or feet (imperial; same factor as altitude, BACnet has no miles)."""
    return float(distance_km) * _KM_TO_FEET if imperial else float(distance_km)


def iss_altitude_for_bacnet(altitude_km: float, imperial: bool) -> float:
    """Altitude: km (metric) or feet (imperial, aviation-style)."""
    return float(altitude_km) * _KM_TO_FEET if imperial else float(altitude_km)


def iss_velocity_for_bacnet(velocity_kph: float, imperial: bool) -> float:
    """Speed: km/h (metric) or mph (imperial)."""
    return float(velocity_kph) * _KPH_TO_MPH if imperial else float(velocity_kph)


@dataclass(frozen=True)
class IssPositionResult:
    """Current ISS state relative to the configured site (weather lat/lon)."""

    ok: bool
    error: str
    timestamp_utc: str
    iss_latitude: float
    iss_longitude: float
    iss_altitude_km: float
    iss_velocity_kph: float
    distance_to_site_km: float
    overhead_now: bool


@dataclass(frozen=True)
class IssPassFetchResult:
    """Raw next-pass row from Open Notify (optional)."""

    ok: bool
    error: str
    next_risetime_unix: Optional[int]
    duration_seconds: float


def compute_iss_distance_km(site_lat: float, site_lon: float, iss_lat: float, iss_lon: float) -> float:
    """Great-circle distance in kilometers (haversine)."""
    p1 = math.radians(site_lat)
    p2 = math.radians(iss_lat)
    dp = math.radians(iss_lat - site_lat)
    dl = math.radians(iss_lon - site_lon)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return _EARTH_RADIUS_KM * c


def is_iss_nearby(distance_km: float, threshold_km: float) -> bool:
    return distance_km <= threshold_km


def format_pass_risetime_site_local(risetime_unix: Optional[int], iana_tz: Optional[str]) -> str:
    """Format next pass start as ISO string in site zone when possible, else UTC."""
    if risetime_unix is None:
        return ""
    try:
        dt_utc = datetime.fromtimestamp(int(risetime_unix), tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return ""
    if iana_tz:
        try:
            from zoneinfo import ZoneInfo

            return dt_utc.astimezone(ZoneInfo(iana_tz)).isoformat(timespec="seconds")
        except Exception:
            pass
    return dt_utc.replace(microsecond=0).isoformat()


def _coerce_velocity_kph(raw: float) -> float:
    """API documents km/h; if a tiny value appears, treat as km/s and scale."""
    v = float(raw)
    if abs(v) < 500.0:
        return v * 3600.0
    return v


def _parse_iss_json(data: dict[str, Any], site_lat: float, site_lon: float) -> IssPositionResult:
    try:
        lat = float(data["latitude"])
        lon = float(data["longitude"])
        alt = float(data["altitude"])
        vel = _coerce_velocity_kph(float(data["velocity"]))
    except (KeyError, TypeError, ValueError) as e:
        return IssPositionResult(
            ok=False,
            error=f"iss_parse:{e}",
            timestamp_utc="",
            iss_latitude=0.0,
            iss_longitude=0.0,
            iss_altitude_km=0.0,
            iss_velocity_kph=0.0,
            distance_to_site_km=0.0,
            overhead_now=False,
        )

    ts_raw = data.get("timestamp")
    ts_utc = ""
    if isinstance(ts_raw, (int, float)):
        try:
            ts_utc = (
                datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
                .replace(microsecond=0)
                .isoformat()
            )
        except (OSError, OverflowError, ValueError):
            ts_utc = str(ts_raw)

    dist = compute_iss_distance_km(site_lat, site_lon, lat, lon)
    overhead = is_iss_nearby(dist, ISS_OVERHEAD_THRESHOLD_KM)

    return IssPositionResult(
        ok=True,
        error="",
        timestamp_utc=ts_utc,
        iss_latitude=lat,
        iss_longitude=lon,
        iss_altitude_km=alt,
        iss_velocity_kph=vel,
        distance_to_site_km=dist,
        overhead_now=overhead,
    )


async def fetch_iss_position(
    site_lat: float,
    site_lon: float,
    timeout_seconds: float,
) -> IssPositionResult:
    """GET current ISS state from wheretheiss.at."""
    if not weather_coords_valid(site_lat, site_lon):
        return IssPositionResult(
            ok=False,
            error="no_site_coordinates",
            timestamp_utc="",
            iss_latitude=0.0,
            iss_longitude=0.0,
            iss_altitude_km=0.0,
            iss_velocity_kph=0.0,
            distance_to_site_km=0.0,
            overhead_now=False,
        )

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=True,
        ) as client:
            r = await client.get(_WHERE_THE_ISS_URL)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        _log.debug("iss_position_fetch_failed err=%s", e)
        return IssPositionResult(
            ok=False,
            error=f"iss_http:{e}",
            timestamp_utc="",
            iss_latitude=0.0,
            iss_longitude=0.0,
            iss_altitude_km=0.0,
            iss_velocity_kph=0.0,
            distance_to_site_km=0.0,
            overhead_now=False,
        )

    if not isinstance(data, dict):
        return IssPositionResult(
            ok=False,
            error="iss_bad_json",
            timestamp_utc="",
            iss_latitude=0.0,
            iss_longitude=0.0,
            iss_altitude_km=0.0,
            iss_velocity_kph=0.0,
            distance_to_site_km=0.0,
            overhead_now=False,
        )

    return _parse_iss_json(data, float(site_lat), float(site_lon))


async def fetch_next_iss_pass(
    site_lat: float,
    site_lon: float,
    timeout_seconds: float,
) -> IssPassFetchResult:
    """GET next ISS pass window from Open Notify (HTTP, no key)."""
    if not weather_coords_valid(site_lat, site_lon):
        return IssPassFetchResult(
            ok=False,
            error="no_site_coordinates",
            next_risetime_unix=None,
            duration_seconds=0.0,
        )

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=True,
        ) as client:
            r = await client.get(
                _OPEN_NOTIFY_PASS_URL,
                params={"lat": float(site_lat), "lon": float(site_lon), "n": 3},
            )
            r.raise_for_status()
            body = r.json()
    except Exception as e:
        _log.debug("iss_pass_fetch_failed err=%s", e)
        return IssPassFetchResult(
            ok=False,
            error=f"pass_http:{e}",
            next_risetime_unix=None,
            duration_seconds=0.0,
        )

    if not isinstance(body, dict) or body.get("message") != "success":
        return IssPassFetchResult(
            ok=False,
            error="pass_api_unavailable",
            next_risetime_unix=None,
            duration_seconds=0.0,
        )

    resp = body.get("response")
    if not isinstance(resp, list) or not resp:
        return IssPassFetchResult(
            ok=False,
            error="pass_no_upcoming",
            next_risetime_unix=None,
            duration_seconds=0.0,
        )

    first = resp[0]
    if not isinstance(first, dict):
        return IssPassFetchResult(
            ok=False,
            error="pass_bad_row",
            next_risetime_unix=None,
            duration_seconds=0.0,
        )

    try:
        risetime = int(first["risetime"])
        duration = float(first.get("duration", 0))
    except (KeyError, TypeError, ValueError):
        return IssPassFetchResult(
            ok=False,
            error="pass_parse",
            next_risetime_unix=None,
            duration_seconds=0.0,
        )

    return IssPassFetchResult(
        ok=True,
        error="",
        next_risetime_unix=risetime,
        duration_seconds=duration,
    )
