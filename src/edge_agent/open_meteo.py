"""Open-Meteo forecast client (current conditions, no API key)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import httpx

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
SUNRISE_SUNSET_ORG_URL = "https://api.sunrise-sunset.org/json"

_CURRENT_VARS = ",".join(
    [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "is_day",
        "precipitation",
        "rain",
        "showers",
        "snowfall",
        "weather_code",
        "cloud_cover",
        "pressure_msl",
        "surface_pressure",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
    ]
)


@dataclass
class OpenMeteoResult:
    """
    Current conditions from Forecast API. Temperatures always °C from API (converted at BACnet layer).
    Wind speed: km/h when metric bundle, mph when imperial bundle (via wind_speed_unit).
    Precipitation / rain / showers: mm or inch (via precipitation_unit).
    Snowfall: cm (metric) or inch (imperial) per Open-Meteo.
    Pressures: always hPa from API (inHg at BACnet layer when imperial).
    """

    temperature_c: float
    apparent_temperature_c: float
    humidity_percent: float
    wind_speed: float
    wind_direction_deg: float
    wind_gust: float
    precipitation: float
    rain: float
    showers: float
    snowfall: float
    weather_code: int
    cloud_cover_percent: float
    pressure_msl_hpa: float
    surface_pressure_hpa: float
    is_day: bool
    fetch_ok: bool
    error: str


@dataclass
class SunTimesResult:
    """Today's sunrise/sunset in site-local form for BACnet strings."""

    sunrise_display: str
    sunset_display: str
    fetch_ok: bool
    error: str


def _bool_from_is_day(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    try:
        return int(v) != 0
    except (TypeError, ValueError):
        return False


def _failed(error: str) -> OpenMeteoResult:
    return OpenMeteoResult(
        temperature_c=0.0,
        apparent_temperature_c=0.0,
        humidity_percent=0.0,
        wind_speed=0.0,
        wind_direction_deg=0.0,
        wind_gust=0.0,
        precipitation=0.0,
        rain=0.0,
        showers=0.0,
        snowfall=0.0,
        weather_code=0,
        cloud_cover_percent=0.0,
        pressure_msl_hpa=0.0,
        surface_pressure_hpa=0.0,
        is_day=False,
        fetch_ok=False,
        error=error,
    )


def _failed_sun(msg: str) -> SunTimesResult:
    return SunTimesResult(sunrise_display="", sunset_display="", fetch_ok=False, error=msg)


def _fmt_local_iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


async def fetch_daily_sunrise_sunset(
    latitude: float,
    longitude: float,
    timezone_name: str,
    local_date_iso: str,
    *,
    timeout_seconds: float = 20.0,
) -> SunTimesResult:
    """
    Open-Meteo forecast ``daily=sunrise,sunset`` with ``timezone`` = IANA zone.
    Picks the row where ``daily.time`` matches ``local_date_iso`` (YYYY-MM-DD).
    Strings are site-local ISO-8601 with offset (seconds precision).
    """
    try:
        params: dict[str, Any] = {
            "latitude": latitude,
            "longitude": longitude,
            "timezone": timezone_name,
            "daily": "sunrise,sunset",
            "forecast_days": 3,
        }
        timeout = httpx.Timeout(timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(OPEN_METEO_FORECAST_URL, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return await _fetch_sunrise_sunset_org_fallback(
            latitude,
            longitude,
            timezone_name,
            local_date_iso,
            timeout_seconds=timeout_seconds,
            err=str(e) or type(e).__name__,
        )

    daily = data.get("daily") if isinstance(data, dict) else None
    if not isinstance(daily, dict):
        return await _fetch_sunrise_sunset_org_fallback(
            latitude,
            longitude,
            timezone_name,
            local_date_iso,
            timeout_seconds=timeout_seconds,
            err="no_daily",
        )

    times = daily.get("time")
    rises = daily.get("sunrise")
    sets = daily.get("sunset")
    if not (isinstance(times, list) and isinstance(rises, list) and isinstance(sets, list)):
        return await _fetch_sunrise_sunset_org_fallback(
            latitude,
            longitude,
            timezone_name,
            local_date_iso,
            timeout_seconds=timeout_seconds,
            err="daily_shape",
        )

    idx = None
    for i, t in enumerate(times):
        if isinstance(t, str) and t[:10] == local_date_iso[:10]:
            idx = i
            break
    if idx is None or idx >= len(rises) or idx >= len(sets):
        return await _fetch_sunrise_sunset_org_fallback(
            latitude,
            longitude,
            timezone_name,
            local_date_iso,
            timeout_seconds=timeout_seconds,
            err="no_row",
        )

    sr = rises[idx]
    ss = sets[idx]
    if not isinstance(sr, str) or not isinstance(ss, str):
        return await _fetch_sunrise_sunset_org_fallback(
            latitude,
            longitude,
            timezone_name,
            local_date_iso,
            timeout_seconds=timeout_seconds,
            err="bad_cell",
        )

    try:
        zi = ZoneInfo(timezone_name)
    except Exception:
        zi = ZoneInfo("UTC")

    def _norm(s: str) -> str:
        s = s.strip()
        if "T" not in s:
            return s
        try:
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(zi)
            else:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=zi)
                else:
                    dt = dt.astimezone(zi)
            return _fmt_local_iso(dt)
        except Exception:
            return s

    return SunTimesResult(
        sunrise_display=_norm(sr),
        sunset_display=_norm(ss),
        fetch_ok=True,
        error="",
    )


async def _fetch_sunrise_sunset_org_fallback(
    latitude: float,
    longitude: float,
    timezone_name: str,
    local_date_iso: str,
    *,
    timeout_seconds: float,
    err: str,
) -> SunTimesResult:
    """api.sunrise-sunset.org returns UTC ISO; convert to site zone."""
    try:
        zi = ZoneInfo(timezone_name)
    except Exception:
        return _failed_sun(f"tz:{err}")

    try:
        params = {
            "lat": latitude,
            "lng": longitude,
            "formatted": 0,
        }
        timeout = httpx.Timeout(timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(SUNRISE_SUNSET_ORG_URL, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return _failed_sun(f"{err}|fallback:{e}")

    res = data.get("results") if isinstance(data, dict) else None
    if not isinstance(res, dict):
        return _failed_sun(f"{err}|fallback_shape")

    sr_raw = res.get("sunrise")
    ss_raw = res.get("sunset")
    if not isinstance(sr_raw, str) or not isinstance(ss_raw, str):
        return _failed_sun(f"{err}|fallback_fields")

    try:
        sr_utc = datetime.fromisoformat(sr_raw.replace("Z", "+00:00"))
        ss_utc = datetime.fromisoformat(ss_raw.replace("Z", "+00:00"))
        sr_loc = sr_utc.astimezone(zi)
        ss_loc = ss_utc.astimezone(zi)
    except Exception as e:
        return _failed_sun(f"{err}|fallback_parse:{e}")

    return SunTimesResult(
        sunrise_display=_fmt_local_iso(sr_loc),
        sunset_display=_fmt_local_iso(ss_loc),
        fetch_ok=True,
        error="",
    )


def daylight_window_active(
    now_local: datetime,
    sunrise_display: str,
    sunset_display: str,
) -> bool:
    """True if ``now_local`` is >= sunrise and < sunset (same calendar day)."""
    try:
        if "T" not in sunrise_display or "T" not in sunset_display:
            return False
        sr = datetime.fromisoformat(sunrise_display)
        ss = datetime.fromisoformat(sunset_display)
        if sr.tzinfo is None or ss.tzinfo is None:
            return False
        return sr <= now_local < ss
    except Exception:
        return False


async def fetch_current_weather(
    latitude: float,
    longitude: float,
    *,
    imperial_bundle: bool = False,
    timeout_seconds: float = 20.0,
) -> OpenMeteoResult:
    """
    Fetch current weather. ``imperial_bundle`` matches SaaS F/US mode: mph, inches, inch snowfall.
    Metric uses km/h, mm, cm snow per https://open-meteo.com/en/docs
    """
    try:
        params: dict[str, Any] = {
            "latitude": latitude,
            "longitude": longitude,
            "current": _CURRENT_VARS,
            "wind_speed_unit": "mph" if imperial_bundle else "kmh",
            "precipitation_unit": "inch" if imperial_bundle else "mm",
        }
        timeout = httpx.Timeout(timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(OPEN_METEO_FORECAST_URL, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return _failed(str(e) or type(e).__name__)

    cur: Optional[dict[str, Any]] = None
    if isinstance(data, dict):
        cur = data.get("current")
    if not isinstance(cur, dict):
        return _failed("open_meteo_invalid_response")

    try:
        t = float(cur.get("temperature_2m", 0.0))
        at = float(cur.get("apparent_temperature", 0.0))
        rh = float(cur.get("relative_humidity_2m", 0.0))
        pr = float(cur.get("precipitation", 0.0))
        rn = float(cur.get("rain", 0.0))
        sh = float(cur.get("showers", 0.0))
        sn = float(cur.get("snowfall", 0.0))
        wc = int(cur.get("weather_code", 0))
        cc = float(cur.get("cloud_cover", 0.0))
        pm = float(cur.get("pressure_msl", 0.0))
        ps = float(cur.get("surface_pressure", 0.0))
        ws = float(cur.get("wind_speed_10m", 0.0))
        wd = float(cur.get("wind_direction_10m", 0.0))
        wg = float(cur.get("wind_gusts_10m", 0.0))
        is_day = _bool_from_is_day(cur.get("is_day", 0))
    except (TypeError, ValueError) as e:
        return _failed(str(e) or "parse_error")

    return OpenMeteoResult(
        temperature_c=t,
        apparent_temperature_c=at,
        humidity_percent=rh,
        wind_speed=ws,
        wind_direction_deg=wd,
        wind_gust=wg,
        precipitation=pr,
        rain=rn,
        showers=sh,
        snowfall=sn,
        weather_code=wc,
        cloud_cover_percent=cc,
        pressure_msl_hpa=pm,
        surface_pressure_hpa=ps,
        is_day=is_day,
        fetch_ok=True,
        error="",
    )
