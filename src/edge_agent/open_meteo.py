"""Open-Meteo forecast client (current conditions, no API key)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

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
