"""Open-Meteo forecast client (current conditions, no API key)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


@dataclass
class OpenMeteoResult:
    temperature_c: float
    humidity_percent: float
    wind_speed_m_s: float
    precipitation_mm: float
    weather_code: int
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


async def fetch_current_weather(
    latitude: float,
    longitude: float,
    *,
    timeout_seconds: float = 20.0,
) -> OpenMeteoResult:
    """
    Fetch current weather from Open-Meteo. Temperature is always Celsius internally.
    Wind speed requested in m/s.
    """
    err = ""
    try:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "wind_speed_unit": "ms",
            "current": ",".join(
                [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                    "weather_code",
                    "wind_speed_10m",
                    "is_day",
                ]
            ),
        }
        timeout = httpx.Timeout(timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(OPEN_METEO_FORECAST_URL, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return OpenMeteoResult(
            temperature_c=0.0,
            humidity_percent=0.0,
            wind_speed_m_s=0.0,
            precipitation_mm=0.0,
            weather_code=0,
            is_day=False,
            fetch_ok=False,
            error=str(e) or type(e).__name__,
        )

    cur: Optional[dict[str, Any]] = None
    if isinstance(data, dict):
        cur = data.get("current")
    if not isinstance(cur, dict):
        return OpenMeteoResult(
            temperature_c=0.0,
            humidity_percent=0.0,
            wind_speed_m_s=0.0,
            precipitation_mm=0.0,
            weather_code=0,
            is_day=False,
            fetch_ok=False,
            error="open_meteo_invalid_response",
        )

    try:
        t = float(cur.get("temperature_2m", 0.0))
        rh = float(cur.get("relative_humidity_2m", 0.0))
        pr = float(cur.get("precipitation", 0.0))
        wc = int(cur.get("weather_code", 0))
        ws = float(cur.get("wind_speed_10m", 0.0))
        is_day = _bool_from_is_day(cur.get("is_day", 0))
    except (TypeError, ValueError) as e:
        err = str(e) or "parse_error"
        return OpenMeteoResult(
            temperature_c=0.0,
            humidity_percent=0.0,
            wind_speed_m_s=0.0,
            precipitation_mm=0.0,
            weather_code=0,
            is_day=False,
            fetch_ok=False,
            error=err,
        )

    return OpenMeteoResult(
        temperature_c=t,
        humidity_percent=rh,
        wind_speed_m_s=ws,
        precipitation_mm=pr,
        weather_code=wc,
        is_day=is_day,
        fetch_ok=True,
        error="",
    )
