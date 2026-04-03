"""Open-Meteo Air Quality API (current values; separate from Forecast API)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx

OPEN_METEO_AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# `current=` fields documented at https://open-meteo.com/en/docs/air-quality-api
_AIR_QUALITY_CURRENT_VARS = ",".join(
    [
        "carbon_dioxide",
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "aerosol_optical_depth",
        "uv_index",
    ]
)


@dataclass
class OpenMeteoAirQualityResult:
    """Native Open-Meteo units: CO2 ppm; gases and PM μg/m³; AOD and UV index dimensionless."""

    carbon_dioxide_ppm: float
    pm10_ugm3: float
    pm2_5_ugm3: float
    carbon_monoxide_ugm3: float
    nitrogen_dioxide_ugm3: float
    sulphur_dioxide_ugm3: float
    ozone_ugm3: float
    aerosol_optical_depth: float
    uv_index: float
    fetch_ok: bool
    error: str


def _req_float(cur: dict[str, Any], key: str) -> float:
    v = cur.get(key)
    if v is None:
        raise ValueError(f"missing_{key}")
    return float(v)


def _aq_failed(error: str) -> OpenMeteoAirQualityResult:
    return OpenMeteoAirQualityResult(
        carbon_dioxide_ppm=0.0,
        pm10_ugm3=0.0,
        pm2_5_ugm3=0.0,
        carbon_monoxide_ugm3=0.0,
        nitrogen_dioxide_ugm3=0.0,
        sulphur_dioxide_ugm3=0.0,
        ozone_ugm3=0.0,
        aerosol_optical_depth=0.0,
        uv_index=0.0,
        fetch_ok=False,
        error=error,
    )


async def fetch_current_air_quality(
    latitude: float,
    longitude: float,
    *,
    timeout_seconds: float = 20.0,
) -> OpenMeteoAirQualityResult:
    try:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": _AIR_QUALITY_CURRENT_VARS,
        }
        timeout = httpx.Timeout(timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            r = await client.get(OPEN_METEO_AIR_QUALITY_URL, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        return _aq_failed(str(e) or type(e).__name__)

    cur: Optional[dict[str, Any]] = None
    if isinstance(data, dict):
        cur = data.get("current")
    if not isinstance(cur, dict):
        return _aq_failed("open_meteo_aq_invalid_response")

    try:
        return OpenMeteoAirQualityResult(
            carbon_dioxide_ppm=_req_float(cur, "carbon_dioxide"),
            pm10_ugm3=_req_float(cur, "pm10"),
            pm2_5_ugm3=_req_float(cur, "pm2_5"),
            carbon_monoxide_ugm3=_req_float(cur, "carbon_monoxide"),
            nitrogen_dioxide_ugm3=_req_float(cur, "nitrogen_dioxide"),
            sulphur_dioxide_ugm3=_req_float(cur, "sulphur_dioxide"),
            ozone_ugm3=_req_float(cur, "ozone"),
            aerosol_optical_depth=_req_float(cur, "aerosol_optical_depth"),
            uv_index=_req_float(cur, "uv_index"),
            fetch_ok=True,
            error="",
        )
    except (TypeError, ValueError) as e:
        return _aq_failed(str(e) or "parse_error")
