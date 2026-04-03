"""Derived outdoor metrics from Open-Meteo ``current`` fields (no extra HTTP).

All inputs use the same units as ``OpenMeteoResult``: temperature in °C, RH in %,
wind speed in km/h (metric bundle) or mph (imperial bundle).

Dew point — Magnus formula over liquid water (typical for -40–50 °C).
Heat index — NWS Rothfusz regression (°F internally); only applied when T ≥ 80 °F;
  otherwise the heat-index value is the dry-bulb temperature (no bogus hot “feels like”).
Wind chill — NWS (2001) for °F + mph when V ≥ 3 mph and T ≤ 50 °F; Environment Canada
  style for °C + km/h when V ≥ ~5 km/h and T ≤ 10 °C; otherwise dry-bulb.
"""

from __future__ import annotations

import math
from typing import Final

# --- Dew point (°C) ---

_B_MAGNUS: Final[float] = 17.67
_C_MAGNUS: Final[float] = 243.5


def dew_point_celsius(t_c: float, rh_percent: float) -> float:
    """Dew point temperature (°C) from dry-bulb (°C) and RH (%)."""
    rh = max(1.0, min(100.0, float(rh_percent)))
    t = float(t_c)
    gamma = math.log(rh / 100.0) + (_B_MAGNUS * t) / (_C_MAGNUS + t)
    denom = _B_MAGNUS - gamma
    if denom <= 1e-9:
        return t
    return (_C_MAGNUS * gamma) / denom


# --- Heat index (NWS, °F) ---

def _heat_index_fahrenheit(t_f: float, rh_percent: float) -> float:
    """NWS heat index; RH in %, T in °F. Below ~80 °F returns dry bulb."""
    T = float(t_f)
    R = max(0.0, min(100.0, float(rh_percent)))
    if T < 80.0:
        return T
    # Rothfusz regression (NOAA/NWS)
    hi = (
        -42.379
        + 2.04901523 * T
        + 10.14333127 * R
        - 0.22475541 * T * R
        - 6.83783e-3 * T * T
        - 5.481717e-2 * R * R
        + 1.22874e-3 * T * T * R
        + 8.5282e-4 * T * R * R
        - 1.99e-6 * T * T * R * R
    )
    # Heat index should not read far below air temperature for sub-threshold cases
    return float(max(hi, T))


def heat_index_display(t_c: float, rh_percent: float, use_fahrenheit: bool) -> float:
    """Heat index in BACnet display units (°F if imperial, else °C)."""
    t_f = t_c * 9.0 / 5.0 + 32.0
    hi_f = _heat_index_fahrenheit(t_f, rh_percent)
    if use_fahrenheit:
        return hi_f
    return (hi_f - 32.0) * 5.0 / 9.0


# --- Wind chill ---

def _wind_chill_fahrenheit(t_f: float, v_mph: float) -> float:
    """NWS wind chill (2001), °F and mph. Outside valid range → dry bulb."""
    T = float(t_f)
    V = float(v_mph)
    if T > 50.0 or V < 3.0:
        return T
    wc = 35.74 + 0.6215 * T - 35.75 * (V**0.16) + 0.4275 * T * (V**0.16)
    return float(min(wc, T))


def _wind_chill_celsius_metric(t_c: float, v_kmh: float) -> float:
    """Environment Canada / ISO-style wind chill, °C and km/h."""
    Ta = float(t_c)
    V = float(v_kmh)
    if Ta > 10.0 or V < 4.828:  # ~3 mph
        return Ta
    wc = 13.12 + 0.6215 * Ta - 11.37 * (V**0.16) + 0.3965 * Ta * (V**0.16)
    return float(min(wc, Ta))


def wind_chill_display(
    t_c: float,
    wind_speed: float,
    *,
    imperial_bundle: bool,
    use_fahrenheit: bool,
) -> float:
    """
    Wind chill in BACnet display units (°F or °C).

    ``wind_speed`` matches Open-Meteo: mph when imperial_bundle else km/h.
    """
    if imperial_bundle:
        t_f = t_c * 9.0 / 5.0 + 32.0
        wc_f = _wind_chill_fahrenheit(t_f, wind_speed)
        if use_fahrenheit:
            return wc_f
        return (wc_f - 32.0) * 5.0 / 9.0
    wc_c = _wind_chill_celsius_metric(t_c, wind_speed)
    if use_fahrenheit:
        return wc_c * 9.0 / 5.0 + 32.0
    return wc_c


def wmo_weather_code_text(code: int) -> str:
    """Short WMO weather interpretation (Open-Meteo / WMO 4677 subset)."""
    m: dict[int, str] = {
        0: "Clear",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        56: "Freezing drizzle",
        57: "Dense freezing drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        66: "Freezing rain",
        67: "Heavy freezing rain",
        71: "Slight snow",
        73: "Moderate snow",
        75: "Heavy snow",
        77: "Snow grains",
        80: "Slight rain showers",
        81: "Moderate rain showers",
        82: "Violent rain showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm w/ hail",
        99: "Thunderstorm w/ heavy hail",
    }
    c = int(code)
    return m.get(c, f"code={c}")
