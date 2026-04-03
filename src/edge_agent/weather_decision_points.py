"""
Operational / decision signals from Open-Meteo forecast + AQ payloads (local only).

Thresholds are intentionally simple and documented inline — not regulatory or medical.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from edge_agent.open_meteo import OpenMeteoResult
from edge_agent.open_meteo_air_quality import OpenMeteoAirQualityResult
from edge_agent.weather_derived import dew_point_celsius, heat_index_display, wind_chill_display

# --- Psychrometrics (outdoor air ~ sea level) ---
_STD_PA: float = 101_325.0


def _saturation_vapor_pressure_hpa(t_c: float) -> float:
    """Tetens-like saturation vapor pressure (hPa), T in °C."""
    t = float(t_c)
    return 6.112 * math.exp((17.67 * t) / (t + 243.5))


def humidity_ratio_kg_kg(t_c: float, rh_percent: float) -> float:
    """Humidity ratio W (kg water / kg dry air); RH 0–100."""
    rh = max(0.0, min(100.0, float(rh_percent))) / 100.0
    es = _saturation_vapor_pressure_hpa(t_c)
    e = rh * es
    p_pa = _STD_PA
    p_hpa = p_pa / 100.0
    return max(0.0, 0.62198 * e / (p_hpa - e))


def enthalpy_kj_per_kg_da(t_c: float, rh_percent: float) -> float:
    """
    Approximate moist-air enthalpy (kJ per kg dry air).
    h = 1.006*T + W*(2501 + 1.86*T) with T in °C (ASHRAE-style simplification).
    """
    t = float(t_c)
    w = humidity_ratio_kg_kg(t, rh_percent)
    return 1.006 * t + w * (2501.0 + 1.86 * t)


def enthalpy_display(t_c: float, rh_percent: float, use_fahrenheit: bool) -> float:
    """Enthalpy in BACnet units: kJ/kg dry air (metric) or BTU/lb dry air (imperial)."""
    h_kj = enthalpy_kj_per_kg_da(t_c, rh_percent)
    if use_fahrenheit:
        return h_kj / 2.326  # ~kJ/kg → BTU/lb
    return h_kj


def dew_point_spread_display(t_c: float, rh_percent: float, use_fahrenheit: bool) -> float:
    """Dry-bulb minus dew point in display °C or °F (positive = margin to saturation)."""
    td_c = dew_point_celsius(t_c, rh_percent)
    spread_c = float(t_c) - td_c
    if use_fahrenheit:
        return spread_c * 9.0 / 5.0
    return spread_c


# --- Binary / MSI helpers ---


def condensation_risk(spread_display: float, use_fahrenheit: bool) -> bool:
    """Active when margin to saturation is small (operational hint only)."""
    if use_fahrenheit:
        return spread_display < 3.0  # °F
    return spread_display < 1.7  # °C


def freeze_risk_dry_bulb_c(t_c: float) -> bool:
    return float(t_c) <= 0.0


def frost_risk(t_c: float, rh_percent: float, spread_c: float) -> bool:
    """Light frost risk: cool + humid / near saturation (coarse)."""
    t = float(t_c)
    rh = float(rh_percent)
    return t <= 4.0 and (rh >= 85.0 or spread_c <= 2.0)


def precip_active(
    precip: float,
    rain: float,
    showers: float,
    *,
    imperial_bundle: bool,
) -> bool:
    """Any liquid precip in current interval (units match Open-Meteo request)."""
    total = float(precip) + float(rain) + float(showers)
    if imperial_bundle:
        return total > 0.0005  # inch
    return total > 0.02  # mm


def snow_active(snowfall: float, *, imperial_bundle: bool) -> bool:
    s = float(snowfall)
    if imperial_bundle:
        return s > 0.0005  # inch
    return s > 0.02  # cm from API in metric mode


def high_wind(wind: float, gust: float, *, imperial_bundle: bool) -> bool:
    """Sustained or gust above a coarse threshold (same units as Open-Meteo)."""
    w = max(float(wind), float(gust))
    if imperial_bundle:
        return w >= 25.0  # mph
    return w >= 40.0  # km/h


def solar_available(is_day: bool, cloud_percent: float) -> bool:
    return bool(is_day) and float(cloud_percent) < 75.0


def daylight_level(is_day: bool, cloud_percent: float) -> int:
    """
    MSI state 1–4: Night, Low, Medium, Bright.
    """
    if not is_day:
        return 1
    cc = float(cloud_percent)
    if cc >= 70.0:
        return 2
    if cc >= 35.0:
        return 3
    return 4


def wind_severity(wind: float, gust: float, *, imperial_bundle: bool) -> int:
    """1=Calm … 4=Severe (same wind units as fetch)."""
    w = max(float(wind), float(gust))
    if imperial_bundle:
        if w < 10.0:
            return 1
        if w < 20.0:
            return 2
        if w < 35.0:
            return 3
        return 4
    if w < 15.0:
        return 1
    if w < 30.0:
        return 2
    if w < 50.0:
        return 3
    return 4


def comfort_level_apparent(at_c: float, use_fahrenheit: bool) -> int:
    """1=Cold … 5=Hot from apparent temperature (°C internally)."""
    if use_fahrenheit:
        at_f = float(at_c) * 9.0 / 5.0 + 32.0
        if at_f < 50.0:
            return 1
        if at_f < 65.0:
            return 2
        if at_f < 75.0:
            return 3
        if at_f < 85.0:
            return 4
        return 5
    at = float(at_c)
    if at < 10.0:
        return 1
    if at < 18.0:
        return 2
    if at < 24.0:
        return 3
    if at < 30.0:
        return 4
    return 5


def heat_stress_level(t_c: float, rh_percent: float, use_fahrenheit: bool) -> int:
    """1=Low, 2=Moderate, 3=High from heat index (NWS-style in weather_derived)."""
    hi = heat_index_display(t_c, rh_percent, use_fahrenheit)
    if use_fahrenheit:
        if hi < 90.0:
            return 1
        if hi < 105.0:
            return 2
        return 3
    if hi < 32.2:
        return 1
    if hi < 40.6:
        return 2
    return 3


def cold_stress_level(
    t_c: float,
    wind_speed: float,
    *,
    imperial_bundle: bool,
    use_fahrenheit: bool,
) -> int:
    """1=Low, 2=Moderate, 3=High from wind-chill proxy."""
    wc = wind_chill_display(
        t_c,
        wind_speed,
        imperial_bundle=imperial_bundle,
        use_fahrenheit=use_fahrenheit,
    )
    if use_fahrenheit:
        if wc > 32.0:
            return 1
        if wc >= 0.0:
            return 2
        return 3
    if wc > 0.0:
        return 1
    if wc >= -18.0:
        return 2
    return 3


def weather_severity(wx: OpenMeteoResult, *, imperial_bundle: bool) -> int:
    """1=Calm … 4=Extreme — coarse blend of WMO code, precip, wind (native units)."""
    code = int(wx.weather_code)
    ptot = float(wx.precipitation) + float(wx.rain) + float(wx.showers)
    wmax = max(float(wx.wind_speed), float(wx.wind_gust))
    if code in (95, 96, 99) or code == 82:
        return 4
    heavy = ptot > (0.25 if imperial_bundle else 5.0)
    if code in (75, 86, 65, 67) or heavy:
        return 3
    if wmax > (35.0 if imperial_bundle else 50.0) or code in (63, 73, 81):
        return 2
    return 1


def aqi_category_pm25(pm25: float) -> int:
    """
    EPA-like PM2.5 bands (μg/m³), 1…6.
    Not official AQI — category label only.
    """
    x = float(pm25)
    if x <= 12.0:
        return 1
    if x <= 35.4:
        return 2
    if x <= 55.4:
        return 3
    if x <= 150.0:
        return 4
    if x <= 250.0:
        return 5
    return 6


def outdoor_air_quality_good(pm25: float, pm10: float) -> bool:
    """Coarse OK for ventilation hint: PM2.5 / PM10 below common moderate cutoffs."""
    return float(pm25) <= 35.4 and float(pm10) <= 154.0


def smoke_risk(pm25: float) -> bool:
    return float(pm25) > 55.0


def dominant_pollutant_state(aq: OpenMeteoAirQualityResult) -> int:
    """
    1=negligible, 2–6 = PM2.5, PM10, O3, NO2, SO2 (max μg/m³ among these).
    States 7–8 (CO, CO2) reserved in BACnet stateText but not selected here — CO/CO2
    units/scales differ; ranking only the five μg/m³ species keeps the signal stable.
    """
    scores = {
        2: float(aq.pm2_5_ugm3),
        3: float(aq.pm10_ugm3),
        4: float(aq.ozone_ugm3),
        5: float(aq.nitrogen_dioxide_ugm3),
        6: float(aq.sulphur_dioxide_ugm3),
    }
    mx = max(scores.values())
    if mx < 0.5:
        return 1
    tie = [s for s, v in scores.items() if v == mx]
    return min(tie)


def economizer_available(
    t_c: float,
    rh_percent: float,
    pm25: float,
    *,
    use_fahrenheit: bool,
) -> bool:
    """
    Simple dry-bulb + enthalpy + PM2.5 gate (both wx + AQ must have succeeded).
    T in 55–75 °F band (or 13–24 °C) and h < ~62 kJ/kg and PM2.5 in moderate-or-better.
    """
    h_kj = enthalpy_kj_per_kg_da(t_c, rh_percent)
    if use_fahrenheit:
        t_f = float(t_c) * 9.0 / 5.0 + 32.0
        ok_t = 55.0 <= t_f <= 75.0
    else:
        ok_t = 13.0 <= float(t_c) <= 24.0
    return ok_t and h_kj < 62.0 and float(pm25) <= 35.4


def outdoor_air_usable(
    t_c: float,
    rh_percent: float,
    pm25: float,
    pm10: float,
) -> bool:
    """Broader “reasonable OA” than economizer: enthalpy not extreme + AQ not bad."""
    h_kj = enthalpy_kj_per_kg_da(t_c, rh_percent)
    return (
        45.0 <= h_kj <= 70.0
        and outdoor_air_quality_good(pm25, pm10)
    )


@dataclass
class OutdoorDecisionComputation:
    """Optional fields = skip BACnet write (preserve last)."""

    dew_spread: Optional[float] = None
    enthalpy: Optional[float] = None
    bi_condensation: Optional[bool] = None
    bi_freeze: Optional[bool] = None
    bi_frost: Optional[bool] = None
    bi_solar: Optional[bool] = None
    bi_precip: Optional[bool] = None
    bi_snow: Optional[bool] = None
    bi_high_wind: Optional[bool] = None
    msi_daylight: Optional[int] = None
    msi_wind_sev: Optional[int] = None
    msi_comfort: Optional[int] = None
    msi_weather_sev: Optional[int] = None
    msi_heat: Optional[int] = None
    msi_cold: Optional[int] = None
    msi_aqi: Optional[int] = None
    bi_smoke: Optional[bool] = None
    bi_aq_good: Optional[bool] = None
    msi_dominant: Optional[int] = None
    bi_econo: Optional[bool] = None
    bi_oa_usable: Optional[bool] = None


def compute_outdoor_decisions(
    wx: OpenMeteoResult,
    aq: OpenMeteoAirQualityResult,
    *,
    wx_ok: bool,
    aq_ok: bool,
    use_fahrenheit: bool,
    imperial_bundle: bool,
) -> OutdoorDecisionComputation:
    out = OutdoorDecisionComputation()
    if wx_ok:
        t_c = float(wx.temperature_c)
        rh = float(wx.humidity_percent)
        td_c = dew_point_celsius(t_c, rh)
        spread_c = t_c - td_c
        dsp = dew_point_spread_display(t_c, rh, use_fahrenheit)
        out.dew_spread = dsp
        out.enthalpy = enthalpy_display(t_c, rh, use_fahrenheit)
        out.bi_condensation = condensation_risk(dsp, use_fahrenheit)
        out.bi_freeze = freeze_risk_dry_bulb_c(t_c)
        out.bi_frost = frost_risk(t_c, rh, spread_c)
        out.bi_solar = solar_available(wx.is_day, float(wx.cloud_cover_percent))
        out.bi_precip = precip_active(
            wx.precipitation,
            wx.rain,
            wx.showers,
            imperial_bundle=imperial_bundle,
        )
        out.bi_snow = snow_active(wx.snowfall, imperial_bundle=imperial_bundle)
        out.bi_high_wind = high_wind(
            wx.wind_speed,
            wx.wind_gust,
            imperial_bundle=imperial_bundle,
        )
        out.msi_daylight = daylight_level(wx.is_day, float(wx.cloud_cover_percent))
        out.msi_wind_sev = wind_severity(
            wx.wind_speed,
            wx.wind_gust,
            imperial_bundle=imperial_bundle,
        )
        out.msi_comfort = comfort_level_apparent(wx.apparent_temperature_c, use_fahrenheit)
        out.msi_weather_sev = weather_severity(wx, imperial_bundle=imperial_bundle)
        out.msi_heat = heat_stress_level(t_c, rh, use_fahrenheit)
        out.msi_cold = cold_stress_level(
            t_c,
            wx.wind_speed,
            imperial_bundle=imperial_bundle,
            use_fahrenheit=use_fahrenheit,
        )
    if aq_ok:
        pm25 = float(aq.pm2_5_ugm3)
        pm10 = float(aq.pm10_ugm3)
        out.msi_aqi = aqi_category_pm25(pm25)
        out.bi_smoke = smoke_risk(pm25)
        out.bi_aq_good = outdoor_air_quality_good(pm25, pm10)
        out.msi_dominant = dominant_pollutant_state(aq)
    if wx_ok and aq_ok:
        pm25 = float(aq.pm2_5_ugm3)
        pm10 = float(aq.pm10_ugm3)
        t_c = float(wx.temperature_c)
        rh = float(wx.humidity_percent)
        out.bi_econo = economizer_available(
            t_c,
            rh,
            pm25,
            use_fahrenheit=use_fahrenheit,
        )
        out.bi_oa_usable = outdoor_air_usable(t_c, rh, pm25, pm10)
    return out
