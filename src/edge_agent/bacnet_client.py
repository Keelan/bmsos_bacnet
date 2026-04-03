"""BACpypes3 BACnet/IP client (Pass 2)."""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import types
from importlib.metadata import PackageNotFoundError, version as package_version
from typing import Any, Optional, Union

# ErrorRejectAbortNack subclasses BaseException, not Exception — BACnet errors
# are not caught by `except Exception`.
from bacpypes3.apdu import (
    AbortPDU,
    AbortReason,
    CreateObjectACK,
    CreateObjectRequest,
    DeleteObjectRequest,
    ErrorRejectAbortNack,
    SimpleAckPDU,
)
from bacpypes3.app import Application
from bacpypes3.errors import MissingRequiredParameter, ParameterOutOfRange
from bacpypes3.argparse import SimpleArgumentParser
from bacpypes3.basetypes import (
    BinaryPV,
    CreateObjectRequestObjectSpecifier,
    EngineeringUnits,
    EventState,
    ObjectTypesSupported,
    Polarity,
    PriorityValue,
    PropertyValue,
    StatusFlags,
)
from bacpypes3.basetypes import CharacterString as StateTextString
from bacpypes3.constructeddata import Array, ArrayOf, SequenceOf
from bacpypes3.local.analog import AnalogInputObject
from bacpypes3.local.binary import BinaryInputObject, BinaryValueObject
from bacpypes3.local.object import Object as LocalObject
from bacpypes3.object import CharacterStringValueObject as _CharacterStringValueObject
from bacpypes3.object import MultiStateInputObject as _MultiStateInputObject
from bacpypes3.pdu import Address, LocalBroadcast
from bacpypes3.primitivedata import Boolean, CharacterString, Null, ObjectIdentifier, Real, Unsigned

from edge_agent.json_safe import failure_message, to_json_safe
from edge_agent.weather_decision_points import compute_outdoor_decisions
from edge_agent.weather_derived import (
    dew_point_celsius,
    heat_index_display,
    wind_chill_display,
    wmo_weather_code_text,
)
from edge_agent.models import (
    EffectiveBacnetConfig,
    JobResultEnvelope,
    RemoteAgentTuning,
    apply_float_tuning,
    apply_int_tuning,
    desired_weather_polling_enabled_from_tuning,
    remote_weather_master_enabled,
    use_fahrenheit_from_tuning,
    utc_now_iso,
)
from edge_agent.open_meteo import OpenMeteoResult
from edge_agent.open_meteo_air_quality import OpenMeteoAirQualityResult
from edge_agent.settings import Settings
from edge_agent.holidays import HolidayEval
from edge_agent.open_meteo import SunTimesResult, daylight_window_active
from edge_agent.site_time import SiteLocalTimeInfo
from edge_agent.storage import Storage

_log = logging.getLogger(__name__)

# Multi-state-input present-value (1-based): last job lifecycle / outcome
_JOB_MSI_IDLE = 1
_JOB_MSI_RUNNING = 2
_JOB_MSI_SUCCESS = 3
_JOB_MSI_PARTIAL = 4
_JOB_MSI_FAILED = 5


def _patch_whois_iam_response(app: Application, mode: str) -> None:
    """
    BACpypes3 ``WhoIsIAmServices.do_WhoIsRequest`` calls ``self.i_am(address=apdu.pduSource)``
    (Original-Unicast-NPDU to the requester). Some tools expect I-Am as a **broadcast**
    (Original-Broadcast-NPDU / BVLC 0x0b on IPv4).

    ``i_am(address=None)`` uses ``GlobalBroadcast``; NetworkServiceAccessPoint maps that to
    ``LocalBroadcast`` for the local adapter, which BIPNormal encodes as OriginalBroadcastNPDU.
    """
    if mode == "unicast":
        return

    _log.info(
        "bacnet_whois_iam_patch: enabled — I-Am will use LocalBroadcast "
        "(BVLC Original-Broadcast-NPDU / 0x0b on IPv4, not unicast to requester)"
    )

    async def do_WhoIsRequest(self, apdu) -> None:
        if not self.device_object:
            return

        low_limit = apdu.deviceInstanceRangeLowLimit
        high_limit = apdu.deviceInstanceRangeHighLimit

        if low_limit is not None:
            if high_limit is None:
                raise MissingRequiredParameter("deviceInstanceRangeHighLimit required")
            if (low_limit < 0) or (low_limit > 4194303):
                raise ParameterOutOfRange("deviceInstanceRangeLowLimit out of range")
        if high_limit is not None:
            if low_limit is None:
                raise MissingRequiredParameter("deviceInstanceRangeLowLimit required")
            if (high_limit < 0) or (high_limit > 4194303):
                raise ParameterOutOfRange("deviceInstanceRangeHighLimit out of range")

        if low_limit is not None:
            if self.device_object.objectIdentifier[1] < low_limit:
                return
        if high_limit is not None:
            if self.device_object.objectIdentifier[1] > high_limit:
                return

        # Explicit LocalBroadcast → BIPNormal uses OriginalBroadcastNPDU (0x0b).
        # (i_am(address=None) uses GlobalBroadcast, which NSAP also maps to broadcast;
        # LocalBroadcast is the direct path some tools expect.)
        self.i_am(address=LocalBroadcast())

    app.do_WhoIsRequest = types.MethodType(do_WhoIsRequest, app)  # type: ignore[method-assign]


class _EdgeCharacterStringValue(LocalObject, _CharacterStringValueObject):
    """Local character-string-value for agent identity / last job text."""

    _required = ("presentValue", "statusFlags", "eventState", "outOfService")


class _EdgeMultiStateInput(LocalObject, _MultiStateInputObject):
    """Local multi-state-input for last job state."""

    _required = (
        "presentValue",
        "statusFlags",
        "eventState",
        "outOfService",
        "numberOfStates",
    )


def _truncate_csv_text(s: str, max_len: int = 400) -> str:
    t = s if len(s) <= max_len else s[: max_len - 3] + "..."
    return t


def _character_string_pv_as_str(obj: Any) -> str:
    """String form of presentValue for local character-string objects (avoid spurious BACnet writes)."""
    pv = getattr(obj, "presentValue", None)
    if pv is None:
        return ""
    return str(pv)


def _set_character_string_if_changed(
    obj: _EdgeCharacterStringValue, text: str, max_len: int = 400
) -> None:
    t = _truncate_csv_text(text, max_len)
    if _character_string_pv_as_str(obj) == t:
        return
    obj.presentValue = CharacterString(t)


def _set_real_if_changed(obj: AnalogInputObject, value: float) -> None:
    try:
        cur = float(obj.presentValue)
    except Exception:
        cur = None
    if cur is not None and abs(cur - value) < 1e-5:
        return
    obj.presentValue = Real(float(value))


def _set_binary_if_changed(obj: BinaryInputObject, pv: BinaryPV) -> None:
    if obj.presentValue == pv:
        return
    obj.presentValue = pv


def _set_multistate_if_changed(obj: _EdgeMultiStateInput, state: int) -> None:
    """Multi-state present value is 1..N matching stateText order."""
    u = Unsigned(int(state))
    if obj.presentValue == u:
        return
    obj.presentValue = u


def _create_agent_telemetry_objects() -> tuple[
    AnalogInputObject,
    _EdgeCharacterStringValue,
    _EdgeCharacterStringValue,
    _EdgeCharacterStringValue,
    _EdgeCharacterStringValue,
    _EdgeMultiStateInput,
]:
    zf = StatusFlags([0, 0, 0, 0])
    common = {
        "statusFlags": zf,
        "eventState": EventState.normal,
        "outOfService": Boolean(False),
    }
    ai_uptime = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,1"),
        objectName=CharacterString("Edge-Uptime"),
        description=CharacterString("Agent process uptime (seconds)"),
        presentValue=Real(0.0),
        covIncrement=Real(1.0),
        units=EngineeringUnits.seconds,
        **common,
    )
    csv_host = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,1"),
        objectName=CharacterString("Edge-Hostname"),
        description=CharacterString("Host name"),
        presentValue=CharacterString(""),
        **common,
    )
    csv_box = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,2"),
        objectName=CharacterString("Edge-BoxId"),
        description=CharacterString("SaaS box id"),
        presentValue=CharacterString(""),
        **common,
    )
    csv_saas = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,3"),
        objectName=CharacterString("Edge-SaaS-Base"),
        description=CharacterString("SaaS API base URL"),
        presentValue=CharacterString(""),
        **common,
    )
    csv_job = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,4"),
        objectName=CharacterString("Edge-LastJob"),
        description=CharacterString("Last job id, status, summary"),
        presentValue=CharacterString(""),
        **common,
    )
    st = ArrayOf(StateTextString)(
        [
            StateTextString("Idle"),
            StateTextString("Running"),
            StateTextString("Success"),
            StateTextString("Partial"),
            StateTextString("Failed"),
        ]
    )
    msi_job = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,1"),
        objectName=CharacterString("Edge-LastJob-State"),
        description=CharacterString("Last job state (idle / running / outcome)"),
        presentValue=Unsigned(_JOB_MSI_IDLE),
        numberOfStates=Unsigned(5),
        stateText=st,
        **common,
    )
    return ai_uptime, csv_host, csv_box, csv_saas, csv_job, msi_job


def _priority_array_binary_empty() -> Any:
    return ArrayOf(PriorityValue)([PriorityValue(null=())] * 16)


def _weather_temp_engineering_units(tuning: Optional[RemoteAgentTuning]) -> EngineeringUnits:
    return (
        EngineeringUnits.degreesFahrenheit
        if use_fahrenheit_from_tuning(tuning)
        else EngineeringUnits.degreesCelsius
    )


def _weather_imperial_bundle(tuning: Optional[RemoteAgentTuning]) -> bool:
    """True when SaaS selects US/imperial: F, mph, inches (same flag as temperature)."""
    return use_fahrenheit_from_tuning(tuning)


def _weather_wind_engineering_units(tuning: Optional[RemoteAgentTuning]) -> EngineeringUnits:
    return (
        EngineeringUnits.milesPerHour
        if _weather_imperial_bundle(tuning)
        else EngineeringUnits.kilometersPerHour
    )


def _weather_precip_engineering_units(tuning: Optional[RemoteAgentTuning]) -> EngineeringUnits:
    return (
        EngineeringUnits.inches
        if _weather_imperial_bundle(tuning)
        else EngineeringUnits.millimeters
    )


def _weather_pressure_engineering_units(tuning: Optional[RemoteAgentTuning]) -> EngineeringUnits:
    return (
        EngineeringUnits.inchesOfMercury
        if _weather_imperial_bundle(tuning)
        else EngineeringUnits.hectopascals
    )


def _weather_snow_engineering_units(tuning: Optional[RemoteAgentTuning]) -> EngineeringUnits:
    return (
        EngineeringUnits.inches
        if _weather_imperial_bundle(tuning)
        else EngineeringUnits.centimeters
    )


# hPa → inHg (when imperial; Open-Meteo always returns hPa for pressure)
_HPA_TO_INHG = 0.029529983071445


def _create_weather_objects(
    tuning: Optional[RemoteAgentTuning],
) -> tuple[
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryValueObject,
    _EdgeCharacterStringValue,
]:
    """
    Forecast weather points (Open-Meteo ``current``).     Instance block AI 2–15 (raw Open-Meteo); BI 3–4,6; BV 1; CSV 5.
    Derived dew point / heat index / wind chill + code text: see ``_create_weather_derived_objects`` (AI 16–18, CSV 11).
    Metric: km/h wind, mm precip/rain/showers, cm snow, hPa pressure. Imperial: mph, in, in snow, inHg.
    """
    zf = StatusFlags([0, 0, 0, 0])
    common = {
        "statusFlags": zf,
        "eventState": EventState.normal,
        "outOfService": Boolean(False),
    }
    temp_units = _weather_temp_engineering_units(tuning)
    wind_units = _weather_wind_engineering_units(tuning)
    precip_units = _weather_precip_engineering_units(tuning)
    press_units = _weather_pressure_engineering_units(tuning)
    snow_units = _weather_snow_engineering_units(tuning)

    ai_temp = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,2"),
        objectName=CharacterString("Weather-OutdoorTemp"),
        description=CharacterString("Outdoor air temperature 2 m (Open-Meteo)"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=temp_units,
        **common,
    )
    ai_rh = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,3"),
        objectName=CharacterString("Weather-Humidity"),
        description=CharacterString("Relative humidity 2 m (%)"),
        presentValue=Real(0.0),
        covIncrement=Real(1.0),
        units=EngineeringUnits.percentRelativeHumidity,
        **common,
    )
    ai_wind = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,4"),
        objectName=CharacterString("Weather-WindSpeed"),
        description=CharacterString(
            "Wind speed 10 m (mph imperial, km/h metric)"
        ),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=wind_units,
        **common,
    )
    ai_precip = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,5"),
        objectName=CharacterString("Weather-Precipitation"),
        description=CharacterString("Precipitation preceding interval (in imperial, mm metric)"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=precip_units,
        **common,
    )
    ai_apparent = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,6"),
        objectName=CharacterString("Weather-ApparentTemp"),
        description=CharacterString("Apparent (feels-like) temperature 2 m"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=temp_units,
        **common,
    )
    ai_rain = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,7"),
        objectName=CharacterString("Weather-Rain"),
        description=CharacterString("Rain preceding interval"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=precip_units,
        **common,
    )
    ai_showers = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,8"),
        objectName=CharacterString("Weather-Showers"),
        description=CharacterString("Showers preceding interval"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=precip_units,
        **common,
    )
    ai_snow = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,9"),
        objectName=CharacterString("Weather-Snowfall"),
        description=CharacterString("Snowfall preceding interval (cm metric, in imperial)"),
        presentValue=Real(0.0),
        covIncrement=Real(0.05),
        units=snow_units,
        **common,
    )
    ai_wcode = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,10"),
        objectName=CharacterString("Weather-Code"),
        description=CharacterString("WMO weather code (dimensionless)"),
        presentValue=Real(0.0),
        covIncrement=Real(1.0),
        units=EngineeringUnits.noUnits,
        **common,
    )
    ai_cloud = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,11"),
        objectName=CharacterString("Weather-CloudCover"),
        description=CharacterString("Total cloud cover (%)"),
        presentValue=Real(0.0),
        covIncrement=Real(1.0),
        units=EngineeringUnits.percent,
        **common,
    )
    ai_pmsl = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,12"),
        objectName=CharacterString("Weather-Pressure-MSL"),
        description=CharacterString("Sea level pressure (hPa metric, inHg imperial)"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=press_units,
        **common,
    )
    ai_psurf = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,13"),
        objectName=CharacterString("Weather-Pressure-Surface"),
        description=CharacterString("Surface pressure (hPa metric, inHg imperial)"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=press_units,
        **common,
    )
    ai_wdir = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,14"),
        objectName=CharacterString("Weather-WindDirection"),
        description=CharacterString("Wind direction 10 m (degrees)"),
        presentValue=Real(0.0),
        covIncrement=Real(1.0),
        units=EngineeringUnits.degreesAngular,
        **common,
    )
    ai_wgust = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,15"),
        objectName=CharacterString("Weather-WindGusts"),
        description=CharacterString("Wind gusts 10 m (mph imperial, km/h metric)"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=wind_units,
        **common,
    )

    def _bi_wx_common() -> dict[str, Any]:
        return {
            "statusFlags": StatusFlags([0, 0, 0, 0]),
            "eventState": EventState.normal,
            "outOfService": Boolean(False),
            "polarity": Polarity.normal,
            "inactiveText": CharacterString("Offline"),
            "activeText": CharacterString("Online"),
        }

    bi_ok = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,3"),
        objectName=CharacterString("Weather-OK"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("Last Open-Meteo Forecast fetch succeeded"),
        **_bi_wx_common(),
    )
    bi_unit = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,4"),
        objectName=CharacterString("Weather-UnitOfMeasure"),
        presentValue=BinaryPV.active if use_fahrenheit_from_tuning(tuning) else BinaryPV.inactive,
        description=CharacterString(
            "Metric vs imperial (USA) bundle from SaaS: temp, wind, precip, snow, pressure. "
            "inactive=Metric; active=Imperial"
        ),
        statusFlags=StatusFlags([0, 0, 0, 0]),
        eventState=EventState.normal,
        outOfService=Boolean(False),
        polarity=Polarity.normal,
        inactiveText=CharacterString("Metric"),
        activeText=CharacterString("Imperial"),
    )
    bi_is_day = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,6"),
        objectName=CharacterString("Weather-IsDay"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("Daylight from Open-Meteo is_day"),
        statusFlags=StatusFlags([0, 0, 0, 0]),
        eventState=EventState.normal,
        outOfService=Boolean(False),
        polarity=Polarity.normal,
        inactiveText=CharacterString("Night"),
        activeText=CharacterString("Day"),
    )

    poll_en = desired_weather_polling_enabled_from_tuning(tuning)
    poll_pv = BinaryPV.active if poll_en else BinaryPV.inactive
    bv_poll = BinaryValueObject(
        objectIdentifier=ObjectIdentifier("binary-value,1"),
        objectName=CharacterString("Weather-Polling-Enabled"),
        description=CharacterString("Active= poll weather; inactive= skip (writable; SaaS sets default on config)"),
        presentValue=poll_pv,
        priorityArray=_priority_array_binary_empty(),
        statusFlags=StatusFlags([0, 0, 0, 0]),
        eventState=EventState.normal,
        outOfService=Boolean(False),
        inactiveText=CharacterString("Disabled"),
        activeText=CharacterString("Enabled"),
        relinquishDefault=poll_pv,
    )
    csv_wx = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,5"),
        objectName=CharacterString("Weather-LastUpdate"),
        description=CharacterString("Last successful fetch or last error"),
        presentValue=CharacterString(""),
        **common,
    )
    return (
        ai_temp,
        ai_rh,
        ai_wind,
        ai_precip,
        ai_apparent,
        ai_rain,
        ai_showers,
        ai_snow,
        ai_wcode,
        ai_cloud,
        ai_pmsl,
        ai_psurf,
        ai_wdir,
        ai_wgust,
        bi_ok,
        bi_unit,
        bi_is_day,
        bv_poll,
        csv_wx,
    )


def _create_weather_derived_objects(
    tuning: Optional[RemoteAgentTuning],
) -> tuple[AnalogInputObject, AnalogInputObject, AnalogInputObject, _EdgeCharacterStringValue]:
    """
    Derived outdoor metrics from the same Open-Meteo ``current`` payload (computed locally).

    Instance reservation:
      - analog-input 16–18: dew point, heat index, wind chill (engineering units match
        Weather-OutdoorTemp: °C metric, °F imperial).
      - characterstringValue 11: WMO weather code description (numeric code remains AI 10).

    AI 19–20: dew-point spread + enthalpy (see ``_create_weather_decision_objects``).
    Gap AI 21–33 reserved for future weather analogs (site time uses 43+).
    """
    zf = StatusFlags([0, 0, 0, 0])
    common = {
        "statusFlags": zf,
        "eventState": EventState.normal,
        "outOfService": Boolean(False),
    }
    temp_units = _weather_temp_engineering_units(tuning)
    ai_dew = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,16"),
        objectName=CharacterString("Weather-DewPoint"),
        description=CharacterString(
            "Dew point from T/RH (Magnus); same °F/°C mode as outdoor temperature"
        ),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=temp_units,
        **common,
    )
    ai_hi = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,17"),
        objectName=CharacterString("Weather-HeatIndex"),
        description=CharacterString(
            "NWS heat index when T≥80 °F; else dry-bulb (see weather_derived)"
        ),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=temp_units,
        **common,
    )
    ai_wc = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,18"),
        objectName=CharacterString("Weather-WindChill"),
        description=CharacterString(
            "Wind chill (NWS mph / Canada km/h); else dry-bulb when out of range"
        ),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=temp_units,
        **common,
    )
    csv_code = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,11"),
        objectName=CharacterString("Weather-Code-Text"),
        description=CharacterString("WMO weather code short label (Open-Meteo)"),
        presentValue=CharacterString(""),
        **common,
    )
    return ai_dew, ai_hi, ai_wc, csv_code


def _create_weather_decision_objects(
    tuning: Optional[RemoteAgentTuning],
) -> tuple[
    AnalogInputObject,
    AnalogInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    _EdgeMultiStateInput,
    _EdgeMultiStateInput,
    _EdgeMultiStateInput,
    _EdgeMultiStateInput,
    _EdgeMultiStateInput,
    _EdgeMultiStateInput,
    _EdgeMultiStateInput,
    _EdgeMultiStateInput,
    _EdgeMultiStateInput,
]:
    """
    Decision / operational signals (local thresholds; see ``weather_decision_points``).

    Instance reservation (no overlap with weather AI 2–18, AQ 34–42, site 43–50):
      - analog-input 19–20: dew-point spread, enthalpy (units follow metric/imperial).
      - binary-input 9–19: moisture, thermal, solar, precip, wind, AQ, economizer flags.
      - multiStateInput 3–10: AQI category, daylight, wind severity, comfort, dominant
        pollutant, weather severity, heat stress, cold stress.

    Edge job uses multiStateInput 1; site weekday uses multiStateInput 2.
    """
    zf = StatusFlags([0, 0, 0, 0])
    common = {
        "statusFlags": zf,
        "eventState": EventState.normal,
        "outOfService": Boolean(False),
    }
    temp_units = _weather_temp_engineering_units(tuning)
    imperial = _weather_imperial_bundle(tuning)
    enth_units = (
        EngineeringUnits.btusPerPoundDryAir
        if imperial
        else EngineeringUnits.kilojoulesPerKilogramDryAir
    )

    def _bi(instance: int, name: str, desc: str, inactive: str, active: str) -> BinaryInputObject:
        return BinaryInputObject(
            objectIdentifier=ObjectIdentifier(f"binary-input,{instance}"),
            objectName=CharacterString(name),
            description=CharacterString(desc),
            presentValue=BinaryPV.inactive,
            polarity=Polarity.normal,
            inactiveText=CharacterString(inactive),
            activeText=CharacterString(active),
            **common,
        )

    ai_spread = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,19"),
        objectName=CharacterString("Weather-DewPointSpread"),
        description=CharacterString("Dry-bulb minus dew point (same ° as outdoor temp)"),
        presentValue=Real(0.0),
        covIncrement=Real(0.1),
        units=temp_units,
        **common,
    )
    ai_h = AnalogInputObject(
        objectIdentifier=ObjectIdentifier("analog-input,20"),
        objectName=CharacterString("Weather-OutdoorEnthalpy"),
        description=CharacterString(
            "Moist air enthalpy (kJ/kg dry air metric, BTU/lb dry air imperial); ~sea level"
        ),
        presentValue=Real(0.0),
        covIncrement=Real(0.5),
        units=enth_units,
        **common,
    )

    bi_cond = _bi(
        9,
        "Weather-Condensation-Risk",
        "Small dew-point spread (surface condensation hint)",
        "Normal",
        "Risk",
    )
    bi_frz = _bi(10, "Weather-Freeze-Risk", "Dry-bulb at or below freezing", "No", "Yes")
    bi_frost = _bi(11, "Weather-Frost-Risk", "Cool humid / near-saturation (coarse)", "No", "Yes")
    bi_solar = _bi(12, "Weather-Solar-Available", "Daytime and cloud cover < 75%", "No", "Yes")
    bi_precip = _bi(13, "Weather-Precipitation-Active", "Liquid precip in current interval", "No", "Yes")
    bi_snow = _bi(14, "Weather-Snow-Active", "Snowfall in current interval", "No", "Yes")
    bi_hw = _bi(15, "Weather-High-Wind", "Wind or gust above coarse threshold", "No", "Yes")
    bi_smoke = _bi(16, "Outdoor-Smoke-Risk", "PM2.5 elevated (operational, not AQI official)", "No", "Yes")
    bi_aqg = _bi(17, "Outdoor-Air-Quality-Good", "Coarse OK for ventilation (PM2.5/PM10)", "No", "Yes")
    bi_econ = _bi(18, "Weather-Economizer-Available", "Dry-bulb + enthalpy + PM2.5 gate (wx+aq)", "No", "Yes")
    bi_oa = _bi(19, "Weather-Outdoor-Air-Usable", "Reasonable OA per enthalpy + AQ (wx+aq)", "No", "Yes")

    st_aqi = ArrayOf(StateTextString)(
        [
            StateTextString("Good"),
            StateTextString("Moderate"),
            StateTextString("UnhealthySG"),
            StateTextString("Unhealthy"),
            StateTextString("VeryUnhealthy"),
            StateTextString("Hazardous"),
        ]
    )
    msi_aqi = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,3"),
        objectName=CharacterString("Outdoor-AQI-Category"),
        description=CharacterString("PM2.5 band category (μg/m³ thresholds; not official AQI)"),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(6),
        stateText=st_aqi,
        **common,
    )
    st_day = ArrayOf(StateTextString)(
        [
            StateTextString("Night"),
            StateTextString("Low"),
            StateTextString("Medium"),
            StateTextString("Bright"),
        ]
    )
    msi_day = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,4"),
        objectName=CharacterString("Weather-Daylight-Level"),
        description=CharacterString("From is_day + cloud cover bands"),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(4),
        stateText=st_day,
        **common,
    )
    st_wind = ArrayOf(StateTextString)(
        [
            StateTextString("Calm"),
            StateTextString("Breezy"),
            StateTextString("Windy"),
            StateTextString("Severe"),
        ]
    )
    msi_wind = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,5"),
        objectName=CharacterString("Weather-Wind-Severity"),
        description=CharacterString("From wind/gust vs mode-specific bands"),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(4),
        stateText=st_wind,
        **common,
    )
    st_comf = ArrayOf(StateTextString)(
        [
            StateTextString("Cold"),
            StateTextString("Cool"),
            StateTextString("Neutral"),
            StateTextString("Warm"),
            StateTextString("Hot"),
        ]
    )
    msi_comf = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,6"),
        objectName=CharacterString("Weather-Comfort-Level"),
        description=CharacterString("From apparent temperature bands"),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(5),
        stateText=st_comf,
        **common,
    )
    st_dom = ArrayOf(StateTextString)(
        [
            StateTextString("None"),
            StateTextString("PM2.5"),
            StateTextString("PM10"),
            StateTextString("O3"),
            StateTextString("NO2"),
            StateTextString("SO2"),
            StateTextString("CO"),
            StateTextString("CO2"),
        ]
    )
    msi_dom = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,7"),
        objectName=CharacterString("Outdoor-Dominant-Pollutant"),
        description=CharacterString("Largest signal among reported species (ranking heuristic)"),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(8),
        stateText=st_dom,
        **common,
    )
    st_wxsev = ArrayOf(StateTextString)(
        [
            StateTextString("Calm"),
            StateTextString("Moderate"),
            StateTextString("Severe"),
            StateTextString("Extreme"),
        ]
    )
    msi_wxsev = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,8"),
        objectName=CharacterString("Weather-Severity"),
        description=CharacterString("Coarse blend of WMO code, precip, wind"),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(4),
        stateText=st_wxsev,
        **common,
    )
    st_heat = ArrayOf(StateTextString)(
        [
            StateTextString("Low"),
            StateTextString("Moderate"),
            StateTextString("High"),
        ]
    )
    msi_heat = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,9"),
        objectName=CharacterString("Outdoor-Heat-Stress"),
        description=CharacterString("From heat index bands"),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(3),
        stateText=st_heat,
        **common,
    )
    st_cold = ArrayOf(StateTextString)(
        [
            StateTextString("Low"),
            StateTextString("Moderate"),
            StateTextString("High"),
        ]
    )
    msi_cold = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,10"),
        objectName=CharacterString("Outdoor-Cold-Stress"),
        description=CharacterString("From wind-chill bands"),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(3),
        stateText=st_cold,
        **common,
    )

    return (
        ai_spread,
        ai_h,
        bi_cond,
        bi_frz,
        bi_frost,
        bi_solar,
        bi_precip,
        bi_snow,
        bi_hw,
        bi_smoke,
        bi_aqg,
        bi_econ,
        bi_oa,
        msi_aqi,
        msi_day,
        msi_wind,
        msi_comf,
        msi_dom,
        msi_wxsev,
        msi_heat,
        msi_cold,
    )


def _create_air_quality_objects() -> tuple[
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    BinaryInputObject,
    _EdgeCharacterStringValue,
]:
    """
    Air-quality analog inputs (Open-Meteo Air Quality API).
    Instance block: analog-input 34–42 (forecast weather uses 2–15), binary-input 5, CSV 6.

    Engineering units: native Open-Meteo units for all modes (no USA/metric conversion;
    ppm and μg/m³ are used for both product unit modes).
    AOD and UV index use noUnits (dimensionless).
    """
    zf = StatusFlags([0, 0, 0, 0])
    common = {
        "statusFlags": zf,
        "eventState": EventState.normal,
        "outOfService": Boolean(False),
    }

    def _ai(instance: int, name: str, desc: str, units: EngineeringUnits, cov: float) -> AnalogInputObject:
        return AnalogInputObject(
            objectIdentifier=ObjectIdentifier(f"analog-input,{instance}"),
            objectName=CharacterString(name),
            description=CharacterString(desc),
            presentValue=Real(0.0),
            covIncrement=Real(float(cov)),
            units=units,
            **common,
        )

    ai_co2 = _ai(
        34,
        "Outdoor-CO2",
        "Outdoor CO2 (Open-Meteo Air Quality API, ppm)",
        EngineeringUnits.partsPerMillion,
        1.0,
    )
    ai_pm25 = _ai(
        35,
        "Outdoor-PM2.5",
        "Outdoor PM2.5 (μg/m³)",
        EngineeringUnits.microgramsPerCubicMeter,
        0.5,
    )
    ai_pm10 = _ai(
        36,
        "Outdoor-PM10",
        "Outdoor PM10 (μg/m³)",
        EngineeringUnits.microgramsPerCubicMeter,
        0.5,
    )
    ai_co = _ai(
        37,
        "Outdoor-CO",
        "Outdoor carbon monoxide (μg/m³)",
        EngineeringUnits.microgramsPerCubicMeter,
        1.0,
    )
    ai_no2 = _ai(
        38,
        "Outdoor-NO2",
        "Outdoor nitrogen dioxide (μg/m³)",
        EngineeringUnits.microgramsPerCubicMeter,
        0.5,
    )
    ai_so2 = _ai(
        39,
        "Outdoor-SO2",
        "Outdoor sulphur dioxide (μg/m³)",
        EngineeringUnits.microgramsPerCubicMeter,
        0.5,
    )
    ai_o3 = _ai(
        40,
        "Outdoor-O3",
        "Outdoor ozone (μg/m³)",
        EngineeringUnits.microgramsPerCubicMeter,
        0.5,
    )
    ai_aod = _ai(
        41,
        "Outdoor-AerosolOpticalDepth",
        "Aerosol optical depth at 550 nm (dimensionless)",
        EngineeringUnits.noUnits,
        0.01,
    )
    ai_uv = _ai(
        42,
        "Outdoor-UVIndex",
        "UV index (dimensionless)",
        EngineeringUnits.noUnits,
        0.1,
    )

    bi_aq_ok = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,5"),
        objectName=CharacterString("Air-Quality-OK"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("Last Open-Meteo Air Quality fetch succeeded"),
        statusFlags=StatusFlags([0, 0, 0, 0]),
        eventState=EventState.normal,
        outOfService=Boolean(False),
        polarity=Polarity.normal,
        inactiveText=CharacterString("Offline"),
        activeText=CharacterString("Online"),
    )
    csv_aq = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,6"),
        objectName=CharacterString("Air-Quality-LastUpdate"),
        description=CharacterString("Last air-quality fetch status or error"),
        presentValue=CharacterString(""),
        **common,
    )
    return (
        ai_co2,
        ai_pm25,
        ai_pm10,
        ai_co,
        ai_no2,
        ai_so2,
        ai_o3,
        ai_aod,
        ai_uv,
        bi_aq_ok,
        csv_aq,
    )


def _create_site_time_objects() -> tuple[
    _EdgeCharacterStringValue,
    _EdgeCharacterStringValue,
    _EdgeCharacterStringValue,
    _EdgeCharacterStringValue,
    BinaryInputObject,
    BinaryInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    _EdgeMultiStateInput,
    AnalogInputObject,
]:
    """
    Site-local time from system UTC + IANA zone resolved offline from weather lat/lon.

    Instance reservation (no overlap with telemetry CSV 1–4, weather CSV 5, AQ CSV 6,
    weather BI 3–4,6, AQ BI 5, edge BI 1–2, weather AI 2–15, weather derived AI 16–18, AQ AI 34–42):

    - characterstringValue 7–10: datetime / timezone / date / time strings
    - binary-input 7–8: Site-Time-OK, Site-DST-Active
    - analog-input 43–50: calendar components + UTC offset minutes
    - multiStateInput 2: Site-Weekday (state 1–7 = ISO Monday–Sunday; same as Site-Weekday-Number AI)
    - multiStateInput 3–10: outdoor decision categories (see ``_create_weather_decision_objects``)
    - binary-input 20–25 + characterstringValue 12–14: schedule context (see ``_create_schedule_context_objects``)
    """
    zf = StatusFlags([0, 0, 0, 0])
    common = {
        "statusFlags": zf,
        "eventState": EventState.normal,
        "outOfService": Boolean(False),
    }

    def _ai(instance: int, name: str, desc: str, units: EngineeringUnits) -> AnalogInputObject:
        return AnalogInputObject(
            objectIdentifier=ObjectIdentifier(f"analog-input,{instance}"),
            objectName=CharacterString(name),
            description=CharacterString(desc),
            presentValue=Real(0.0),
            covIncrement=Real(1.0),
            units=units,
            **common,
        )

    csv_dt = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,7"),
        objectName=CharacterString("Site-Local-DateTime"),
        description=CharacterString("ISO 8601 site-local date-time with UTC offset (from system clock)"),
        presentValue=CharacterString(""),
        **common,
    )
    csv_tz = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,8"),
        objectName=CharacterString("Site-Timezone-Name"),
        description=CharacterString("IANA timezone from weather_latitude/longitude (offline lookup)"),
        presentValue=CharacterString(""),
        **common,
    )
    csv_date = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,9"),
        objectName=CharacterString("Site-Local-Date"),
        description=CharacterString("Site-local calendar date YYYY-MM-DD"),
        presentValue=CharacterString(""),
        **common,
    )
    csv_time = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,10"),
        objectName=CharacterString("Site-Local-Time"),
        description=CharacterString("Site-local time of day HH:MM:SS (24-hour)"),
        presentValue=CharacterString(""),
        **common,
    )
    bi_ok = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,7"),
        objectName=CharacterString("Site-Time-OK"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("Site-local time valid (weather lat/lon + timezone resolution)"),
        statusFlags=StatusFlags([0, 0, 0, 0]),
        eventState=EventState.normal,
        outOfService=Boolean(False),
        polarity=Polarity.normal,
        inactiveText=CharacterString("Invalid"),
        activeText=CharacterString("OK"),
    )
    bi_dst = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,8"),
        objectName=CharacterString("Site-DST-Active"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("Daylight saving time active at site (zoneinfo)"),
        statusFlags=StatusFlags([0, 0, 0, 0]),
        eventState=EventState.normal,
        outOfService=Boolean(False),
        polarity=Polarity.normal,
        inactiveText=CharacterString("Standard"),
        activeText=CharacterString("DST"),
    )
    ai_y = _ai(
        43,
        "Site-Year",
        "Site-local calendar year (dimensionless)",
        EngineeringUnits.noUnits,
    )
    ai_mo = _ai(
        44,
        "Site-Month",
        "Site-local month 1–12 (dimensionless)",
        EngineeringUnits.noUnits,
    )
    ai_d = _ai(
        45,
        "Site-Day",
        "Site-local day of month 1–31 (dimensionless)",
        EngineeringUnits.noUnits,
    )
    ai_h = _ai(
        46,
        "Site-Hour",
        "Site-local hour 0–23 (dimensionless)",
        EngineeringUnits.noUnits,
    )
    ai_mi = _ai(
        47,
        "Site-Minute",
        "Site-local minute 0–59 (dimensionless)",
        EngineeringUnits.noUnits,
    )
    ai_s = _ai(
        48,
        "Site-Second",
        "Site-local second 0–59 (dimensionless)",
        EngineeringUnits.noUnits,
    )
    ai_wd = _ai(
        49,
        "Site-Weekday-Number",
        "ISO weekday 1=Monday … 7=Sunday (dimensionless)",
        EngineeringUnits.noUnits,
    )
    st_weekday = ArrayOf(StateTextString)(
        [
            StateTextString("Monday"),
            StateTextString("Tuesday"),
            StateTextString("Wednesday"),
            StateTextString("Thursday"),
            StateTextString("Friday"),
            StateTextString("Saturday"),
            StateTextString("Sunday"),
        ]
    )
    msi_wd = _EdgeMultiStateInput(
        objectIdentifier=ObjectIdentifier("multiStateInput,2"),
        objectName=CharacterString("Site-Weekday"),
        description=CharacterString(
            "Site-local weekday (state text); present value 1–7 = ISO Monday–Sunday (same as analog Site-Weekday-Number)"
        ),
        presentValue=Unsigned(1),
        numberOfStates=Unsigned(7),
        stateText=st_weekday,
        **common,
    )
    ai_off = _ai(
        50,
        "Site-UTC-Offset-Minutes",
        "Site-local offset from UTC in minutes (zoneinfo)",
        EngineeringUnits.minutes,
    )
    return (
        csv_dt,
        csv_tz,
        csv_date,
        csv_time,
        bi_ok,
        bi_dst,
        ai_y,
        ai_mo,
        ai_d,
        ai_h,
        ai_mi,
        ai_s,
        ai_wd,
        msi_wd,
        ai_off,
    )


def _create_schedule_context_objects() -> tuple[
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    _EdgeCharacterStringValue,
    _EdgeCharacterStringValue,
    _EdgeCharacterStringValue,
]:
    """
    Holiday + sun schedule (Nager.Date + Open-Meteo daily sunrise/sunset).

    Instance reservation:
      - binary-input 20–25: holiday/business/long-weekend/daylight + API OK flags
      - characterstringValue 12–14: holiday name, sunrise, sunset (ISO local strings)
    """
    zf = StatusFlags([0, 0, 0, 0])
    common = {
        "statusFlags": zf,
        "eventState": EventState.normal,
        "outOfService": Boolean(False),
    }

    def _bi(
        instance: int,
        name: str,
        desc: str,
        inactive: str,
        active: str,
    ) -> BinaryInputObject:
        return BinaryInputObject(
            objectIdentifier=ObjectIdentifier(f"binary-input,{instance}"),
            objectName=CharacterString(name),
            description=CharacterString(desc),
            presentValue=BinaryPV.inactive,
            polarity=Polarity.normal,
            inactiveText=CharacterString(inactive),
            activeText=CharacterString(active),
            **common,
        )

    bi_ht = _bi(20, "Holiday-Today", "Site-local date is a public holiday (Nager.Date)", "No", "Yes")
    bi_bd = _bi(21, "Business-Day", "Monday–Friday and not a public holiday", "No", "Yes")
    bi_lw = _bi(22, "Long-Weekend", "Holiday on Fri/Sat/Sun or Monday holiday (coarse)", "No", "Yes")
    bi_dw = _bi(23, "Daylight-Window", "Local time between sunrise and sunset", "Night", "Day")
    bi_hok = _bi(24, "Holiday-API-OK", "Nager.Date fetch succeeded for configured country/year", "Offline", "Online")
    bi_sok = _bi(25, "Sun-Data-OK", "Sunrise/sunset fetch succeeded", "Offline", "Online")

    csv_hn = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,12"),
        objectName=CharacterString("Holiday-Name"),
        description=CharacterString("Public holiday name when Holiday-Today is active"),
        presentValue=CharacterString(""),
        **common,
    )
    csv_sr = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,13"),
        objectName=CharacterString("Site-Sunrise-Time"),
        description=CharacterString("Today sunrise (site-local ISO-8601 with offset)"),
        presentValue=CharacterString(""),
        **common,
    )
    csv_ss = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,14"),
        objectName=CharacterString("Site-Sunset-Time"),
        description=CharacterString("Today sunset (site-local ISO-8601 with offset)"),
        presentValue=CharacterString(""),
        **common,
    )
    return bi_ht, bi_bd, bi_lw, bi_dw, bi_hok, bi_sok, csv_hn, csv_sr, csv_ss


def _create_agent_config_snapshot_objects() -> tuple[
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    AnalogInputObject,
    BinaryInputObject,
    BinaryInputObject,
    BinaryInputObject,
    _EdgeCharacterStringValue,
]:
    """
    Read-only mirror of effective SaaS ``agent`` tuning + env defaults (job / weather / site timers).

    Instance reservation (gap analog-input 21–33 before AQ block 34–42; BI 26–28; CSV 15):
      - analog-input 21–32: poll + heartbeat + config + edge status + Who-Is + read-device limits
        + weather lat/lon + weather/site/schedule poll intervals
      - analog-input 33: SaaS online threshold (edge status BI; env only, not in agent JSON)
      - binary-input 26–28: weather master active (enabled + coords), display °F, polling desired
      - characterstringValue 15: site country code (ISO 3166-1 alpha-2) for holidays
    """
    zf = StatusFlags([0, 0, 0, 0])
    common = {
        "statusFlags": zf,
        "eventState": EventState.normal,
        "outOfService": Boolean(False),
    }

    def _ai(
        instance: int,
        name: str,
        desc: str,
        units: EngineeringUnits,
        cov: float,
    ) -> AnalogInputObject:
        return AnalogInputObject(
            objectIdentifier=ObjectIdentifier(f"analog-input,{instance}"),
            objectName=CharacterString(name),
            description=CharacterString(desc),
            presentValue=Real(0.0),
            covIncrement=Real(float(cov)),
            units=units,
            **common,
        )

    ai_poll = _ai(
        21,
        "Agent-JobPollInterval",
        "Effective job poll interval (seconds)",
        EngineeringUnits.seconds,
        0.5,
    )
    ai_hb = _ai(
        22,
        "Agent-HeartbeatInterval",
        "Effective SaaS heartbeat interval (seconds)",
        EngineeringUnits.seconds,
        0.5,
    )
    ai_cfg = _ai(
        23,
        "Agent-ConfigPollInterval",
        "Effective remote config poll interval (seconds)",
        EngineeringUnits.seconds,
        0.5,
    )
    ai_edge = _ai(
        24,
        "Agent-EdgeStatusInterval",
        "Effective edge status / uptime refresh interval (seconds)",
        EngineeringUnits.seconds,
        0.5,
    )
    ai_who = _ai(
        25,
        "Agent-WhoIsTimeout",
        "Effective Who-Is timeout for discovery jobs (seconds)",
        EngineeringUnits.seconds,
        0.1,
    )
    ai_rmax = _ai(
        26,
        "Agent-ReadDeviceLiveMaxObjects",
        "Effective max objects per read_device_live job (dimensionless)",
        EngineeringUnits.noUnits,
        1.0,
    )
    ai_rtmo = _ai(
        27,
        "Agent-ReadDeviceLiveTimeout",
        "Effective read_device_live deadline (seconds)",
        EngineeringUnits.seconds,
        0.5,
    )
    ai_lat = _ai(
        28,
        "Agent-Weather-Latitude",
        "SaaS weather latitude (decimal degrees; 0 if unset)",
        EngineeringUnits.noUnits,
        1e-4,
    )
    ai_lon = _ai(
        29,
        "Agent-Weather-Longitude",
        "SaaS weather longitude (decimal degrees; 0 if unset)",
        EngineeringUnits.noUnits,
        1e-4,
    )
    ai_wxp = _ai(
        30,
        "Agent-WeatherPollInterval",
        "Effective Open-Meteo poll interval (seconds)",
        EngineeringUnits.seconds,
        1.0,
    )
    ai_st = _ai(
        31,
        "Agent-SiteTimePollInterval",
        "Effective site-local time BACnet refresh interval (seconds)",
        EngineeringUnits.seconds,
        0.1,
    )
    ai_sch = _ai(
        32,
        "Agent-ScheduleContextPollInterval",
        "Effective holiday + sun schedule poll interval (seconds)",
        EngineeringUnits.seconds,
        0.5,
    )
    ai_saas_thr = _ai(
        33,
        "Agent-SaaSOnlineThreshold",
        "Heartbeat staleness window for Edge-SaaS BI (seconds; env)",
        EngineeringUnits.seconds,
        0.5,
    )

    def _bi(
        instance: int,
        name: str,
        desc: str,
        inactive: str,
        active: str,
    ) -> BinaryInputObject:
        return BinaryInputObject(
            objectIdentifier=ObjectIdentifier(f"binary-input,{instance}"),
            objectName=CharacterString(name),
            description=CharacterString(desc),
            presentValue=BinaryPV.inactive,
            polarity=Polarity.normal,
            inactiveText=CharacterString(inactive),
            activeText=CharacterString(active),
            **common,
        )

    bi_wx_ok = _bi(
        26,
        "Agent-Weather-Master-Active",
        "SaaS weather_enabled and valid lat/lon (same gate as weather poll task)",
        "No",
        "Yes",
    )
    bi_wx_f = _bi(
        27,
        "Agent-Weather-Display-Fahrenheit",
        "SaaS temperature display: inactive=Celsius, active=Fahrenheit",
        "Celsius",
        "Fahrenheit",
    )
    bi_wx_poll = _bi(
        28,
        "Agent-Weather-Polling-Desired",
        "SaaS default for Weather-Polling BV when key omitted (read-only mirror)",
        "No",
        "Yes",
    )

    csv_cc = _EdgeCharacterStringValue(
        objectIdentifier=ObjectIdentifier("characterstringValue,15"),
        objectName=CharacterString("Agent-SiteCountryCode"),
        description=CharacterString("ISO 3166-1 alpha-2 for Nager.Date holidays (empty if unset)"),
        presentValue=CharacterString(""),
        **common,
    )

    return (
        ai_poll,
        ai_hb,
        ai_cfg,
        ai_edge,
        ai_who,
        ai_rmax,
        ai_rtmo,
        ai_lat,
        ai_lon,
        ai_wxp,
        ai_st,
        ai_sch,
        ai_saas_thr,
        bi_wx_ok,
        bi_wx_f,
        bi_wx_poll,
        csv_cc,
    )


def format_bacpypes_device_address(bind_ip: str, bind_prefix: int, udp_port: int) -> str:
    """
    BACpypes3 parses bare ip:port as /32; then addrBroadcastTuple == addrTuple and
    Who-Is (LocalBroadcast) raises RuntimeError('no broadcast'). Use ip/prefix:port.
    If bind_ip already contains '/' (e.g. 192.168.1.5/24), only append :port.
    """
    ip = bind_ip.strip()
    if not ip:
        return ""
    if "/" in ip:
        return f"{ip}:{udp_port}"
    return f"{ip}/{int(bind_prefix)}:{udp_port}"


def _camel_to_kebab(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1-\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1-\2", s1).lower()


# Plain normalized type (no separators) -> BACnet object-id token for ReadProperty.
# Without this, "binaryvalue" becomes "binaryvalue,1" and stacks often reject it;
# "binary-value,1" works. Same for analog-value, multi-state-value, etc.
_PLAIN_KIND_TO_OBJECT_ID_TOKEN: dict[str, str] = {
    "analoginput": "analog-input",
    "analogoutput": "analog-output",
    "analogvalue": "analog-value",
    "binaryinput": "binary-input",
    "binaryoutput": "binary-output",
    "binaryvalue": "binary-value",
    "multistateinput": "multi-state-input",
    "multistateoutput": "multi-state-output",
    "multistatevalue": "multi-state-value",
    "characterstringvalue": "character-string-value",
    "notificationclass": "notification-class",
    "trendlog": "trend-log",
    "trendlogmultiple": "trend-log-multiple",
    "eventenrollment": "event-enrollment",
}


def _object_type_label(raw: Any) -> str:
    """
    Stable BACnet object-type label for planning reads and JSON rows.
    Handles BACpypes enums (use .name), 'ObjectType.analogValue' str forms, etc.
    """
    if raw is None:
        return ""
    name = getattr(raw, "name", None)
    if isinstance(name, str) and name.strip():
        base = name.strip()
    else:
        base = str(raw).strip()
    if "." in base:
        base = base.rsplit(".", 1)[-1]
    return base


def _object_type_kind_key(object_type: Any) -> str:
    """Normalize for _snapshot_property_plan (camel, kebab, snake, spaces)."""
    label = _object_type_label(object_type)
    return label.lower().replace("-", "").replace("_", "").replace(" ", "")


def _object_id_string(object_type: str, object_instance: int) -> str:
    # BACpypes3 ObjectIdentifier string parsing requires "type,instance" or "type:instance"
    # (a space separator is rejected and breaks every read_property on objects).
    p = str(object_type).strip()
    pk = _object_type_kind_key(p)
    if pk in _PLAIN_KIND_TO_OBJECT_ID_TOKEN:
        p = _PLAIN_KIND_TO_OBJECT_ID_TOKEN[pk]
    return f"{_camel_to_kebab(p)},{object_instance}"


def _object_type_for_json(ot_label: str) -> str:
    """Kebab-case object_type for API consumers (matches Explorer / SaaS)."""
    pk = _object_type_kind_key(ot_label)
    if pk in _PLAIN_KIND_TO_OBJECT_ID_TOKEN:
        return _PLAIN_KIND_TO_OBJECT_ID_TOKEN[pk]
    return _camel_to_kebab(ot_label)


def _bacnet_property_identifier(prop: str) -> str:
    """Normalize SaaS property names to BACnet property id (kebab-case)."""
    p = str(prop).strip()
    if not p:
        return p
    if not any(c.isupper() for c in p):
        return p.lower().replace("_", "-")
    return _camel_to_kebab(p)


def _json_key_for_bacnet_property(prop_kebab: str) -> str:
    """Stable snake_case key for readback JSON (e.g. present-value -> present_value)."""
    return str(prop_kebab).replace("-", "_")


async def _object_identifiers(app: Application, device_address: Address, device_identifier: ObjectIdentifier):
    try:
        object_list = await app.read_property(device_address, device_identifier, "object-list")
        if isinstance(object_list, ErrorRejectAbortNack):
            _log.debug("object-list error response: %s", object_list)
            return []
        return list(object_list)
    except AbortPDU as err:
        if err.apduAbortRejectReason != AbortReason.segmentationNotSupported:
            _log.debug("object-list abort: %s", err)
            return []
    except ErrorRejectAbortNack as err:
        _log.debug("object-list err: %s", err)
        return []

    object_list: list[Any] = []
    try:
        object_list_length = await app.read_property(
            device_address,
            device_identifier,
            "object-list",
            array_index=0,
        )
        if isinstance(object_list_length, ErrorRejectAbortNack):
            _log.debug("object-list length error: %s", object_list_length)
            return []
        for i in range(int(object_list_length)):
            oid = await app.read_property(
                device_address,
                device_identifier,
                "object-list",
                array_index=i + 1,
            )
            if isinstance(oid, ErrorRejectAbortNack):
                _log.debug("object-list element error: %s", oid)
                break
            object_list.append(oid)
    except ErrorRejectAbortNack as err:
        _log.debug("object-list indexed err: %s", err)

    return object_list


def _is_device_object_type(object_type: Any) -> bool:
    """True for BACnet device object (object-list entry); works with str or BACpypes enum."""
    label = getattr(object_type, "name", object_type)
    if not isinstance(label, str):
        label = str(label)
    return _object_type_kind_key(label) == "device"


def _is_binary_object_type(object_type: Any) -> bool:
    return _object_type_kind_key(object_type).startswith("binary")


def _is_multistate_object_type(object_type: Any) -> bool:
    return _object_type_kind_key(object_type).startswith("multistate")


def _snapshot_property_plan(object_type: Any) -> tuple[list[tuple[str, str]], bool]:
    """
    BACnet properties to read per object type (property id, JSON key).
    The bool is True when we should try an extra optional (silent) reliability read
    for stacks that expose it on BV/AV/etc.
    """
    k = _object_type_kind_key(object_type)
    base: list[tuple[str, str]] = [
        ("object-name", "object_name"),
        ("description", "description"),
    ]
    if k.isdigit():
        return base, False
    meta_only = frozenset(
        {
            "file",
            "notificationclass",
            "eventenrollment",
            "program",
            "trendlog",
            "trendlogmultiple",
        }
    )
    if k in meta_only:
        return base, False
    if k == "schedule":
        # present-value is often a constructed schedule; skip bulk read (avoids repr leaks).
        return base, False
    tail_pv = [
        ("present-value", "present_value"),
        ("status-flags", "status_flags"),
        ("out-of-service", "out_of_service"),
    ]
    rel: tuple[str, str] = ("reliability", "reliability")
    # Objects with Priority_Array normally expose Relinquish_Default (not analog/binary inputs).
    rd: tuple[str, str] = ("relinquish-default", "relinquish_default")
    pa: tuple[str, str] = ("priority-array", "priority_array")
    if k == "calendar":
        return base + [("present-value", "present_value")], False
    if k in ("analoginput", "analogoutput"):
        ao = base + [("units", "units")] + tail_pv + [rel]
        if k == "analogoutput":
            ao.append(rd)
            ao.append(pa)
        return ao, False
    if k == "analogvalue":
        return base + [("units", "units")] + tail_pv + [rd, pa], True
    if k.startswith("binary") or k.startswith("multistate") or k == "characterstringvalue":
        row = base + list(tail_pv)
        if k in (
            "binaryoutput",
            "binaryvalue",
            "multistateoutput",
            "multistatevalue",
            "characterstringvalue",
        ):
            row.append(rd)
        if k in (
            "binaryoutput",
            "binaryvalue",
            "multistateoutput",
            "multistatevalue",
        ):
            row.append(pa)
        return row, True
    if k == "loop":
        return base + tail_pv, True
    return base + tail_pv, True


def _coerce_present_value_active(pv: Any) -> Optional[bool]:
    """Map BACnet binary present-value (enum / int / str) to True=active, False=inactive."""
    if pv is None:
        return None
    if isinstance(pv, bool):
        return pv
    name = getattr(pv, "name", None)
    if isinstance(name, str):
        n = name.lower()
        if n == "active":
            return True
        if n == "inactive":
            return False
    s = str(pv).lower()
    if s in ("active", "1", "true"):
        return True
    if s in ("inactive", "0", "false"):
        return False
    try:
        i = int(pv)
        if i == 1:
            return True
        if i == 0:
            return False
    except (TypeError, ValueError):
        pass
    return None


def _present_value_label(
    pv: Any,
    object_type: str,
    active_text: Optional[str],
    inactive_text: Optional[str],
    state_text: Optional[list[str]],
) -> Optional[str]:
    if _is_binary_object_type(object_type):
        side = _coerce_present_value_active(pv)
        if side is True:
            return active_text or "active"
        if side is False:
            return inactive_text or "inactive"
    if _is_multistate_object_type(object_type) and state_text:
        try:
            idx = int(pv) - 1
            if 0 <= idx < len(state_text):
                return state_text[idx]
        except (TypeError, ValueError):
            pass
    return None


async def _snap_read_property_ex(
    app: Application,
    addr: Address,
    oid: Union[ObjectIdentifier, str],
    prop: str,
    read_timeout: float,
    errors: list[dict[str, Any]],
    err_extra: dict[str, Any],
    array_index: Optional[int] = None,
    *,
    record_error: bool = True,
) -> tuple[Any, bool]:
    """
    Returns (value, success). success is False on NACK/error; value may be None
    on success (e.g. BACnet null) — caller decides whether to set a JSON key.
    """
    try:
        if array_index is not None:
            val = await asyncio.wait_for(
                app.read_property(addr, oid, prop, array_index=array_index),
                timeout=read_timeout,
            )
        else:
            val = await asyncio.wait_for(
                app.read_property(addr, oid, prop),
                timeout=read_timeout,
            )
        if isinstance(val, ErrorRejectAbortNack):
            if record_error:
                errors.append(
                    {
                        **err_extra,
                        "property": prop,
                        "message": failure_message(
                            val, default="read property rejected"
                        ),
                    }
                )
            return None, False
        return val, True
    except ErrorRejectAbortNack as err:
        if record_error:
            errors.append(
                {
                    **err_extra,
                    "property": prop,
                    "message": failure_message(
                        err, default="read property rejected"
                    ),
                }
            )
        return None, False
    except Exception as e:
        if record_error:
            errors.append(
                {
                    **err_extra,
                    "property": prop,
                    "message": failure_message(
                        e, default="read property exception"
                    ),
                }
            )
        return None, False


async def _snap_read_property(
    app: Application,
    addr: Address,
    oid: Union[ObjectIdentifier, str],
    prop: str,
    read_timeout: float,
    errors: list[dict[str, Any]],
    err_extra: dict[str, Any],
    array_index: Optional[int] = None,
    *,
    record_error: bool = True,
) -> Any:
    val, _ok = await _snap_read_property_ex(
        app,
        addr,
        oid,
        prop,
        read_timeout,
        errors,
        err_extra,
        array_index,
        record_error=record_error,
    )
    return val


def _priority_array_whole_has_live_slot(whole: Any) -> bool:
    """
    True if at least one priority slot decodes to a non-null JSON value.
    Some devices return a 'successful' full-array read of 16 empty slots while
    indexed reads 1..16 return the real PriorityValues (see priority-array reads).
    """
    try:
        n = len(whole)  # type: ignore[arg-type]
    except TypeError:
        return True
    for i in range(min(n, 16)):
        try:
            slot = whole[i]
        except (IndexError, TypeError):
            return True
        if to_json_safe(slot) is not None:
            return True
    return False


def _priority_array_whole_is_usable(whole: Any) -> bool:
    if whole is None:
        return False
    try:
        n = len(whole)
    except TypeError:
        return False
    if n != 16:
        return False
    # Bulk read may succeed with 16 empty-looking slots; prefer indexed fallback.
    if not _priority_array_whole_has_live_slot(whole):
        return False
    return True


async def _read_priority_array_for_snapshot(
    app: Application,
    addr: Address,
    oid: str,
    read_timeout: float,
    errors: list[dict[str, Any]],
    err_extra: dict[str, Any],
) -> list[Any]:
    """
    Many devices reject or truncate a single ReadProperty on the full
    priority-array; fall back to indexed reads 1..16 (like state-text).
    """
    whole, whole_ok = await _snap_read_property_ex(
        app,
        addr,
        oid,
        "priority-array",
        read_timeout,
        errors,
        err_extra,
        record_error=False,
    )
    if whole_ok and _priority_array_whole_is_usable(whole):
        return list(whole)

    slots: list[Any] = []
    for i in range(1, 17):
        part, _ok = await _snap_read_property_ex(
            app,
            addr,
            oid,
            "priority-array",
            read_timeout,
            errors,
            err_extra,
            array_index=i,
            record_error=False,
        )
        slots.append(part)
    return slots


def _bacnet_relinquish_present_value_as_null() -> Any:
    """
    WriteProperty to present-value with priority: BACnet NULL relinquishes that slot.
    BACpypes3 write_property skips coercion only for primitivedata.Null when priority is set.
    """
    from bacpypes3.primitivedata import Null

    return Null(())


def _bacnet_null_priority_array_element() -> Any:
    """priority-array[index] relinquish — element type is PriorityValue."""
    from bacpypes3.basetypes import PriorityValue

    return PriorityValue(null=())


def _normalize_write_value_for_bacnet(
    pid: str,
    val: Any,
    priority: Optional[int],
    array_index: Optional[int],
) -> Any:
    if val is not None:
        return val
    if pid == "priority-array" and array_index is not None:
        return _bacnet_null_priority_array_element()
    if pid == "present-value" and priority is not None:
        return _bacnet_relinquish_present_value_as_null()
    return val


async def _list_of_initial_values_for_create_object(
    app: Application,
    addr: Address,
    vendor_info: Any,
    new_object_type_str: str,
    initial_properties: list[dict[str, Any]],
) -> tuple[Optional[Any], Optional[str]]:
    """
    Build BACnet SequenceOf(PropertyValue) for CreateObject listOfInitialValues.
    Uses the same property typing/coercion rules as WriteProperty.
    Returns (None, None) when initial_properties is empty (omit listOfInitialValues).
    """
    if not initial_properties:
        return None, None

    try:
        oid_template = await app.parse_object_identifier(
            _object_id_string(new_object_type_str, 1),
            vendor_info=vendor_info,
        )
    except (TypeError, ValueError) as e:
        return None, failure_message(e, default="invalid object_type for create")

    object_class = vendor_info.get_object_class(oid_template[0])
    if not object_class:
        return None, "no object class for type (vendor mapping)"

    # Match CreateObjectRequest.listOfInitialValues (bacpypes3 apdu.py).
    seq_cls = SequenceOf(PropertyValue, _context=1, _optional=True)
    out: list[PropertyValue] = []

    for spec in initial_properties:
        if not isinstance(spec, dict):
            return None, "initial_properties entry must be an object"
        prop_raw = spec.get("property")
        if prop_raw is None or str(prop_raw).strip() == "":
            return None, "missing property in initial_properties"
        if "value" not in spec:
            return None, "missing value in initial_properties (use null when applicable)"

        pid = _bacnet_property_identifier(str(prop_raw))
        if not pid:
            return None, "empty property id"

        val = spec["value"]
        pri = spec.get("priority")
        if pri is not None:
            pri = int(pri)
        arr_idx = spec.get("array_index")
        if arr_idx is not None:
            arr_idx = int(arr_idx)

        if pid == "present-value":
            if val is None and pri is None:
                return None, (
                    "present-value null (relinquish) requires priority 1-16 in "
                    "initial_properties, or use priority-array with array_index"
                )
            if pri is not None and (pri < 1 or pri > 16):
                return None, "priority must be 1-16 for present-value"
        if pid == "priority-array" and arr_idx is None:
            return None, "priority-array requires array_index (1-16)"
        if (
            pid == "priority-array"
            and arr_idx is not None
            and (arr_idx < 1 or arr_idx > 16)
        ):
            return None, "priority-array array_index must be 1-16"
        if pid == "present-value":
            if arr_idx is not None and pri is None:
                return None, (
                    "present-value uses BACnet priority (1-16), not array_index; "
                    "omit array_index, set priority for that slot, or use property "
                    "priority-array with array_index"
                )

        val = _normalize_write_value_for_bacnet(pid, val, pri, arr_idx)

        try:
            prop_ref = await app.parse_property_reference(
                pid, vendor_info=vendor_info
            )
        except (TypeError, ValueError) as e:
            return None, failure_message(e, default="invalid property reference")

        prop_enum = prop_ref.propertyIdentifier
        if prop_ref.propertyArrayIndex is not None and arr_idx is None:
            arr_idx = int(prop_ref.propertyArrayIndex)

        property_type = object_class.get_property_type(prop_enum)
        if not property_type:
            return None, f"unknown property for object type: {pid}"

        if issubclass(property_type, Array):
            if arr_idx is None:
                pass
            elif arr_idx == 0:
                property_type = Unsigned
            else:
                property_type = property_type._subtype

        if (pri is not None) and isinstance(val, Null):
            pass
        elif not isinstance(val, property_type):
            try:
                val = property_type(val)
            except (TypeError, ValueError) as e:
                return None, failure_message(
                    e, default=f"value coercion failed for {pid}"
                )

        pv = PropertyValue(
            propertyIdentifier=prop_enum,
            value=val,
        )
        if arr_idx is not None:
            pv.propertyArrayIndex = Unsigned(arr_idx)
        if pri is not None:
            pv.priority = Unsigned(pri)
        out.append(pv)

    return seq_cls(out), None


def _iter_state_text_sequence(raw: Any) -> Optional[list[str]]:
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return None
    if isinstance(raw, (list, tuple)):
        if not raw:
            return None
        return ["" if x is None else str(x) for x in raw]
    try:
        it = iter(raw)
    except TypeError:
        return None
    items = list(it)
    if not items:
        return None
    return ["" if x is None else str(x) for x in items]


async def _read_multistate_state_text(
    app: Application,
    addr: Address,
    oid: ObjectIdentifier,
    read_timeout: float,
    errors: list[dict[str, Any]],
    err_extra: dict[str, Any],
) -> tuple[Optional[int], list[str]]:
    # Some devices return the full array in one read; element-wise reads fail or are slow.
    whole = await _snap_read_property(
        app,
        addr,
        oid,
        "state-text",
        read_timeout,
        errors,
        err_extra,
        record_error=False,
    )
    texts = _iter_state_text_sequence(whole)
    if texts:
        return len(texts), texts

    nraw = await _snap_read_property(
        app, addr, oid, "number-of-states", read_timeout, errors, err_extra
    )
    if nraw is None:
        return None, []
    try:
        n = int(nraw)
    except (TypeError, ValueError):
        return None, []
    if n < 1:
        return n, []
    out: list[str] = []
    for i in range(1, n + 1):
        part = await _snap_read_property(
            app,
            addr,
            oid,
            "state-text",
            read_timeout,
            errors,
            err_extra,
            array_index=i,
        )
        out.append("" if part is None else str(part))
    return n, out


def _is_present_value_property(prop: str) -> bool:
    p = prop.replace("present-value", "presentValue").strip().lower()
    return p == "presentvalue"


async def _build_snapshot_style_object_entry(
    app: Application,
    addr: Address,
    device_instance: int,
    object_type: Any,
    object_instance: int,
    read_timeout: float,
    errors: list[dict[str, Any]],
    *,
    read_oid: Optional[Any] = None,
    present_value_precooked: Optional[Any] = None,
) -> dict[str, Any]:
    """One BACnet object's snapshot-shaped row (same keys as snapshot_network objects[])."""
    ot = _object_type_label(object_type)
    oi = int(object_instance)
    oid = read_oid if read_oid is not None else _object_id_string(ot, oi)
    ot_json = _object_type_for_json(ot)
    err_obj: dict[str, Any] = {
        "device_instance": device_instance,
        "object_type": ot_json,
        "object_instance": oi,
    }
    entry: dict[str, Any] = {
        "object_type": ot_json,
        "object_instance": oi,
    }
    plan, try_optional_reliability = _snapshot_property_plan(ot)
    for prop, key in plan:
        if present_value_precooked is not None and key == "present_value":
            entry[key] = present_value_precooked
            continue
        if key == "priority_array":
            entry[key] = await _read_priority_array_for_snapshot(
                app, addr, oid, read_timeout, errors, err_obj
            )
            continue
        if key == "relinquish_default":
            val_rd, ok_rd = await _snap_read_property_ex(
                app, addr, oid, prop, read_timeout, errors, err_obj
            )
            if ok_rd:
                entry[key] = val_rd
            continue
        val = await _snap_read_property(
            app, addr, oid, prop, read_timeout, errors, err_obj
        )
        if val is not None:
            entry[key] = val
    if try_optional_reliability and "reliability" not in entry:
        r = await _snap_read_property(
            app,
            addr,
            oid,
            "reliability",
            read_timeout,
            errors,
            err_obj,
            record_error=False,
        )
        if r is not None:
            entry["reliability"] = r

    active_text: Optional[str] = None
    inactive_text: Optional[str] = None
    state_text: Optional[list[str]] = None
    if _is_binary_object_type(ot):
        at = await _snap_read_property(
            app, addr, oid, "active-text", read_timeout, errors, err_obj
        )
        it = await _snap_read_property(
            app, addr, oid, "inactive-text", read_timeout, errors, err_obj
        )
        if at is not None:
            active_text = str(at)
            entry["active_text"] = active_text
        if it is not None:
            inactive_text = str(it)
            entry["inactive_text"] = inactive_text
    elif _is_multistate_object_type(ot):
        n_states, texts = await _read_multistate_state_text(
            app, addr, oid, read_timeout, errors, err_obj
        )
        if n_states is not None:
            entry["number_of_states"] = n_states
        if texts:
            state_text = texts
            entry["state_text"] = texts

    label = _present_value_label(
        entry.get("present_value"),
        ot,
        active_text,
        inactive_text,
        state_text,
    )
    if label is not None:
        entry["present_value_label"] = label

    return entry


def _create_edge_status_binary_inputs() -> tuple[BinaryInputObject, BinaryInputObject]:
    """
    Two local binary-input objects on the edge device: WAN check and SaaS heartbeat liveness.
    inactive/active texts are Offline/Online (present-value label via BACnet state text).
    """
    def _bi_common() -> dict[str, Any]:
        return {
            "statusFlags": StatusFlags([0, 0, 0, 0]),
            "eventState": EventState.normal,
            "outOfService": Boolean(False),
            "polarity": Polarity.normal,
            "inactiveText": CharacterString("Offline"),
            "activeText": CharacterString("Online"),
        }

    internet = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,1"),
        objectName=CharacterString("Edge-Internet"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("Internet / WAN (HTTP reachability)"),
        **_bi_common(),
    )
    saas = BinaryInputObject(
        objectIdentifier=ObjectIdentifier("binary-input,2"),
        objectName=CharacterString("Edge-SaaS"),
        presentValue=BinaryPV.inactive,
        description=CharacterString("SaaS API heartbeat within online threshold"),
        **_bi_common(),
    )
    return internet, saas


def _patch_local_device_object_types_supported(app: Application) -> None:
    """
    BACpypes3's local DeviceObject returns an empty protocol-object-types-supported
    bitstring. Many supervisors only expose object types that are marked supported,
    so binary-input points would not appear even though object-list contains them.
    """
    dev = app.device_object
    base = dev.__class__

    class _DeviceWithObjectTypesSupported(base):
        @property
        def protocolObjectTypesSupported(self) -> ObjectTypesSupported:
            ots = ObjectTypesSupported([0] * 63)
            ots[ObjectTypesSupported.analogInput] = 1
            ots[ObjectTypesSupported.binaryInput] = 1
            ots[ObjectTypesSupported.multiStateInput] = 1
            ots[ObjectTypesSupported.characterstringValue] = 1
            ots[ObjectTypesSupported.binaryValue] = 1
            ots[ObjectTypesSupported.device] = 1
            ots[ObjectTypesSupported.networkPort] = 1
            return ots

    dev.__class__ = _DeviceWithObjectTypesSupported


def _resolved_edge_agent_version(settings: Settings) -> str:
    v = (settings.software_version or "").strip()
    if v:
        return v
    try:
        return package_version("edge-agent")
    except PackageNotFoundError:
        return "unknown"


def _apply_device_metadata(
    app: Application,
    settings: Settings,
    saas_config_revision: Optional[int],
) -> None:
    """
    BACnet device metadata:
    - application-software-version = SaaS remote config revision (same concept as heartbeat bacnet_config_revision).
    - firmware-revision = edge-agent package / SOFTWARE_VERSION from env.
    - database-revision = same config revision as Unsigned (0 if never synced).
    """
    ver = _resolved_edge_agent_version(settings)
    app.device_object.firmwareRevision = CharacterString(ver)
    if saas_config_revision is not None:
        app.device_object.applicationSoftwareVersion = CharacterString(str(int(saas_config_revision)))
        app.device_object.databaseRevision = Unsigned(int(saas_config_revision))
    else:
        app.device_object.applicationSoftwareVersion = CharacterString("none")
        app.device_object.databaseRevision = Unsigned(0)
    vendor = (settings.bacnet_vendor_name or "").strip() or "bmsOS"
    app.device_object.vendorName = CharacterString(vendor)
    model = (settings.bacnet_model_name or "").strip() or "bmOS-edge"
    app.device_object.modelName = CharacterString(model)


class BacnetPypesClient:
    """Wraps BACpypes3 Application; recreate via manager on config change."""

    def __init__(self, settings: Settings, effective: EffectiveBacnetConfig, storage: Storage) -> None:
        self._settings = settings
        self._effective = effective
        self._storage = storage
        self._app: Optional[Application] = None
        self._bi_internet: Optional[BinaryInputObject] = None
        self._bi_saas: Optional[BinaryInputObject] = None
        self._ai_uptime: Optional[AnalogInputObject] = None
        self._csv_hostname: Optional[_EdgeCharacterStringValue] = None
        self._csv_box_id: Optional[_EdgeCharacterStringValue] = None
        self._csv_saas_base: Optional[_EdgeCharacterStringValue] = None
        self._csv_last_job: Optional[_EdgeCharacterStringValue] = None
        self._msi_last_job: Optional[_EdgeMultiStateInput] = None
        self._ai_weather_temp: Optional[AnalogInputObject] = None
        self._ai_weather_rh: Optional[AnalogInputObject] = None
        self._ai_weather_wind: Optional[AnalogInputObject] = None
        self._ai_weather_precip: Optional[AnalogInputObject] = None
        self._ai_weather_apparent: Optional[AnalogInputObject] = None
        self._ai_weather_rain: Optional[AnalogInputObject] = None
        self._ai_weather_showers: Optional[AnalogInputObject] = None
        self._ai_weather_snow: Optional[AnalogInputObject] = None
        self._ai_weather_code: Optional[AnalogInputObject] = None
        self._ai_weather_cloud: Optional[AnalogInputObject] = None
        self._ai_weather_pmsl: Optional[AnalogInputObject] = None
        self._ai_weather_psurf: Optional[AnalogInputObject] = None
        self._ai_weather_wdir: Optional[AnalogInputObject] = None
        self._ai_weather_wgust: Optional[AnalogInputObject] = None
        self._bi_weather_ok: Optional[BinaryInputObject] = None
        self._bi_weather_unit_of_measure: Optional[BinaryInputObject] = None
        self._bi_weather_is_day: Optional[BinaryInputObject] = None
        self._bv_weather_polling: Optional[BinaryValueObject] = None
        self._csv_weather_last: Optional[_EdgeCharacterStringValue] = None
        self._ai_weather_dew: Optional[AnalogInputObject] = None
        self._ai_weather_heat_index: Optional[AnalogInputObject] = None
        self._ai_weather_wind_chill: Optional[AnalogInputObject] = None
        self._csv_weather_code_text: Optional[_EdgeCharacterStringValue] = None
        self._ai_weather_dew_spread: Optional[AnalogInputObject] = None
        self._ai_weather_enthalpy: Optional[AnalogInputObject] = None
        self._bi_weather_condensation_risk: Optional[BinaryInputObject] = None
        self._bi_weather_freeze_risk: Optional[BinaryInputObject] = None
        self._bi_weather_frost_risk: Optional[BinaryInputObject] = None
        self._bi_weather_solar_available: Optional[BinaryInputObject] = None
        self._bi_weather_precipitation_active: Optional[BinaryInputObject] = None
        self._bi_weather_snow_active: Optional[BinaryInputObject] = None
        self._bi_weather_high_wind: Optional[BinaryInputObject] = None
        self._bi_outdoor_smoke_risk: Optional[BinaryInputObject] = None
        self._bi_outdoor_air_quality_good: Optional[BinaryInputObject] = None
        self._bi_weather_economizer_available: Optional[BinaryInputObject] = None
        self._bi_weather_outdoor_air_usable: Optional[BinaryInputObject] = None
        self._msi_outdoor_aqi_category: Optional[_EdgeMultiStateInput] = None
        self._msi_weather_daylight_level: Optional[_EdgeMultiStateInput] = None
        self._msi_weather_wind_severity: Optional[_EdgeMultiStateInput] = None
        self._msi_weather_comfort_level: Optional[_EdgeMultiStateInput] = None
        self._msi_outdoor_dominant_pollutant: Optional[_EdgeMultiStateInput] = None
        self._msi_weather_severity: Optional[_EdgeMultiStateInput] = None
        self._msi_outdoor_heat_stress: Optional[_EdgeMultiStateInput] = None
        self._msi_outdoor_cold_stress: Optional[_EdgeMultiStateInput] = None
        self._ai_aq_co2: Optional[AnalogInputObject] = None
        self._ai_aq_pm25: Optional[AnalogInputObject] = None
        self._ai_aq_pm10: Optional[AnalogInputObject] = None
        self._ai_aq_co: Optional[AnalogInputObject] = None
        self._ai_aq_no2: Optional[AnalogInputObject] = None
        self._ai_aq_so2: Optional[AnalogInputObject] = None
        self._ai_aq_o3: Optional[AnalogInputObject] = None
        self._ai_aq_aod: Optional[AnalogInputObject] = None
        self._ai_aq_uv: Optional[AnalogInputObject] = None
        self._bi_aq_ok: Optional[BinaryInputObject] = None
        self._csv_aq_last: Optional[_EdgeCharacterStringValue] = None
        self._csv_site_local_dt: Optional[_EdgeCharacterStringValue] = None
        self._csv_site_tz: Optional[_EdgeCharacterStringValue] = None
        self._csv_site_date: Optional[_EdgeCharacterStringValue] = None
        self._csv_site_time: Optional[_EdgeCharacterStringValue] = None
        self._bi_site_time_ok: Optional[BinaryInputObject] = None
        self._bi_site_dst: Optional[BinaryInputObject] = None
        self._ai_site_year: Optional[AnalogInputObject] = None
        self._ai_site_month: Optional[AnalogInputObject] = None
        self._ai_site_day: Optional[AnalogInputObject] = None
        self._ai_site_hour: Optional[AnalogInputObject] = None
        self._ai_site_minute: Optional[AnalogInputObject] = None
        self._ai_site_second: Optional[AnalogInputObject] = None
        self._ai_site_weekday: Optional[AnalogInputObject] = None
        self._msi_site_weekday: Optional[_EdgeMultiStateInput] = None
        self._ai_site_utc_offset_min: Optional[AnalogInputObject] = None
        self._bi_holiday_today: Optional[BinaryInputObject] = None
        self._bi_business_day: Optional[BinaryInputObject] = None
        self._bi_long_weekend: Optional[BinaryInputObject] = None
        self._bi_daylight_window: Optional[BinaryInputObject] = None
        self._bi_holiday_api_ok: Optional[BinaryInputObject] = None
        self._bi_sun_data_ok: Optional[BinaryInputObject] = None
        self._csv_holiday_name: Optional[_EdgeCharacterStringValue] = None
        self._csv_site_sunrise: Optional[_EdgeCharacterStringValue] = None
        self._csv_site_sunset: Optional[_EdgeCharacterStringValue] = None
        self._ai_agent_poll_interval: Optional[AnalogInputObject] = None
        self._ai_agent_heartbeat_interval: Optional[AnalogInputObject] = None
        self._ai_agent_config_poll_interval: Optional[AnalogInputObject] = None
        self._ai_agent_edge_status_interval: Optional[AnalogInputObject] = None
        self._ai_agent_who_is_timeout: Optional[AnalogInputObject] = None
        self._ai_agent_read_device_max: Optional[AnalogInputObject] = None
        self._ai_agent_read_device_timeout: Optional[AnalogInputObject] = None
        self._ai_agent_weather_latitude: Optional[AnalogInputObject] = None
        self._ai_agent_weather_longitude: Optional[AnalogInputObject] = None
        self._ai_agent_weather_poll_interval: Optional[AnalogInputObject] = None
        self._ai_agent_site_time_poll_interval: Optional[AnalogInputObject] = None
        self._ai_agent_schedule_context_poll_interval: Optional[AnalogInputObject] = None
        self._ai_agent_saas_online_threshold: Optional[AnalogInputObject] = None
        self._bi_agent_weather_master_active: Optional[BinaryInputObject] = None
        self._bi_agent_weather_display_fahrenheit: Optional[BinaryInputObject] = None
        self._bi_agent_weather_polling_desired: Optional[BinaryInputObject] = None
        self._csv_agent_site_country_code: Optional[_EdgeCharacterStringValue] = None
        self._iam_response_effective: str = "unicast"

    def _build_application(self) -> Application:
        # BACpypes3 snapshots BACPYPES_* from os.environ when bacpypes3.argparse is
        # imported; later os.environ changes are NOT used as argparse defaults.
        # Always pass bind/instance/vendor on the CLI so .env values apply.
        parser = SimpleArgumentParser()
        cli = [
            "--name",
            self._effective.device_name,
            "--instance",
            str(self._effective.device_instance),
            "--vendoridentifier",
            str(self._effective.vendor_identifier),
        ]
        if self._effective.bind_ip.strip():
            addr = format_bacpypes_device_address(
                self._effective.bind_ip,
                self._effective.bind_prefix,
                self._effective.udp_port,
            )
            cli.extend(["--address", addr])
        args = parser.parse_args(cli)
        app = Application.from_args(args)
        _patch_local_device_object_types_supported(app)
        rev, _ = self._storage.get_remote_config_state()
        _apply_device_metadata(app, self._settings, rev)
        bi_internet, bi_saas = _create_edge_status_binary_inputs()
        app.add_object(bi_internet)
        app.add_object(bi_saas)
        self._bi_internet = bi_internet
        self._bi_saas = bi_saas
        (
            ai_uptime,
            csv_host,
            csv_box,
            csv_saas,
            csv_job,
            msi_job,
        ) = _create_agent_telemetry_objects()
        for o in (ai_uptime, csv_host, csv_box, csv_saas, csv_job, msi_job):
            app.add_object(o)
        self._ai_uptime = ai_uptime
        self._csv_hostname = csv_host
        self._csv_box_id = csv_box
        self._csv_saas_base = csv_saas
        self._csv_last_job = csv_job
        self._msi_last_job = msi_job
        wx_tuning = self._storage.get_remote_agent_tuning()
        (
            ai_wx_t,
            ai_wx_rh,
            ai_wx_w,
            ai_wx_p,
            ai_wx_app,
            ai_wx_rn,
            ai_wx_sh,
            ai_wx_sn,
            ai_wx_wc,
            ai_wx_cl,
            ai_wx_pmsl,
            ai_wx_ps,
            ai_wx_wd,
            ai_wx_wg,
            bi_wx_ok,
            bi_wx_u,
            bi_wx_day,
            bv_wx_poll,
            csv_wx,
        ) = _create_weather_objects(wx_tuning)
        for o in (
            ai_wx_t,
            ai_wx_rh,
            ai_wx_w,
            ai_wx_p,
            ai_wx_app,
            ai_wx_rn,
            ai_wx_sh,
            ai_wx_sn,
            ai_wx_wc,
            ai_wx_cl,
            ai_wx_pmsl,
            ai_wx_ps,
            ai_wx_wd,
            ai_wx_wg,
            bi_wx_ok,
            bi_wx_u,
            bi_wx_day,
            bv_wx_poll,
            csv_wx,
        ):
            app.add_object(o)
        self._ai_weather_temp = ai_wx_t
        self._ai_weather_rh = ai_wx_rh
        self._ai_weather_wind = ai_wx_w
        self._ai_weather_precip = ai_wx_p
        self._ai_weather_apparent = ai_wx_app
        self._ai_weather_rain = ai_wx_rn
        self._ai_weather_showers = ai_wx_sh
        self._ai_weather_snow = ai_wx_sn
        self._ai_weather_code = ai_wx_wc
        self._ai_weather_cloud = ai_wx_cl
        self._ai_weather_pmsl = ai_wx_pmsl
        self._ai_weather_psurf = ai_wx_ps
        self._ai_weather_wdir = ai_wx_wd
        self._ai_weather_wgust = ai_wx_wg
        self._bi_weather_ok = bi_wx_ok
        self._bi_weather_unit_of_measure = bi_wx_u
        self._bi_weather_is_day = bi_wx_day
        self._bv_weather_polling = bv_wx_poll
        self._csv_weather_last = csv_wx
        ai_wx_dew, ai_wx_hi, ai_wx_wc, csv_wx_code = _create_weather_derived_objects(wx_tuning)
        for o in (ai_wx_dew, ai_wx_hi, ai_wx_wc, csv_wx_code):
            app.add_object(o)
        self._ai_weather_dew = ai_wx_dew
        self._ai_weather_heat_index = ai_wx_hi
        self._ai_weather_wind_chill = ai_wx_wc
        self._csv_weather_code_text = csv_wx_code
        (
            ai_wx_spread,
            ai_wx_enthalpy,
            bi_wx_cond,
            bi_wx_frz,
            bi_wx_frost,
            bi_wx_solar,
            bi_wx_precip,
            bi_wx_snow,
            bi_wx_hw,
            bi_out_smoke,
            bi_out_aqg,
            bi_wx_econ,
            bi_wx_oa,
            msi_aqi,
            msi_dayl,
            msi_wsev,
            msi_comf,
            msi_dom,
            msi_wxsev,
            msi_heat,
            msi_cold,
        ) = _create_weather_decision_objects(wx_tuning)
        for o in (
            ai_wx_spread,
            ai_wx_enthalpy,
            bi_wx_cond,
            bi_wx_frz,
            bi_wx_frost,
            bi_wx_solar,
            bi_wx_precip,
            bi_wx_snow,
            bi_wx_hw,
            bi_out_smoke,
            bi_out_aqg,
            bi_wx_econ,
            bi_wx_oa,
            msi_aqi,
            msi_dayl,
            msi_wsev,
            msi_comf,
            msi_dom,
            msi_wxsev,
            msi_heat,
            msi_cold,
        ):
            app.add_object(o)
        self._ai_weather_dew_spread = ai_wx_spread
        self._ai_weather_enthalpy = ai_wx_enthalpy
        self._bi_weather_condensation_risk = bi_wx_cond
        self._bi_weather_freeze_risk = bi_wx_frz
        self._bi_weather_frost_risk = bi_wx_frost
        self._bi_weather_solar_available = bi_wx_solar
        self._bi_weather_precipitation_active = bi_wx_precip
        self._bi_weather_snow_active = bi_wx_snow
        self._bi_weather_high_wind = bi_wx_hw
        self._bi_outdoor_smoke_risk = bi_out_smoke
        self._bi_outdoor_air_quality_good = bi_out_aqg
        self._bi_weather_economizer_available = bi_wx_econ
        self._bi_weather_outdoor_air_usable = bi_wx_oa
        self._msi_outdoor_aqi_category = msi_aqi
        self._msi_weather_daylight_level = msi_dayl
        self._msi_weather_wind_severity = msi_wsev
        self._msi_weather_comfort_level = msi_comf
        self._msi_outdoor_dominant_pollutant = msi_dom
        self._msi_weather_severity = msi_wxsev
        self._msi_outdoor_heat_stress = msi_heat
        self._msi_outdoor_cold_stress = msi_cold
        (
            ai_aq_co2,
            ai_aq_pm25,
            ai_aq_pm10,
            ai_aq_co,
            ai_aq_no2,
            ai_aq_so2,
            ai_aq_o3,
            ai_aq_aod,
            ai_aq_uv,
            bi_aq_ok,
            csv_aq,
        ) = _create_air_quality_objects()
        for o in (
            ai_aq_co2,
            ai_aq_pm25,
            ai_aq_pm10,
            ai_aq_co,
            ai_aq_no2,
            ai_aq_so2,
            ai_aq_o3,
            ai_aq_aod,
            ai_aq_uv,
            bi_aq_ok,
            csv_aq,
        ):
            app.add_object(o)
        self._ai_aq_co2 = ai_aq_co2
        self._ai_aq_pm25 = ai_aq_pm25
        self._ai_aq_pm10 = ai_aq_pm10
        self._ai_aq_co = ai_aq_co
        self._ai_aq_no2 = ai_aq_no2
        self._ai_aq_so2 = ai_aq_so2
        self._ai_aq_o3 = ai_aq_o3
        self._ai_aq_aod = ai_aq_aod
        self._ai_aq_uv = ai_aq_uv
        self._bi_aq_ok = bi_aq_ok
        self._csv_aq_last = csv_aq
        (
            csv_site_dt,
            csv_site_tz,
            csv_site_date,
            csv_site_time,
            bi_site_ok,
            bi_site_dst,
            ai_site_y,
            ai_site_mo,
            ai_site_d,
            ai_site_h,
            ai_site_mi,
            ai_site_s,
            ai_site_wd,
            msi_site_wd,
            ai_site_off,
        ) = _create_site_time_objects()
        for o in (
            csv_site_dt,
            csv_site_tz,
            csv_site_date,
            csv_site_time,
            bi_site_ok,
            bi_site_dst,
            ai_site_y,
            ai_site_mo,
            ai_site_d,
            ai_site_h,
            ai_site_mi,
            ai_site_s,
            ai_site_wd,
            msi_site_wd,
            ai_site_off,
        ):
            app.add_object(o)
        self._csv_site_local_dt = csv_site_dt
        self._csv_site_tz = csv_site_tz
        self._csv_site_date = csv_site_date
        self._csv_site_time = csv_site_time
        self._bi_site_time_ok = bi_site_ok
        self._bi_site_dst = bi_site_dst
        self._ai_site_year = ai_site_y
        self._ai_site_month = ai_site_mo
        self._ai_site_day = ai_site_d
        self._ai_site_hour = ai_site_h
        self._ai_site_minute = ai_site_mi
        self._ai_site_second = ai_site_s
        self._ai_site_weekday = ai_site_wd
        self._msi_site_weekday = msi_site_wd
        self._ai_site_utc_offset_min = ai_site_off
        (
            bi_ht,
            bi_bd,
            bi_lw,
            bi_dw,
            bi_hok,
            bi_sok,
            csv_hn,
            csv_sr,
            csv_ss,
        ) = _create_schedule_context_objects()
        for o in (bi_ht, bi_bd, bi_lw, bi_dw, bi_hok, bi_sok, csv_hn, csv_sr, csv_ss):
            app.add_object(o)
        self._bi_holiday_today = bi_ht
        self._bi_business_day = bi_bd
        self._bi_long_weekend = bi_lw
        self._bi_daylight_window = bi_dw
        self._bi_holiday_api_ok = bi_hok
        self._bi_sun_data_ok = bi_sok
        self._csv_holiday_name = csv_hn
        self._csv_site_sunrise = csv_sr
        self._csv_site_sunset = csv_ss
        (
            ai_apoll,
            ai_ahb,
            ai_acfg,
            ai_aedge,
            ai_awho,
            ai_armax,
            ai_artmo,
            ai_alat,
            ai_alon,
            ai_awxp,
            ai_ast,
            ai_asch,
            ai_athr,
            bi_amaster,
            bi_afahr,
            bi_apdesired,
            csv_acc,
        ) = _create_agent_config_snapshot_objects()
        for o in (
            ai_apoll,
            ai_ahb,
            ai_acfg,
            ai_aedge,
            ai_awho,
            ai_armax,
            ai_artmo,
            ai_alat,
            ai_alon,
            ai_awxp,
            ai_ast,
            ai_asch,
            ai_athr,
            bi_amaster,
            bi_afahr,
            bi_apdesired,
            csv_acc,
        ):
            app.add_object(o)
        self._ai_agent_poll_interval = ai_apoll
        self._ai_agent_heartbeat_interval = ai_ahb
        self._ai_agent_config_poll_interval = ai_acfg
        self._ai_agent_edge_status_interval = ai_aedge
        self._ai_agent_who_is_timeout = ai_awho
        self._ai_agent_read_device_max = ai_armax
        self._ai_agent_read_device_timeout = ai_artmo
        self._ai_agent_weather_latitude = ai_alat
        self._ai_agent_weather_longitude = ai_alon
        self._ai_agent_weather_poll_interval = ai_awxp
        self._ai_agent_site_time_poll_interval = ai_ast
        self._ai_agent_schedule_context_poll_interval = ai_asch
        self._ai_agent_saas_online_threshold = ai_athr
        self._bi_agent_weather_master_active = bi_amaster
        self._bi_agent_weather_display_fahrenheit = bi_afahr
        self._bi_agent_weather_polling_desired = bi_apdesired
        self._csv_agent_site_country_code = csv_acc
        self.update_agent_config_snapshot()
        self._iam_response_effective = self._effective.iam_response_mode
        _patch_whois_iam_response(app, self._iam_response_effective)
        return app

    def update_edge_status_binary_inputs(self, internet_ok: bool, saas_ok: bool) -> None:
        """Update present-value for Edge-Internet and Edge-SaaS binary-input objects."""
        if self._bi_internet is None or self._bi_saas is None:
            return
        self._bi_internet.presentValue = BinaryPV.active if internet_ok else BinaryPV.inactive
        self._bi_saas.presentValue = BinaryPV.active if saas_ok else BinaryPV.inactive

    def update_agent_config_snapshot(self) -> None:
        """Refresh Agent-* analog/binary/CSV points to match effective SaaS tuning + env defaults."""
        if self._ai_agent_poll_interval is None:
            return
        s = self._settings
        tuning = self._storage.get_remote_agent_tuning()
        self._ai_agent_poll_interval.presentValue = Real(
            apply_float_tuning(s.poll_interval_seconds, tuning, "poll_interval_seconds", 1.0, 120.0)
        )
        self._ai_agent_heartbeat_interval.presentValue = Real(
            apply_float_tuning(
                s.heartbeat_interval_seconds, tuning, "heartbeat_interval_seconds", 10.0, 600.0
            )
        )
        self._ai_agent_config_poll_interval.presentValue = Real(
            apply_float_tuning(
                s.config_poll_interval_seconds, tuning, "config_poll_interval_seconds", 15.0, 3600.0
            )
        )
        self._ai_agent_edge_status_interval.presentValue = Real(
            apply_float_tuning(
                s.edge_status_check_interval_seconds,
                tuning,
                "edge_status_check_interval_seconds",
                5.0,
                600.0,
            )
        )
        self._ai_agent_who_is_timeout.presentValue = Real(
            apply_float_tuning(s.who_is_timeout_seconds, tuning, "who_is_timeout_seconds", 1.0, 120.0)
        )
        self._ai_agent_read_device_max.presentValue = Real(
            float(
                apply_int_tuning(
                    s.read_device_live_max_objects,
                    tuning,
                    "read_device_live_max_objects",
                    1,
                    10000,
                )
            )
        )
        self._ai_agent_read_device_timeout.presentValue = Real(
            apply_float_tuning(
                s.read_device_live_timeout_seconds,
                tuning,
                "read_device_live_timeout_seconds",
                10.0,
                600.0,
            )
        )
        lat = 0.0
        lon = 0.0
        if tuning is not None:
            if tuning.weather_latitude is not None:
                lat = float(tuning.weather_latitude)
            if tuning.weather_longitude is not None:
                lon = float(tuning.weather_longitude)
        self._ai_agent_weather_latitude.presentValue = Real(lat)
        self._ai_agent_weather_longitude.presentValue = Real(lon)
        self._ai_agent_weather_poll_interval.presentValue = Real(
            apply_float_tuning(
                s.weather_poll_interval_seconds,
                tuning,
                "weather_poll_interval_seconds",
                900.0,
                3600.0,
            )
        )
        self._ai_agent_site_time_poll_interval.presentValue = Real(
            apply_float_tuning(
                s.site_time_poll_interval_seconds,
                tuning,
                "site_time_poll_interval_seconds",
                1.0,
                3600.0,
            )
        )
        self._ai_agent_schedule_context_poll_interval.presentValue = Real(
            apply_float_tuning(
                s.schedule_context_poll_interval_seconds,
                tuning,
                "schedule_context_poll_interval_seconds",
                30.0,
                3600.0,
            )
        )
        self._ai_agent_saas_online_threshold.presentValue = Real(float(s.saas_online_threshold_seconds))
        if self._bi_agent_weather_master_active is not None:
            self._bi_agent_weather_master_active.presentValue = (
                BinaryPV.active if remote_weather_master_enabled(tuning) else BinaryPV.inactive
            )
        if self._bi_agent_weather_display_fahrenheit is not None:
            self._bi_agent_weather_display_fahrenheit.presentValue = (
                BinaryPV.active if use_fahrenheit_from_tuning(tuning) else BinaryPV.inactive
            )
        if self._bi_agent_weather_polling_desired is not None:
            self._bi_agent_weather_polling_desired.presentValue = (
                BinaryPV.active
                if desired_weather_polling_enabled_from_tuning(tuning)
                else BinaryPV.inactive
            )
        if self._csv_agent_site_country_code is not None:
            cc = ""
            if tuning is not None and tuning.site_country_code:
                cc = str(tuning.site_country_code).strip().upper()
            self._csv_agent_site_country_code.presentValue = CharacterString(
                _truncate_csv_text(cc, 8)
            )

    def update_agent_uptime_seconds(self, uptime_seconds: float) -> None:
        """Analog-input Edge-Uptime: present value in seconds."""
        if self._ai_uptime is None:
            return
        self._ai_uptime.presentValue = Real(float(uptime_seconds))

    def set_agent_identity_csv(self, hostname: str, box_id: str, saas_base_url: str) -> None:
        """Character-string values: hostname, box id, SaaS base URL."""
        if self._csv_hostname is None or self._csv_box_id is None or self._csv_saas_base is None:
            return
        self._csv_hostname.presentValue = CharacterString(_truncate_csv_text(hostname, 256))
        self._csv_box_id.presentValue = CharacterString(_truncate_csv_text(box_id, 256))
        self._csv_saas_base.presentValue = CharacterString(_truncate_csv_text(saas_base_url, 400))

    def set_last_job_running(self, job_id: str, job_type: str) -> None:
        """Multi-state = Running; CSV = short running description."""
        if self._msi_last_job is None or self._csv_last_job is None:
            return
        self._msi_last_job.presentValue = Unsigned(_JOB_MSI_RUNNING)
        text = _truncate_csv_text(f"job_id={job_id} type={job_type} status=running")
        self._csv_last_job.presentValue = CharacterString(text)

    def set_last_job_finished(self, envelope: JobResultEnvelope) -> None:
        """Multi-state = outcome; CSV = id, status, summary."""
        if self._msi_last_job is None or self._csv_last_job is None:
            return
        if envelope.status == "success":
            pv = _JOB_MSI_SUCCESS
        elif envelope.status == "partial_success":
            pv = _JOB_MSI_PARTIAL
        else:
            pv = _JOB_MSI_FAILED
        self._msi_last_job.presentValue = Unsigned(pv)
        text = _truncate_csv_text(
            f"job_id={envelope.job_id} status={envelope.status} summary={envelope.summary or ''}"
        )
        self._csv_last_job.presentValue = CharacterString(text)

    def set_weather_polling_enabled_from_config(self, tuning: Optional[RemoteAgentTuning]) -> None:
        """Apply SaaS desired Weather-Polling-Enabled BV (clears priority array so value takes effect)."""
        if self._bv_weather_polling is None:
            return
        en = desired_weather_polling_enabled_from_tuning(tuning)
        pv = BinaryPV.active if en else BinaryPV.inactive
        for i in range(16):
            self._bv_weather_polling.priorityArray[i] = PriorityValue(null=())
        self._bv_weather_polling.relinquishDefault = pv
        self._bv_weather_polling.presentValue = pv

    def is_weather_polling_bv_active(self) -> bool:
        if self._bv_weather_polling is None:
            return False
        return self._bv_weather_polling.presentValue == BinaryPV.active

    def update_weather(self, result: OpenMeteoResult, use_fahrenheit: bool) -> None:
        """Update forecast weather points; on failure keep last analog values and Weather-IsDay."""
        if (
            self._ai_weather_temp is None
            or self._ai_weather_rh is None
            or self._ai_weather_wind is None
            or self._ai_weather_precip is None
            or self._ai_weather_apparent is None
            or self._ai_weather_rain is None
            or self._ai_weather_showers is None
            or self._ai_weather_snow is None
            or self._ai_weather_code is None
            or self._ai_weather_cloud is None
            or self._ai_weather_pmsl is None
            or self._ai_weather_psurf is None
            or self._ai_weather_wdir is None
            or self._ai_weather_wgust is None
            or self._bi_weather_ok is None
            or self._bi_weather_unit_of_measure is None
            or self._bi_weather_is_day is None
            or self._csv_weather_last is None
            or self._ai_weather_dew is None
            or self._ai_weather_heat_index is None
            or self._ai_weather_wind_chill is None
            or self._csv_weather_code_text is None
        ):
            return
        imperial_bundle = use_fahrenheit
        self._bi_weather_unit_of_measure.presentValue = (
            BinaryPV.active if use_fahrenheit else BinaryPV.inactive
        )
        if result.fetch_ok:
            t_c = result.temperature_c
            t_disp = (t_c * 9.0 / 5.0 + 32.0) if use_fahrenheit else t_c
            at_c = result.apparent_temperature_c
            at_disp = (at_c * 9.0 / 5.0 + 32.0) if use_fahrenheit else at_c
            self._ai_weather_temp.presentValue = Real(float(t_disp))
            self._ai_weather_apparent.presentValue = Real(float(at_disp))
            self._ai_weather_rh.presentValue = Real(float(result.humidity_percent))
            self._ai_weather_wind.presentValue = Real(float(result.wind_speed))
            self._ai_weather_wgust.presentValue = Real(float(result.wind_gust))
            self._ai_weather_precip.presentValue = Real(float(result.precipitation))
            self._ai_weather_rain.presentValue = Real(float(result.rain))
            self._ai_weather_showers.presentValue = Real(float(result.showers))
            self._ai_weather_snow.presentValue = Real(float(result.snowfall))
            self._ai_weather_code.presentValue = Real(float(result.weather_code))
            self._ai_weather_cloud.presentValue = Real(float(result.cloud_cover_percent))
            self._ai_weather_wdir.presentValue = Real(float(result.wind_direction_deg))
            if use_fahrenheit:
                self._ai_weather_pmsl.presentValue = Real(
                    float(result.pressure_msl_hpa) * _HPA_TO_INHG
                )
                self._ai_weather_psurf.presentValue = Real(
                    float(result.surface_pressure_hpa) * _HPA_TO_INHG
                )
            else:
                self._ai_weather_pmsl.presentValue = Real(float(result.pressure_msl_hpa))
                self._ai_weather_psurf.presentValue = Real(float(result.surface_pressure_hpa))
            self._bi_weather_is_day.presentValue = (
                BinaryPV.active if result.is_day else BinaryPV.inactive
            )
            self._bi_weather_ok.presentValue = BinaryPV.active
            dew_c = dew_point_celsius(t_c, result.humidity_percent)
            dew_disp = (dew_c * 9.0 / 5.0 + 32.0) if use_fahrenheit else dew_c
            self._ai_weather_dew.presentValue = Real(float(dew_disp))
            self._ai_weather_heat_index.presentValue = Real(
                float(heat_index_display(t_c, result.humidity_percent, use_fahrenheit))
            )
            self._ai_weather_wind_chill.presentValue = Real(
                float(
                    wind_chill_display(
                        t_c,
                        float(result.wind_speed),
                        imperial_bundle=imperial_bundle,
                        use_fahrenheit=use_fahrenheit,
                    )
                )
            )
            self._csv_weather_code_text.presentValue = CharacterString(
                _truncate_csv_text(
                    wmo_weather_code_text(int(result.weather_code)),
                    128,
                )
            )
            self._csv_weather_last.presentValue = CharacterString(
                _truncate_csv_text(
                    f"ok code={result.weather_code} t_c={t_c:.2f} rh={result.humidity_percent:.1f}"
                )
            )
        else:
            self._bi_weather_ok.presentValue = BinaryPV.inactive
            err = (result.error or "fetch_failed").strip() or "fetch_failed"
            self._csv_weather_last.presentValue = CharacterString(_truncate_csv_text(f"err {err}"))

    def update_air_quality(self, result: OpenMeteoAirQualityResult) -> None:
        """Update air-quality analog inputs; on failure keep last analog values (like weather)."""
        if (
            self._ai_aq_co2 is None
            or self._ai_aq_pm25 is None
            or self._ai_aq_pm10 is None
            or self._ai_aq_co is None
            or self._ai_aq_no2 is None
            or self._ai_aq_so2 is None
            or self._ai_aq_o3 is None
            or self._ai_aq_aod is None
            or self._ai_aq_uv is None
            or self._bi_aq_ok is None
            or self._csv_aq_last is None
        ):
            return
        if result.fetch_ok:
            self._ai_aq_co2.presentValue = Real(float(result.carbon_dioxide_ppm))
            self._ai_aq_pm25.presentValue = Real(float(result.pm2_5_ugm3))
            self._ai_aq_pm10.presentValue = Real(float(result.pm10_ugm3))
            self._ai_aq_co.presentValue = Real(float(result.carbon_monoxide_ugm3))
            self._ai_aq_no2.presentValue = Real(float(result.nitrogen_dioxide_ugm3))
            self._ai_aq_so2.presentValue = Real(float(result.sulphur_dioxide_ugm3))
            self._ai_aq_o3.presentValue = Real(float(result.ozone_ugm3))
            self._ai_aq_aod.presentValue = Real(float(result.aerosol_optical_depth))
            self._ai_aq_uv.presentValue = Real(float(result.uv_index))
            self._bi_aq_ok.presentValue = BinaryPV.active
            self._csv_aq_last.presentValue = CharacterString(
                _truncate_csv_text(
                    f"ok co2={result.carbon_dioxide_ppm:.1f}pm pm2.5={result.pm2_5_ugm3:.1f}"
                )
            )
        else:
            self._bi_aq_ok.presentValue = BinaryPV.inactive
            err = (result.error or "fetch_failed").strip() or "fetch_failed"
            self._csv_aq_last.presentValue = CharacterString(_truncate_csv_text(f"err {err}"))

    def update_outdoor_decision_points(
        self,
        wx: OpenMeteoResult,
        aq: OpenMeteoAirQualityResult,
        use_fahrenheit: bool,
    ) -> None:
        """Decision BI/MSI and extra AIs from latest fetch results (preserves last on partial failure)."""
        if (
            self._ai_weather_dew_spread is None
            or self._ai_weather_enthalpy is None
            or self._bi_weather_condensation_risk is None
            or self._bi_weather_freeze_risk is None
            or self._bi_weather_frost_risk is None
            or self._bi_weather_solar_available is None
            or self._bi_weather_precipitation_active is None
            or self._bi_weather_snow_active is None
            or self._bi_weather_high_wind is None
            or self._bi_outdoor_smoke_risk is None
            or self._bi_outdoor_air_quality_good is None
            or self._bi_weather_economizer_available is None
            or self._bi_weather_outdoor_air_usable is None
            or self._msi_outdoor_aqi_category is None
            or self._msi_weather_daylight_level is None
            or self._msi_weather_wind_severity is None
            or self._msi_weather_comfort_level is None
            or self._msi_outdoor_dominant_pollutant is None
            or self._msi_weather_severity is None
            or self._msi_outdoor_heat_stress is None
            or self._msi_outdoor_cold_stress is None
        ):
            return
        dec = compute_outdoor_decisions(
            wx,
            aq,
            wx_ok=wx.fetch_ok,
            aq_ok=aq.fetch_ok,
            use_fahrenheit=use_fahrenheit,
            imperial_bundle=use_fahrenheit,
        )
        if dec.dew_spread is not None:
            self._ai_weather_dew_spread.presentValue = Real(float(dec.dew_spread))
        if dec.enthalpy is not None:
            self._ai_weather_enthalpy.presentValue = Real(float(dec.enthalpy))
        if dec.bi_condensation is not None:
            self._bi_weather_condensation_risk.presentValue = (
                BinaryPV.active if dec.bi_condensation else BinaryPV.inactive
            )
        if dec.bi_freeze is not None:
            self._bi_weather_freeze_risk.presentValue = (
                BinaryPV.active if dec.bi_freeze else BinaryPV.inactive
            )
        if dec.bi_frost is not None:
            self._bi_weather_frost_risk.presentValue = (
                BinaryPV.active if dec.bi_frost else BinaryPV.inactive
            )
        if dec.bi_solar is not None:
            self._bi_weather_solar_available.presentValue = (
                BinaryPV.active if dec.bi_solar else BinaryPV.inactive
            )
        if dec.bi_precip is not None:
            self._bi_weather_precipitation_active.presentValue = (
                BinaryPV.active if dec.bi_precip else BinaryPV.inactive
            )
        if dec.bi_snow is not None:
            self._bi_weather_snow_active.presentValue = BinaryPV.active if dec.bi_snow else BinaryPV.inactive
        if dec.bi_high_wind is not None:
            self._bi_weather_high_wind.presentValue = BinaryPV.active if dec.bi_high_wind else BinaryPV.inactive
        if dec.bi_smoke is not None:
            self._bi_outdoor_smoke_risk.presentValue = BinaryPV.active if dec.bi_smoke else BinaryPV.inactive
        if dec.bi_aq_good is not None:
            self._bi_outdoor_air_quality_good.presentValue = (
                BinaryPV.active if dec.bi_aq_good else BinaryPV.inactive
            )
        if dec.bi_econo is not None:
            self._bi_weather_economizer_available.presentValue = (
                BinaryPV.active if dec.bi_econo else BinaryPV.inactive
            )
        if dec.bi_oa_usable is not None:
            self._bi_weather_outdoor_air_usable.presentValue = (
                BinaryPV.active if dec.bi_oa_usable else BinaryPV.inactive
            )
        if dec.msi_aqi is not None:
            self._msi_outdoor_aqi_category.presentValue = Unsigned(int(dec.msi_aqi))
        if dec.msi_daylight is not None:
            self._msi_weather_daylight_level.presentValue = Unsigned(int(dec.msi_daylight))
        if dec.msi_wind_sev is not None:
            self._msi_weather_wind_severity.presentValue = Unsigned(int(dec.msi_wind_sev))
        if dec.msi_comfort is not None:
            self._msi_weather_comfort_level.presentValue = Unsigned(int(dec.msi_comfort))
        if dec.msi_dominant is not None:
            self._msi_outdoor_dominant_pollutant.presentValue = Unsigned(int(dec.msi_dominant))
        if dec.msi_weather_sev is not None:
            self._msi_weather_severity.presentValue = Unsigned(int(dec.msi_weather_sev))
        if dec.msi_heat is not None:
            self._msi_outdoor_heat_stress.presentValue = Unsigned(int(dec.msi_heat))
        if dec.msi_cold is not None:
            self._msi_outdoor_cold_stress.presentValue = Unsigned(int(dec.msi_cold))

    def update_site_time(self, info: SiteLocalTimeInfo) -> None:
        """Update site-local time BACnet points; on failure only clears OK/DST (preserves last strings/values)."""
        if (
            self._csv_site_local_dt is None
            or self._csv_site_tz is None
            or self._csv_site_date is None
            or self._csv_site_time is None
            or self._bi_site_time_ok is None
            or self._bi_site_dst is None
            or self._ai_site_year is None
            or self._ai_site_month is None
            or self._ai_site_day is None
            or self._ai_site_hour is None
            or self._ai_site_minute is None
            or self._ai_site_second is None
            or self._ai_site_weekday is None
            or self._msi_site_weekday is None
            or self._ai_site_utc_offset_min is None
        ):
            return
        if not info.ok:
            _set_binary_if_changed(self._bi_site_time_ok, BinaryPV.inactive)
            _set_binary_if_changed(self._bi_site_dst, BinaryPV.inactive)
            return
        _set_binary_if_changed(self._bi_site_time_ok, BinaryPV.active)
        _set_binary_if_changed(
            self._bi_site_dst,
            BinaryPV.active if info.is_dst else BinaryPV.inactive,
        )
        tz_label = (info.timezone_name or "").strip() or "unknown"
        _set_character_string_if_changed(self._csv_site_tz, tz_label, 256)
        _set_character_string_if_changed(self._csv_site_local_dt, info.local_datetime_iso, 400)
        _set_character_string_if_changed(self._csv_site_date, info.local_date_iso, 32)
        _set_character_string_if_changed(self._csv_site_time, info.local_time_iso, 32)
        _set_real_if_changed(self._ai_site_year, float(info.year))
        _set_real_if_changed(self._ai_site_month, float(info.month))
        _set_real_if_changed(self._ai_site_day, float(info.day))
        _set_real_if_changed(self._ai_site_hour, float(info.hour))
        _set_real_if_changed(self._ai_site_minute, float(info.minute))
        _set_real_if_changed(self._ai_site_second, float(info.second))
        _set_real_if_changed(self._ai_site_weekday, float(info.weekday_number))
        _set_multistate_if_changed(self._msi_site_weekday, info.weekday_number)
        _set_real_if_changed(self._ai_site_utc_offset_min, float(info.utc_offset_minutes))

    def update_schedule_context(
        self,
        info: SiteLocalTimeInfo,
        holiday: HolidayEval,
        sun: SunTimesResult,
    ) -> None:
        """Holiday + sun BACnet points; preserves CSV strings on sun failure."""
        if (
            self._bi_holiday_today is None
            or self._bi_business_day is None
            or self._bi_long_weekend is None
            or self._bi_daylight_window is None
            or self._bi_holiday_api_ok is None
            or self._bi_sun_data_ok is None
            or self._csv_holiday_name is None
            or self._csv_site_sunrise is None
            or self._csv_site_sunset is None
        ):
            return
        if not info.ok:
            self._bi_holiday_api_ok.presentValue = BinaryPV.inactive
            self._bi_sun_data_ok.presentValue = BinaryPV.inactive
            return

        self._bi_holiday_today.presentValue = (
            BinaryPV.active if holiday.holiday_today else BinaryPV.inactive
        )
        self._bi_business_day.presentValue = BinaryPV.active if holiday.business_day else BinaryPV.inactive
        self._bi_long_weekend.presentValue = BinaryPV.active if holiday.long_weekend else BinaryPV.inactive
        self._bi_holiday_api_ok.presentValue = (
            BinaryPV.active if holiday.holiday_api_ok else BinaryPV.inactive
        )
        self._csv_holiday_name.presentValue = CharacterString(
            _truncate_csv_text(holiday.holiday_name, 256)
        )

        if sun.fetch_ok and sun.sunrise_display.strip() and sun.sunset_display.strip():
            self._csv_site_sunrise.presentValue = CharacterString(
                _truncate_csv_text(sun.sunrise_display, 256)
            )
            self._csv_site_sunset.presentValue = CharacterString(
                _truncate_csv_text(sun.sunset_display, 256)
            )
            self._bi_sun_data_ok.presentValue = BinaryPV.active
            if info.local_datetime is not None:
                dw = daylight_window_active(
                    info.local_datetime,
                    sun.sunrise_display,
                    sun.sunset_display,
                )
                self._bi_daylight_window.presentValue = BinaryPV.active if dw else BinaryPV.inactive
            else:
                self._bi_daylight_window.presentValue = BinaryPV.inactive
        else:
            self._bi_sun_data_ok.presentValue = BinaryPV.inactive
            self._bi_daylight_window.presentValue = BinaryPV.inactive

    async def start(self) -> None:
        if self._app is not None:
            return
        self._app = self._build_application()
        addr_log = (
            format_bacpypes_device_address(
                self._effective.bind_ip,
                self._effective.bind_prefix,
                self._effective.udp_port,
            )
            if self._effective.bind_ip.strip()
            else "(default host)"
        )
        _log.info(
            "bacnet_stack_started name=%s device_instance=%s address=%s iam_response_mode=%s",
            self._effective.device_name,
            self._effective.device_instance,
            addr_log,
            self._iam_response_effective,
        )

    async def stop(self) -> None:
        if self._app is not None:
            self._app.close()
            self._app = None
            _log.info("bacnet_stack_stopped")
        self._bi_internet = None
        self._bi_saas = None
        self._ai_uptime = None
        self._csv_hostname = None
        self._csv_box_id = None
        self._csv_saas_base = None
        self._csv_last_job = None
        self._msi_last_job = None
        self._ai_weather_temp = None
        self._ai_weather_rh = None
        self._ai_weather_wind = None
        self._ai_weather_precip = None
        self._ai_weather_apparent = None
        self._ai_weather_rain = None
        self._ai_weather_showers = None
        self._ai_weather_snow = None
        self._ai_weather_code = None
        self._ai_weather_cloud = None
        self._ai_weather_pmsl = None
        self._ai_weather_psurf = None
        self._ai_weather_wdir = None
        self._ai_weather_wgust = None
        self._bi_weather_ok = None
        self._bi_weather_unit_of_measure = None
        self._bi_weather_is_day = None
        self._bv_weather_polling = None
        self._csv_weather_last = None
        self._ai_weather_dew = None
        self._ai_weather_heat_index = None
        self._ai_weather_wind_chill = None
        self._csv_weather_code_text = None
        self._ai_weather_dew_spread = None
        self._ai_weather_enthalpy = None
        self._bi_weather_condensation_risk = None
        self._bi_weather_freeze_risk = None
        self._bi_weather_frost_risk = None
        self._bi_weather_solar_available = None
        self._bi_weather_precipitation_active = None
        self._bi_weather_snow_active = None
        self._bi_weather_high_wind = None
        self._bi_outdoor_smoke_risk = None
        self._bi_outdoor_air_quality_good = None
        self._bi_weather_economizer_available = None
        self._bi_weather_outdoor_air_usable = None
        self._msi_outdoor_aqi_category = None
        self._msi_weather_daylight_level = None
        self._msi_weather_wind_severity = None
        self._msi_weather_comfort_level = None
        self._msi_outdoor_dominant_pollutant = None
        self._msi_weather_severity = None
        self._msi_outdoor_heat_stress = None
        self._msi_outdoor_cold_stress = None
        self._ai_aq_co2 = None
        self._ai_aq_pm25 = None
        self._ai_aq_pm10 = None
        self._ai_aq_co = None
        self._ai_aq_no2 = None
        self._ai_aq_so2 = None
        self._ai_aq_o3 = None
        self._ai_aq_aod = None
        self._ai_aq_uv = None
        self._bi_aq_ok = None
        self._csv_aq_last = None
        self._csv_site_local_dt = None
        self._csv_site_tz = None
        self._csv_site_date = None
        self._csv_site_time = None
        self._bi_site_time_ok = None
        self._bi_site_dst = None
        self._ai_site_year = None
        self._ai_site_month = None
        self._ai_site_day = None
        self._ai_site_hour = None
        self._ai_site_minute = None
        self._ai_site_second = None
        self._ai_site_weekday = None
        self._msi_site_weekday = None
        self._ai_site_utc_offset_min = None
        self._bi_holiday_today = None
        self._bi_business_day = None
        self._bi_long_weekend = None
        self._bi_daylight_window = None
        self._bi_holiday_api_ok = None
        self._bi_sun_data_ok = None
        self._csv_holiday_name = None
        self._csv_site_sunrise = None
        self._csv_site_sunset = None
        self._ai_agent_poll_interval = None
        self._ai_agent_heartbeat_interval = None
        self._ai_agent_config_poll_interval = None
        self._ai_agent_edge_status_interval = None
        self._ai_agent_who_is_timeout = None
        self._ai_agent_read_device_max = None
        self._ai_agent_read_device_timeout = None
        self._ai_agent_weather_latitude = None
        self._ai_agent_weather_longitude = None
        self._ai_agent_weather_poll_interval = None
        self._ai_agent_site_time_poll_interval = None
        self._ai_agent_schedule_context_poll_interval = None
        self._ai_agent_saas_online_threshold = None
        self._bi_agent_weather_master_active = None
        self._bi_agent_weather_display_fahrenheit = None
        self._bi_agent_weather_polling_desired = None
        self._csv_agent_site_country_code = None

    async def restart(self, effective: EffectiveBacnetConfig) -> None:
        await self.stop()
        self._effective = effective
        await self.start()

    def _require_app(self) -> Application:
        if not self._app:
            raise RuntimeError("BACnet stack not started")
        return self._app

    async def discover_network(self, who_is_timeout: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        app = self._require_app()
        errors: list[dict[str, Any]] = []
        devices: list[dict[str, Any]] = []
        now = utc_now_iso()
        try:
            fut = app.who_is(0, 4194303, timeout=who_is_timeout)
            i_ams = await asyncio.wait_for(fut, timeout=who_is_timeout + 2.0)
        except ErrorRejectAbortNack as e:
            errors.append(
                {
                    "message": f"who_is failed: {failure_message(e, default='rejected')}",
                }
            )
            return devices, errors
        except Exception as e:
            errors.append(
                {
                    "message": f"who_is failed: {failure_message(e, default='failed')}",
                }
            )
            return devices, errors

        for i_am in i_ams:
            try:
                di = i_am.iAmDeviceIdentifier[1]
                seg = getattr(i_am.segmentationSupported, "name", None) or str(
                    i_am.segmentationSupported
                )
                devices.append(
                    {
                        "device_instance": di,
                        "address": str(i_am.pduSource),
                        "vendor_id": int(i_am.vendorID),
                        "max_apdu": int(i_am.maxAPDULengthAccepted),
                        "segmentation": seg,
                        "last_seen_at": now,
                    }
                )
            except Exception as e:
                errors.append(
                    {
                        "message": failure_message(e, default="i_am parse failed"),
                        "raw": "i_am_parse",
                    }
                )
        return devices, errors

    async def snapshot_network(self, who_is_timeout: float, read_timeout: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        devices, derr = await self.discover_network(who_is_timeout)
        errors: list[dict[str, Any]] = list(derr)
        out_devices: list[dict[str, Any]] = []
        app = self._require_app()

        for d in devices:
            di = int(d["device_instance"])
            addr = Address(d["address"])
            dev_obj_id = ObjectIdentifier(("device", di))
            out_entry: dict[str, Any] = dict(d)

            try:
                oids = await asyncio.wait_for(
                    _object_identifiers(app, addr, dev_obj_id),
                    timeout=read_timeout,
                )
            except ErrorRejectAbortNack as e:
                errors.append(
                    {
                        "device_instance": di,
                        "message": f"object-list: {failure_message(e, default='rejected')}",
                    }
                )
                continue
            except Exception as e:
                errors.append(
                    {
                        "device_instance": di,
                        "message": f"object-list: {failure_message(e, default='failed')}",
                    }
                )
                continue

            err_dev: dict[str, Any] = {"device_instance": di}
            dev_oname = await _snap_read_property(
                app, addr, dev_obj_id, "object-name", read_timeout, errors, err_dev
            )
            if dev_oname is not None:
                nm = str(dev_oname)
                out_entry["object_name"] = nm
                out_entry["name"] = nm
            for prop, key in (
                ("description", "description"),
                ("location", "location"),
                ("vendor-name", "vendor_name"),
                ("model-name", "model_name"),
                ("firmware-revision", "firmware_revision"),
                ("application-software-version", "application_software_version"),
                ("protocol-version", "protocol_version"),
            ):
                v = await _snap_read_property(
                    app, addr, dev_obj_id, prop, read_timeout, errors, err_dev
                )
                if v is not None:
                    out_entry[key] = v

            objects: list[dict[str, Any]] = []
            for oid in oids:
                if _is_device_object_type(oid[0]):
                    continue
                oi = int(oid[1])
                entry = await _build_snapshot_style_object_entry(
                    app,
                    addr,
                    di,
                    oid[0],
                    oi,
                    read_timeout,
                    errors,
                    read_oid=oid,
                )
                objects.append(entry)

            out_entry["objects"] = objects
            out_devices.append(out_entry)

        return {
            "snapshot_format_version": 2,
            "snapshot_at": utc_now_iso(),
            "devices": out_devices,
        }, errors

    async def read_device_live(
        self,
        device_instance: int,
        read_timeout: float,
        max_objects: int,
        deadline_monotonic: Optional[float] = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Read snapshot-shaped object rows for one device (Explorer live panel)."""
        app = self._require_app()
        errors: list[dict[str, Any]] = []
        read_at = utc_now_iso()
        empty_data: dict[str, Any] = {
            "device_instance": device_instance,
            "read_at": read_at,
            "objects": [],
        }

        i_ams_fut = app.who_is(
            device_instance, device_instance, timeout=self._settings.who_is_timeout_seconds
        )
        try:
            i_ams = await asyncio.wait_for(
                i_ams_fut,
                timeout=self._settings.who_is_timeout_seconds + 2.0,
            )
        except ErrorRejectAbortNack as e:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": f"who_is: {failure_message(e, default='rejected')}",
                }
            )
            return empty_data, errors
        except Exception as e:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": f"who_is: {failure_message(e, default='failed')}",
                }
            )
            return empty_data, errors

        if not i_ams:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": "device not found (I-Am)",
                }
            )
            return empty_data, errors

        addr = Address(i_ams[0].pduSource)
        dev_obj_id = ObjectIdentifier(("device", device_instance))

        try:
            oids = await asyncio.wait_for(
                _object_identifiers(app, addr, dev_obj_id),
                timeout=read_timeout,
            )
        except ErrorRejectAbortNack as e:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": f"object-list: {failure_message(e, default='rejected')}",
                }
            )
            return empty_data, errors
        except Exception as e:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": f"object-list: {failure_message(e, default='failed')}",
                }
            )
            return empty_data, errors

        if not oids:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": "object-list empty or unreadable",
                }
            )
            return empty_data, errors

        non_dev = [o for o in oids if not _is_device_object_type(o[0])]
        total_object_count = len(non_dev)
        if total_object_count == 0:
            errors.append(
                {
                    "device_instance": device_instance,
                    "message": "no non-device objects in object-list",
                }
            )
            return empty_data, errors

        if max_objects and max_objects > 0:
            to_process = non_dev[:max_objects]
        else:
            to_process = non_dev

        truncated_by_count = len(to_process) < total_object_count
        objects_out: list[dict[str, Any]] = []
        truncated_by_time = False

        for oid in to_process:
            if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
                truncated_by_time = True
                break
            oi = int(oid[1])
            entry = await _build_snapshot_style_object_entry(
                app,
                addr,
                device_instance,
                oid[0],
                oi,
                read_timeout,
                errors,
                read_oid=oid,
            )
            objects_out.append(entry)

        returned_object_count = len(objects_out)
        data: dict[str, Any] = {
            "device_instance": device_instance,
            "read_at": read_at,
            "objects": objects_out,
        }
        truncated = truncated_by_count or truncated_by_time
        if truncated:
            data["truncated"] = True
            data["total_object_count"] = total_object_count
            data["returned_object_count"] = returned_object_count

        return data, errors

    async def read_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        prop: str,
        read_timeout: float,
        array_index: Optional[int] = None,
    ) -> dict[str, Any]:
        app = self._require_app()
        arr_idx: Optional[int] = (
            int(array_index) if array_index is not None else None
        )
        i_ams_fut = app.who_is(device_instance, device_instance, timeout=self._settings.who_is_timeout_seconds)
        try:
            i_ams = await asyncio.wait_for(
                i_ams_fut,
                timeout=self._settings.who_is_timeout_seconds + 2.0,
            )
        except ErrorRejectAbortNack as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": failure_message(e, default="who-is failed"),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": failure_message(e, default="who-is exception"),
            }
        if not i_ams:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": "device not found (I-Am)",
            }
        addr = Address(i_ams[0].pduSource)
        ois = _object_id_string(object_type, object_instance)

        if _is_present_value_property(prop) and arr_idx is None:
            try:
                val = await asyncio.wait_for(
                    app.read_property(addr, ois, "present-value"),
                    timeout=read_timeout,
                )
                if isinstance(val, ErrorRejectAbortNack):
                    return {
                        "device_instance": device_instance,
                        "object_type": object_type,
                        "object_instance": object_instance,
                        "property": prop,
                        "error": failure_message(
                            val, default="read present-value rejected"
                        ),
                    }
            except ErrorRejectAbortNack as err:
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "error": failure_message(
                        err, default="read present-value rejected"
                    ),
                }
            except Exception as e:
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "error": failure_message(e, default="read present-value failed"),
                }

            enrich_errors: list[dict[str, Any]] = []
            entry = await _build_snapshot_style_object_entry(
                app,
                addr,
                device_instance,
                object_type,
                object_instance,
                read_timeout,
                enrich_errors,
                present_value_precooked=val,
            )
            read_ts = utc_now_iso()
            out: dict[str, Any] = dict(entry)
            out["device_instance"] = device_instance
            out["object_instance"] = object_instance
            out["property"] = prop
            out["present_value"] = entry.get("present_value")
            out["value"] = entry.get("present_value")
            out["read_at"] = read_ts
            out["datatype"] = type(val).__name__
            if enrich_errors:
                out["_property_errors"] = enrich_errors
            return out

        pid = (
            "present-value"
            if _is_present_value_property(prop)
            else _bacnet_property_identifier(str(prop))
        )
        if not pid:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "array_index": arr_idx,
                "error": "empty property id",
            }
        try:
            if pid == "priority-array" and arr_idx is None:
                pe: list[dict[str, Any]] = []
                pa_list = await _read_priority_array_for_snapshot(
                    app,
                    addr,
                    ois,
                    read_timeout,
                    pe,
                    {
                        "device_instance": device_instance,
                        "object_type": object_type,
                        "object_instance": object_instance,
                    },
                )
                safe = to_json_safe(pa_list)
                out_pa: dict[str, Any] = {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "bacnet_property": pid,
                    "value": safe,
                    "datatype": "list",
                    "read_at": utc_now_iso(),
                }
                if pe:
                    out_pa["_property_errors"] = pe
                return out_pa

            if arr_idx is not None:
                val = await asyncio.wait_for(
                    app.read_property(
                        addr, ois, pid, array_index=int(arr_idx)
                    ),
                    timeout=read_timeout,
                )
            else:
                val = await asyncio.wait_for(
                    app.read_property(addr, ois, pid),
                    timeout=read_timeout,
                )
            if isinstance(val, ErrorRejectAbortNack):
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": prop,
                    "bacnet_property": pid,
                    "array_index": arr_idx,
                    "error": failure_message(val, default="read property rejected"),
                }
            safe = to_json_safe(val)
            out: dict[str, Any] = {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "bacnet_property": pid,
                "value": safe,
                "datatype": type(val).__name__,
                "read_at": utc_now_iso(),
            }
            if arr_idx is not None:
                out["array_index"] = arr_idx
            return out
        except ErrorRejectAbortNack as err:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "bacnet_property": pid,
                "array_index": arr_idx,
                "error": failure_message(err, default="read property rejected"),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": prop,
                "bacnet_property": pid,
                "array_index": arr_idx,
                "error": failure_message(e, default="read property failed"),
            }

    async def _resolve_device_address(
        self, device_instance: int
    ) -> tuple[Optional[Address], Optional[str]]:
        app = self._require_app()
        i_ams_fut = app.who_is(
            device_instance, device_instance, timeout=self._settings.who_is_timeout_seconds
        )
        try:
            i_ams = await asyncio.wait_for(
                i_ams_fut,
                timeout=self._settings.who_is_timeout_seconds + 2.0,
            )
        except ErrorRejectAbortNack as e:
            return None, failure_message(
                e, default="who-is / address resolution rejected"
            )
        except Exception as e:
            return None, failure_message(
                e, default="who-is / address resolution failed"
            )
        if not i_ams:
            return None, "device not found (I-Am)"
        return Address(i_ams[0].pduSource), None

    async def _write_property_dispatch(
        self,
        app: Application,
        addr: Address,
        ois: str,
        pid: str,
        val: Any,
        write_timeout: float,
        priority: Optional[int],
        array_index: Optional[int],
    ) -> Union[Any, ErrorRejectAbortNack]:
        """Single BACnet WriteProperty; priority only for present-value; array_index for arrays."""
        val = _normalize_write_value_for_bacnet(pid, val, priority, array_index)
        if pid == "present-value":
            if array_index is not None and priority is None:
                raise ValueError(
                    "present-value uses BACnet priority (1-16), not array_index; "
                    "omit array_index, set priority for that slot, or use property "
                    "priority-array with array_index"
                )
            if priority is not None and array_index is not None:
                return await asyncio.wait_for(
                    app.write_property(
                        addr,
                        ois,
                        pid,
                        val,
                        priority=int(priority),
                        array_index=int(array_index),
                    ),
                    timeout=write_timeout,
                )
            if priority is not None:
                return await asyncio.wait_for(
                    app.write_property(
                        addr, ois, pid, val, priority=int(priority)
                    ),
                    timeout=write_timeout,
                )
        if array_index is not None:
            return await asyncio.wait_for(
                app.write_property(
                    addr, ois, pid, val, array_index=int(array_index)
                ),
                timeout=write_timeout,
            )
        return await asyncio.wait_for(
            app.write_property(addr, ois, pid, val),
            timeout=write_timeout,
        )

    async def write_point_multi(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        writes: list[dict[str, Any]],
        write_timeout: float,
        include_readback: bool = False,
        readback_properties: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """
        Apply multiple WriteProperty operations in order. Per-write ok/error in write_results.
        Manufacturers may reject some properties; use result status partial_success on SaaS.
        """
        addr, addr_err = await self._resolve_device_address(device_instance)
        if addr_err:
            return {
                "error": failure_message(
                    addr_err, default="device address resolution failed"
                ),
                "device_instance": device_instance,
                "write_results": [],
            }
        app = self._require_app()
        ois = _object_id_string(object_type, object_instance)
        write_results: list[dict[str, Any]] = []

        for i, spec in enumerate(writes):
            if not isinstance(spec, dict):
                write_results.append(
                    {
                        "index": i,
                        "property": None,
                        "bacnet_property": None,
                        "ok": False,
                        "error": "write entry must be an object",
                    }
                )
                continue
            prop_raw = spec.get("property")
            if prop_raw is None or str(prop_raw).strip() == "":
                write_results.append(
                    {
                        "index": i,
                        "property": None,
                        "bacnet_property": None,
                        "ok": False,
                        "error": "missing property",
                    }
                )
                continue
            if "value" not in spec:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": None,
                        "ok": False,
                        "error": "missing value (use null for BACnet null when applicable)",
                    }
                )
                continue

            pid = _bacnet_property_identifier(str(prop_raw))
            if not pid:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": None,
                        "ok": False,
                        "error": "empty property id",
                    }
                )
                continue

            val = spec["value"]
            pri = spec.get("priority")
            if pri is not None:
                pri = int(pri)
            arr_idx = spec.get("array_index")
            if arr_idx is not None:
                arr_idx = int(arr_idx)

            if pid == "present-value":
                if val is None and pri is None:
                    write_results.append(
                        {
                            "index": i,
                            "property": str(prop_raw),
                            "bacnet_property": pid,
                            "ok": False,
                            "error": (
                                "present-value null (relinquish) requires priority 1-16, "
                                "or use property priority-array with array_index and value null"
                            ),
                        }
                    )
                    continue
                if pri is not None and (pri < 1 or pri > 16):
                    write_results.append(
                        {
                            "index": i,
                            "property": str(prop_raw),
                            "bacnet_property": pid,
                            "ok": False,
                            "error": "priority must be 1-16 for present-value",
                        }
                    )
                    continue
            if pid == "priority-array" and arr_idx is None:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": "priority-array write requires array_index (1-16)",
                    }
                )
                continue
            if (
                pid == "priority-array"
                and arr_idx is not None
                and (arr_idx < 1 or arr_idx > 16)
            ):
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": "priority-array array_index must be 1-16",
                    }
                )
                continue

            try:
                resp = await self._write_property_dispatch(
                    app, addr, ois, pid, val, write_timeout, pri, arr_idx
                )
                if isinstance(resp, ErrorRejectAbortNack):
                    write_results.append(
                        {
                            "index": i,
                            "property": str(prop_raw),
                            "bacnet_property": pid,
                            "ok": False,
                            "error": failure_message(
                                resp, default="BACnet write rejected"
                            ),
                        }
                    )
                else:
                    write_results.append(
                        {
                            "index": i,
                            "property": str(prop_raw),
                            "bacnet_property": pid,
                            "ok": True,
                        }
                    )
            except ErrorRejectAbortNack as err:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": failure_message(err, default="BACnet write rejected"),
                    }
                )
            except Exception as e:
                write_results.append(
                    {
                        "index": i,
                        "property": str(prop_raw),
                        "bacnet_property": pid,
                        "ok": False,
                        "error": failure_message(e, default="write raised exception"),
                    }
                )

        for row in write_results:
            if row.get("ok") is True:
                continue
            row["error"] = failure_message(
                row.get("error"),
                default=f"write failed (index {row.get('index')})",
            )

        result: dict[str, Any] = {
            "device_instance": device_instance,
            "object_type": object_type,
            "object_instance": object_instance,
            "write_mode": "multi",
            "write_results": write_results,
        }

        props_to_read: Optional[list[str]] = None
        if include_readback:
            props_to_read = readback_properties if readback_properties else ["present-value"]

        if props_to_read:
            rb_at = utc_now_iso()
            rb_obj: dict[str, Any] = {}
            for rb in props_to_read:
                rpid = _bacnet_property_identifier(str(rb))
                jkey = _json_key_for_bacnet_property(rpid)
                try:
                    if rpid == "priority-array":
                        pe: list[dict[str, Any]] = []
                        pa_list = await asyncio.wait_for(
                            _read_priority_array_for_snapshot(
                                app,
                                addr,
                                ois,
                                write_timeout,
                                pe,
                                {
                                    "device_instance": device_instance,
                                    "object_type": object_type,
                                    "object_instance": object_instance,
                                },
                            ),
                            timeout=write_timeout + 2.0,
                        )
                        rb_obj[jkey] = to_json_safe(pa_list)
                        if pe:
                            rb_obj[f"{jkey}_errors"] = pe
                    else:
                        pv = await asyncio.wait_for(
                            app.read_property(addr, ois, rpid),
                            timeout=write_timeout,
                        )
                        if isinstance(pv, ErrorRejectAbortNack):
                            rb_obj[jkey] = None
                            rb_obj[f"{jkey}_error"] = failure_message(
                                pv, default="readback rejected"
                            )
                        else:
                            rb_obj[jkey] = to_json_safe(pv)
                except (ErrorRejectAbortNack, Exception) as e:
                    rb_obj[jkey] = None
                    rb_obj[f"{jkey}_error"] = failure_message(
                        e, default="readback failed"
                    )
            result["readback"] = rb_obj
            result["read_at"] = rb_at

        return result

    async def write_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        value: Any,
        priority: Optional[int],
        write_timeout: float,
        include_readback: bool = False,
    ) -> dict[str, Any]:
        addr, addr_err = await self._resolve_device_address(device_instance)
        if addr_err:
            return {
                "error": failure_message(
                    addr_err, default="device address resolution failed"
                )
            }
        app = self._require_app()
        ois = _object_id_string(object_type, object_instance)
        try:
            resp = await self._write_property_dispatch(
                app,
                addr,
                ois,
                "present-value",
                value,
                write_timeout,
                priority,
                None,
            )
            if isinstance(resp, ErrorRejectAbortNack):
                return {
                    "device_instance": device_instance,
                    "object_type": object_type,
                    "object_instance": object_instance,
                    "property": "presentValue",
                    "value": value,
                    "priority": priority,
                    "error": failure_message(resp, default="BACnet write rejected"),
                }
            result: dict[str, Any] = {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": "presentValue",
                "value": value,
                "priority": priority,
            }
            if include_readback:
                rb_at = utc_now_iso()
                try:
                    pv = await asyncio.wait_for(
                        app.read_property(addr, ois, "present-value"),
                        timeout=write_timeout,
                    )
                    if isinstance(pv, ErrorRejectAbortNack):
                        result["present_value_after"] = None
                    else:
                        result["present_value_after"] = to_json_safe(pv)
                except (ErrorRejectAbortNack, Exception):
                    result["present_value_after"] = None
                result["read_at"] = rb_at
            return result
        except ErrorRejectAbortNack as err:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": "presentValue",
                "value": value,
                "priority": priority,
                "error": failure_message(err, default="BACnet write rejected"),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": object_type,
                "object_instance": object_instance,
                "property": "presentValue",
                "value": value,
                "priority": priority,
                "error": failure_message(e, default="write raised exception"),
            }

    async def create_object(
        self,
        device_instance: int,
        object_type: str,
        object_instance: Optional[int],
        initial_properties: Optional[list[dict[str, Any]]],
        write_timeout: float,
    ) -> dict[str, Any]:
        addr, addr_err = await self._resolve_device_address(device_instance)
        if addr_err:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "error": failure_message(
                    addr_err, default="device address resolution failed"
                ),
            }
        app = self._require_app()
        vendor_info = await app.get_vendor_info(device_address=addr)

        spec = CreateObjectRequestObjectSpecifier()
        try:
            if object_instance is not None:
                oid = await app.parse_object_identifier(
                    _object_id_string(object_type, int(object_instance)),
                    vendor_info=vendor_info,
                )
                spec.objectIdentifier = oid
            else:
                oid_t = await app.parse_object_identifier(
                    _object_id_string(object_type, 1),
                    vendor_info=vendor_info,
                )
                spec.objectType = oid_t[0]
        except (TypeError, ValueError) as e:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "error": failure_message(e, default="invalid object type or instance"),
            }

        init_list = list(initial_properties) if initial_properties else []
        list_of_vals, build_err = await _list_of_initial_values_for_create_object(
            app, addr, vendor_info, object_type, init_list
        )
        if build_err:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "error": build_err,
            }

        req = CreateObjectRequest(objectSpecifier=spec, destination=addr)
        if list_of_vals is not None:
            req.listOfInitialValues = list_of_vals

        try:
            response = await asyncio.wait_for(
                app.request(req),
                timeout=write_timeout,
            )
        except ErrorRejectAbortNack as err:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "error": failure_message(err, default="CreateObject rejected"),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "error": failure_message(e, default="CreateObject failed"),
            }

        if isinstance(response, ErrorRejectAbortNack):
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "error": failure_message(response, default="CreateObject rejected"),
            }
        if not isinstance(response, CreateObjectACK):
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "error": "unexpected response to CreateObject",
            }

        created = response.objectIdentifier
        ot_label = _object_type_label(created[0])
        out: dict[str, Any] = {
            "device_instance": device_instance,
            "object_type": _object_type_for_json(ot_label),
            "object_instance": int(created[1]),
        }
        if object_instance is not None:
            out["requested_object_instance"] = int(object_instance)
        return out

    async def delete_object(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        write_timeout: float,
    ) -> dict[str, Any]:
        addr, addr_err = await self._resolve_device_address(device_instance)
        if addr_err:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "object_instance": object_instance,
                "error": failure_message(
                    addr_err, default="device address resolution failed"
                ),
            }
        app = self._require_app()
        vendor_info = await app.get_vendor_info(device_address=addr)
        try:
            oid = await app.parse_object_identifier(
                _object_id_string(object_type, object_instance),
                vendor_info=vendor_info,
            )
        except (TypeError, ValueError) as e:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "object_instance": object_instance,
                "error": failure_message(e, default="invalid object type or instance"),
            }

        req = DeleteObjectRequest(objectIdentifier=oid, destination=addr)
        try:
            response = await asyncio.wait_for(
                app.request(req),
                timeout=write_timeout,
            )
        except ErrorRejectAbortNack as err:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "object_instance": object_instance,
                "error": failure_message(err, default="DeleteObject rejected"),
            }
        except Exception as e:
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "object_instance": object_instance,
                "error": failure_message(e, default="DeleteObject failed"),
            }

        if isinstance(response, ErrorRejectAbortNack):
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "object_instance": object_instance,
                "error": failure_message(response, default="DeleteObject rejected"),
            }
        if not isinstance(response, SimpleAckPDU):
            return {
                "device_instance": device_instance,
                "object_type": _object_type_for_json(object_type),
                "object_instance": object_instance,
                "error": "unexpected response to DeleteObject",
            }
        return {
            "device_instance": device_instance,
            "object_type": _object_type_for_json(object_type),
            "object_instance": object_instance,
        }
