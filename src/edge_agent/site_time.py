"""Offline site-local time from weather lat/lon via timezonefinder + zoneinfo."""

from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo

from edge_agent.models import weather_coords_valid

_log = logging.getLogger(__name__)

# One shared finder (timezonefinder loads geographic data once per process).
_finder: Optional[TimezoneFinder] = None

# Cache IANA zone lookup per rounded lat/lon: (iana_name or None, error or None).
_tz_cache_key: Optional[tuple[float, float]] = None
_cached_tz_lookup: Optional[tuple[Optional[str], Optional[str]]] = None


def _get_finder() -> TimezoneFinder:
    global _finder
    if _finder is None:
        _finder = TimezoneFinder()
    return _finder


def _coord_key(lat: float, lon: float) -> tuple[float, float]:
    return (round(lat, 6), round(lon, 6))


def _resolve_tz_name_for_coords(lat: float, lon: float) -> tuple[Optional[str], Optional[str]]:
    """
    Return (iana_zone_name, error_message).
    Caches by rounded lat/lon; does not re-query timezonefinder until coordinates change.
    """
    global _tz_cache_key, _cached_tz_lookup
    key = _coord_key(lat, lon)
    if _tz_cache_key == key and _cached_tz_lookup is not None:
        return _cached_tz_lookup

    try:
        tz_name = _get_finder().timezone_at(lat=float(lat), lng=float(lon))
    except Exception as e:
        _log.warning("site_time_timezone_lookup_failed lat=%s lon=%s err=%s", lat, lon, e)
        # Do not cache: transient failure may succeed on a later poll.
        return None, f"timezone_lookup_failed:{e}"

    _tz_cache_key = key
    if tz_name:
        _cached_tz_lookup = (tz_name, None)
    else:
        _cached_tz_lookup = (None, "no_timezone_for_coordinates")
    return _cached_tz_lookup


def resolve_timezone_name(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    """Return IANA timezone name for (lat, lon), or None if invalid or lookup fails."""
    if not weather_coords_valid(lat, lon):
        return None
    assert lat is not None and lon is not None
    name, err = _resolve_tz_name_for_coords(float(lat), float(lon))
    if err:
        return None
    return name


@dataclass(frozen=True)
class SiteLocalTimeInfo:
    """Site-local clock snapshot from system UTC + resolved IANA zone."""

    timezone_name: Optional[str]
    local_datetime: Optional[datetime]
    local_date_iso: str
    local_time_iso: str
    local_datetime_iso: str
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int
    weekday_name: str
    weekday_number: int  # ISO 8601: 1=Monday … 7=Sunday
    is_dst: bool
    utc_offset_seconds: int
    utc_offset_minutes: int
    ok: bool
    error: Optional[str]


def _failed_info(error: str) -> SiteLocalTimeInfo:
    return SiteLocalTimeInfo(
        timezone_name=None,
        local_datetime=None,
        local_date_iso="",
        local_time_iso="",
        local_datetime_iso="",
        year=0,
        month=0,
        day=0,
        hour=0,
        minute=0,
        second=0,
        weekday_name="",
        weekday_number=0,
        is_dst=False,
        utc_offset_seconds=0,
        utc_offset_minutes=0,
        ok=False,
        error=error,
    )


def get_local_time_info(
    lat: Optional[float],
    lon: Optional[float],
) -> SiteLocalTimeInfo:
    """
    Convert current system UTC to site-local time using cached IANA zone from lat/lon.

    Uses ``datetime.now(timezone.utc)`` as the only time source (no network).
    On failure, ``ok`` is False and numeric/string fields are zeroed or empty; callers
    should preserve last BACnet presentValues if they should not flash-clear on error.
    """
    if not weather_coords_valid(lat, lon):
        return _failed_info("invalid_or_missing_coordinates")

    assert lat is not None and lon is not None
    la, lo = float(lat), float(lon)

    tz_name, err = _resolve_tz_name_for_coords(la, lo)
    if err or not tz_name:
        return _failed_info(err or "no_timezone_name")

    try:
        zi = ZoneInfo(tz_name)
    except Exception as e:
        _log.warning("site_time_zoneinfo_failed tz=%s err=%s", tz_name, e)
        return _failed_info(f"invalid_timezone_id:{e}")

    now_utc = datetime.now(timezone.utc)
    try:
        local = now_utc.astimezone(zi)
    except Exception as e:
        _log.warning("site_time_astimezone_failed tz=%s err=%s", tz_name, e)
        return _failed_info(f"astimezone_failed:{e}")

    off = local.utcoffset()
    utc_offset_seconds = int(off.total_seconds()) if off is not None else 0
    dst = local.dst()
    is_dst = dst is not None and dst.total_seconds() != 0

    d_iso = local.date().isoformat()
    t_iso = local.strftime("%H:%M:%S")
    # ISO 8601 local with offset, seconds precision
    dt_iso = local.isoformat(timespec="seconds")

    wn = calendar.day_name[local.weekday()]
    iso_dow = local.isoweekday()

    return SiteLocalTimeInfo(
        timezone_name=tz_name,
        local_datetime=local,
        local_date_iso=d_iso,
        local_time_iso=t_iso,
        local_datetime_iso=dt_iso,
        year=local.year,
        month=local.month,
        day=local.day,
        hour=local.hour,
        minute=local.minute,
        second=local.second,
        weekday_name=wn,
        weekday_number=iso_dow,
        is_dst=is_dst,
        utc_offset_seconds=utc_offset_seconds,
        utc_offset_minutes=utc_offset_seconds // 60,
        ok=True,
        error=None,
    )
