"""Public holidays via Nager.Date (no API key). Cached per (country, year)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import httpx

NAGER_PUBLIC_HOLIDAYS_URL = "https://date.nager.at/api/v3/PublicHolidays/{year}/{country}"

# Full-year list per (ISO country alpha-2 upper, year).
_holiday_lists: dict[tuple[str, int], list[dict[str, Any]]] = {}


@dataclass(frozen=True)
class HolidayEval:
    """Derived from site-local calendar date and Nager.Date list."""

    holiday_today: bool
    holiday_name: str
    business_day: bool
    long_weekend: bool
    holiday_api_ok: bool
    error: str


def _parse_holiday_date(item: dict[str, Any]) -> Optional[date]:
    raw = item.get("date")
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


async def load_public_holidays_year(
    country_code: str,
    year: int,
    *,
    client: httpx.AsyncClient,
    timeout_seconds: float = 20.0,
) -> tuple[list[dict[str, Any]], bool, str]:
    """
    Fetch and cache full-year holidays. Returns (list, ok, error_message).
    On HTTP failure, returns ([], False, err) without discarding a prior good cache for that key.
    """
    cc = country_code.strip().upper()
    y = int(year)
    key = (cc, y)
    if key in _holiday_lists:
        return _holiday_lists[key], True, ""

    url = NAGER_PUBLIC_HOLIDAYS_URL.format(year=y, country=cc)
    try:
        r = await client.get(url, timeout=httpx.Timeout(timeout_seconds))
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return [], False, str(e) or type(e).__name__

    if not isinstance(data, list):
        return [], False, "nager_invalid_response"

    _holiday_lists[key] = data
    return data, True, ""


def evaluate_holidays_for_local_date(
    country_code: Optional[str],
    local_date: date,
    weekday_iso: int,
    cached_list: Optional[list[dict[str, Any]]],
    *,
    load_ok: bool,
    load_error: str,
) -> HolidayEval:
    """
    ``weekday_iso``: 1=Monday … 7=Sunday (``datetime.isoweekday()``).

    Long-weekend rule (conservative, documented):
    - Active when today is a public holiday on Friday, Saturday, or Sunday (extended weekend),
      or when today is a Monday public holiday (weekend extension). Does not model all bridge days.
    """
    if not country_code or len(country_code.strip()) != 2:
        return HolidayEval(
            holiday_today=False,
            holiday_name="",
            business_day=(1 <= weekday_iso <= 5),
            long_weekend=False,
            holiday_api_ok=False,
            error="missing_or_invalid_site_country_code",
        )

    if not load_ok:
        return HolidayEval(
            holiday_today=False,
            holiday_name="",
            business_day=(1 <= weekday_iso <= 5),
            long_weekend=False,
            holiday_api_ok=False,
            error=load_error or "holiday_load_failed",
        )

    rows = cached_list if cached_list is not None else []

    holiday_dates: dict[date, str] = {}
    for item in rows:
        d = _parse_holiday_date(item)
        if d is None:
            continue
        name = item.get("localName") or item.get("name") or ""
        if isinstance(name, str) and name.strip():
            holiday_dates[d] = name.strip()
        elif d not in holiday_dates:
            holiday_dates[d] = "Holiday"

    is_holiday = local_date in holiday_dates
    name = holiday_dates.get(local_date, "")

    business_day = (1 <= weekday_iso <= 5) and not is_holiday

    long_weekend = False
    if is_holiday:
        if weekday_iso in (5, 6, 7):
            long_weekend = True
        elif weekday_iso == 1:
            long_weekend = True

    return HolidayEval(
        holiday_today=is_holiday,
        holiday_name=name,
        business_day=business_day,
        long_weekend=long_weekend,
        holiday_api_ok=True,
        error="",
    )


def clear_holiday_cache_for_tests() -> None:
    _holiday_lists.clear()
