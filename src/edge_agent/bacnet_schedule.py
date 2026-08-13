"""BACnet Schedule and Calendar payload encoding/decoding.

The wire values are constructed BACnet types.  Keep their JSON representation
typed so schedule values such as Enumerated(1), Boolean(True), and Real(1.0)
cannot be confused during deploy or readback verification.
"""

from __future__ import annotations

from datetime import date as py_date
from typing import Any, Iterable

from bacpypes3.basetypes import (
    CalendarEntry,
    DailySchedule,
    DateRange,
    DeviceObjectPropertyReference,
    SpecialEvent,
    SpecialEventPeriod,
    TimeValue,
    WeekNDay,
)
from bacpypes3.constructeddata import AnyAtomic, ArrayOf, ListOf
from bacpypes3.primitivedata import (
    Boolean,
    CharacterString,
    Date,
    Double,
    Enumerated,
    Integer,
    Null,
    ObjectIdentifier,
    PropertyIdentifier,
    Real,
    Time,
    Unsigned,
)


WEEKDAYS = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)

MAX_DAILY_TRANSITIONS = 6
MAX_EXCEPTIONS = 12
MAX_EXCEPTION_TRANSITIONS = 6
MAX_REFERENCES = 10

_ATOMIC_TYPES: dict[str, type] = {
    "boolean": Boolean,
    "enumerated": Enumerated,
    "unsigned": Unsigned,
    "integer": Integer,
    "real": Real,
    "double": Double,
    "character-string": CharacterString,
}

_REFERENCE_VALUE_TYPES = {
    "analog-output": "real",
    "analog-value": "real",
    "binary-output": "enumerated",
    "binary-value": "enumerated",
    "multi-state-output": "unsigned",
    "multi-state-value": "unsigned",
}


def _type_key(value: Any) -> str:
    return str(value).strip().lower().replace("_", "-")


def typed_value_to_json(value: Any) -> dict[str, Any]:
    """Decode AnyAtomic/Atomic while preserving its BACnet primitive type."""
    if isinstance(value, AnyAtomic):
        if value.tagList is None or not len(value.tagList):
            return {"type": "null", "value": None}
        value = value.tagList.peek().app_to_object()

    if isinstance(value, Null):
        return {"type": "null", "value": None}
    if isinstance(value, Boolean):
        return {"type": "boolean", "value": bool(value)}
    if isinstance(value, Enumerated):
        return {"type": "enumerated", "value": int(value)}
    if isinstance(value, Unsigned):
        return {"type": "unsigned", "value": int(value)}
    if isinstance(value, Integer):
        return {"type": "integer", "value": int(value)}
    if isinstance(value, Real):
        return {"type": "real", "value": float(value)}
    if isinstance(value, Double):
        return {"type": "double", "value": float(value)}
    if isinstance(value, CharacterString):
        return {"type": "character-string", "value": str(value)}
    raise ValueError(f"unsupported BACnet schedule value type: {type(value).__name__}")


def typed_value_from_json(value: Any) -> AnyAtomic:
    if value is None:
        return AnyAtomic(Null(()))
    if not isinstance(value, dict):
        raise ValueError("schedule values must contain type and value")

    kind = _type_key(value.get("type", ""))
    raw = value.get("value")
    if kind == "null":
        return AnyAtomic(Null(()))
    atomic_type = _ATOMIC_TYPES.get(kind)
    if atomic_type is None:
        raise ValueError(f"unsupported schedule value type: {kind or '(empty)'}")
    if raw is None:
        raise ValueError(f"schedule value for {kind} cannot be null")

    if kind == "boolean":
        if isinstance(raw, str):
            lowered = raw.strip().lower()
            if lowered not in ("true", "false", "1", "0"):
                raise ValueError("boolean schedule value must be true or false")
            raw = lowered in ("true", "1")
        else:
            raw = bool(raw)
    elif kind in ("enumerated", "unsigned", "integer"):
        raw = int(raw)
        if kind == "unsigned" and raw < 0:
            raise ValueError("unsigned schedule value cannot be negative")
    elif kind in ("real", "double"):
        raw = float(raw)
    else:
        raw = str(raw)
    return AnyAtomic(atomic_type(raw))


def _date_to_string(value: Date) -> str:
    year, month, day, _day_of_week = tuple(value)
    year_text = "*" if year == 255 else str(int(year) + 1900)
    month_text = "*" if month == 255 else f"{int(month):02d}"
    day_text = "*" if day == 255 else f"{int(day):02d}"
    return f"{year_text}-{month_text}-{day_text}"


def _date_from_string(value: Any) -> Date:
    text = str(value or "").strip()
    parts = text.split("-")
    if len(parts) != 3:
        raise ValueError("BACnet date must be YYYY-MM-DD with optional * wildcards")
    year_text, month_text, day_text = parts
    year = 255 if year_text == "*" else int(year_text) - 1900
    month = 255 if month_text == "*" else int(month_text)
    day = 255 if day_text == "*" else int(day_text)
    if year != 255 and not 0 <= year <= 255:
        raise ValueError("BACnet date year must be between 1900 and 2155")
    if month != 255 and not 1 <= month <= 12:
        raise ValueError("BACnet date month must be 1-12")
    if day != 255 and not 1 <= day <= 31:
        raise ValueError("BACnet date day must be 1-31")
    day_of_week = 255
    if year != 255 and month != 255 and day != 255:
        day_of_week = py_date(year + 1900, month, day).isoweekday()
    elif month != 255 and day != 255:
        py_date(2000, month, day)
    return Date((year, month, day, day_of_week))


def _time_to_string(value: Time) -> str:
    hour, minute, second, hundredth = tuple(value)
    base = f"{int(hour):02d}:{int(minute):02d}:{int(second):02d}"
    return f"{base}.{int(hundredth):02d}" if hundredth else base


def _time_from_string(value: Any) -> Time:
    text = str(value or "").strip()
    try:
        parsed = Time(text)
    except (TypeError, ValueError) as exc:
        raise ValueError("BACnet time must be HH:MM[:SS[.hh]]") from exc
    if any(part == 255 for part in tuple(parsed)):
        raise ValueError("schedule transition times cannot contain wildcards")
    return parsed


def _validate_date_range(start: Date, end: Date) -> None:
    start_parts = tuple(start)[:3]
    end_parts = tuple(end)[:3]
    if 255 not in start_parts and 255 not in end_parts and end_parts < start_parts:
        raise ValueError("date range end must be on or after start")


def _object_type(value: Any) -> str:
    return str(value).strip().lower().replace("_", "-")


def _calendar_entry_to_json(entry: CalendarEntry) -> dict[str, Any]:
    choice = getattr(entry, "_choice", None)
    if choice == "date":
        return {"type": "date", "date": _date_to_string(entry.date)}
    if choice == "dateRange":
        return {
            "type": "date-range",
            "start_date": _date_to_string(entry.dateRange.startDate),
            "end_date": _date_to_string(entry.dateRange.endDate),
        }
    if choice == "weekNDay":
        month, week_of_month, day_of_week = bytes(entry.weekNDay)
        return {
            "type": "week-n-day",
            "month": None if month == 255 else int(month),
            "week_of_month": None if week_of_month == 255 else int(week_of_month),
            "day_of_week": None if day_of_week == 255 else int(day_of_week),
        }
    raise ValueError("unsupported or empty CalendarEntry")


def _calendar_entry_from_json(entry: Any) -> CalendarEntry:
    if not isinstance(entry, dict):
        raise ValueError("calendar entry must be an object")
    kind = _type_key(entry.get("type", ""))
    if kind == "date":
        return CalendarEntry(date=_date_from_string(entry.get("date")))
    if kind == "date-range":
        start = _date_from_string(entry.get("start_date"))
        end = _date_from_string(entry.get("end_date"))
        _validate_date_range(start, end)
        return CalendarEntry(
            dateRange=DateRange(
                startDate=start,
                endDate=end,
            )
        )
    if kind == "week-n-day":
        values = []
        for key, minimum, maximum in (
            ("month", 1, 14),
            ("week_of_month", 1, 6),
            ("day_of_week", 1, 7),
        ):
            raw = entry.get(key)
            parsed = 255 if raw is None else int(raw)
            if parsed != 255 and not minimum <= parsed <= maximum:
                raise ValueError(f"{key} must be {minimum}-{maximum} or null")
            values.append(parsed)
        return CalendarEntry(weekNDay=WeekNDay(bytes(values)))
    raise ValueError(f"unsupported calendar entry type: {kind or '(empty)'}")


def _time_values_to_json(values: Iterable[TimeValue]) -> list[dict[str, Any]]:
    return [
        {"time": _time_to_string(item.time), "value": typed_value_to_json(item.value)}
        for item in values
    ]


def _time_values_from_json(values: Any) -> list[TimeValue]:
    if not isinstance(values, list):
        raise ValueError("time_values must be a list")
    out: list[TimeValue] = []
    previous: tuple[int, int, int, int] | None = None
    for item in values:
        if not isinstance(item, dict):
            raise ValueError("time value must be an object")
        time = _time_from_string(item.get("time"))
        current = tuple(time)
        if previous is not None and current <= previous:
            raise ValueError("schedule transition times must be strictly increasing")
        previous = current
        out.append(TimeValue(time=time, value=typed_value_from_json(item.get("value"))))
    return out


def weekly_schedule_to_json(value: Iterable[DailySchedule]) -> dict[str, Any]:
    days = list(value)
    if len(days) != 7:
        raise ValueError("weekly-schedule must contain seven days")
    return {
        weekday: _time_values_to_json(days[index].daySchedule)
        for index, weekday in enumerate(WEEKDAYS)
    }


def weekly_schedule_from_json(value: Any) -> Any:
    if not isinstance(value, dict):
        raise ValueError("weekly_schedule must be an object keyed by weekday")
    daily = [
        DailySchedule(daySchedule=_time_values_from_json(value.get(day, [])))
        for day in WEEKDAYS
    ]
    return ArrayOf(DailySchedule)(daily)


def exception_schedule_to_json(value: Iterable[SpecialEvent]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for event in value:
        time_values = _time_values_to_json(event.listOfTimeValues)
        # KMC fixed Schedule slots contain a dangling CAL1 event with no
        # transitions. It has no effect and should not become a design dependency.
        if not time_values:
            continue
        period = event.period
        choice = getattr(period, "_choice", None)
        if choice == "calendarReference":
            ref = period.calendarReference
            period_json: dict[str, Any] = {
                "type": "calendar-reference",
                "object_instance": int(ref[1]),
            }
        elif choice == "calendarEntry":
            period_json = _calendar_entry_to_json(period.calendarEntry)
        else:
            raise ValueError("unsupported or empty SpecialEventPeriod")
        out.append(
            {
                "period": period_json,
                "time_values": time_values,
                "priority": int(event.eventPriority),
            }
        )
    return out


def exception_schedule_from_json(value: Any) -> Any:
    if not isinstance(value, list):
        raise ValueError("exception_schedule must be a list")
    events: list[SpecialEvent] = []
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("period"), dict):
            raise ValueError("schedule exception must contain a period")
        period_json = item["period"]
        kind = _type_key(period_json.get("type", ""))
        if kind == "calendar-reference":
            instance = int(period_json.get("object_instance"))
            if instance < 1:
                raise ValueError("calendar reference object_instance must be positive")
            period = SpecialEventPeriod(
                calendarReference=ObjectIdentifier(("calendar", instance))
            )
        else:
            period = SpecialEventPeriod(
                calendarEntry=_calendar_entry_from_json(period_json)
            )
        priority = int(item.get("priority", 16))
        if not 1 <= priority <= 16:
            raise ValueError("schedule exception priority must be 1-16")
        events.append(
            SpecialEvent(
                period=period,
                listOfTimeValues=_time_values_from_json(item.get("time_values", [])),
                eventPriority=Unsigned(priority),
            )
        )
    return ArrayOf(SpecialEvent)(events)


def references_to_json(value: Iterable[DeviceObjectPropertyReference]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for reference in value:
        oid = reference.objectIdentifier
        row: dict[str, Any] = {
            "object_type": _object_type(oid[0]),
            "object_instance": int(oid[1]),
            "property": str(reference.propertyIdentifier),
        }
        if reference.propertyArrayIndex is not None:
            row["array_index"] = int(reference.propertyArrayIndex)
        if reference.deviceIdentifier is not None:
            row["device_instance"] = int(reference.deviceIdentifier[1])
        out.append(row)
    return out


def references_from_json(value: Any) -> Any:
    if not isinstance(value, list):
        raise ValueError("references must be a list")
    if len(value) > MAX_REFERENCES:
        raise ValueError(f"references cannot exceed {MAX_REFERENCES} objects")
    references: list[DeviceObjectPropertyReference] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("schedule reference must be an object")
        object_type = _object_type(item.get("object_type", ""))
        object_instance = int(item.get("object_instance"))
        if not object_type or object_instance < 0:
            raise ValueError("schedule reference requires a valid object")
        if object_type not in _REFERENCE_VALUE_TYPES:
            raise ValueError(f"unsupported schedule target object type: {object_type}")
        property_name = _type_key(item.get("property", "present-value"))
        if property_name != "present-value":
            raise ValueError("schedule references currently support present-value only")
        if item.get("device_instance") is not None:
            raise ValueError("schedule references must target objects on the local controller")
        kwargs: dict[str, Any] = {
            "objectIdentifier": ObjectIdentifier((object_type, object_instance)),
            "propertyIdentifier": PropertyIdentifier(property_name),
        }
        if item.get("array_index") is not None:
            kwargs["propertyArrayIndex"] = Unsigned(int(item["array_index"]))
        references.append(DeviceObjectPropertyReference(**kwargs))
    return ListOf(DeviceObjectPropertyReference)(references)


def schedule_property_to_json(property_name: str, value: Any) -> Any:
    key = _type_key(property_name)
    if key in ("present-value", "schedule-default"):
        return typed_value_to_json(value)
    if key == "effective-period":
        return {
            "start_date": _date_to_string(value.startDate),
            "end_date": _date_to_string(value.endDate),
        }
    if key == "weekly-schedule":
        return weekly_schedule_to_json(value)
    if key == "exception-schedule":
        return exception_schedule_to_json(value)
    if key == "list-of-object-property-references":
        return references_to_json(value)
    if key == "priority-for-writing":
        return int(value)
    if key in ("out-of-service", "present-value"):
        return bool(value)
    return str(value)


def schedule_write_values(schedule: Any) -> dict[str, Any]:
    if not isinstance(schedule, dict):
        raise ValueError("schedule must be an object")
    object_name = str(schedule.get("object_name") or "").strip()
    description = str(schedule.get("description") or "")
    if not object_name:
        raise ValueError("schedule object_name is required")
    if len(object_name) > 32:
        raise ValueError("schedule object_name cannot exceed 32 characters")
    if len(description) > 64:
        raise ValueError("schedule description cannot exceed 64 characters")
    effective = schedule.get("effective_period")
    if not isinstance(effective, dict):
        raise ValueError("effective_period is required")
    priority = int(schedule.get("priority_for_writing", 16))
    if not 1 <= priority <= 16:
        raise ValueError("priority_for_writing must be 1-16")
    weekly_json = schedule.get("weekly_schedule", {})
    exceptions_json = schedule.get("exception_schedule", [])
    if not isinstance(weekly_json, dict):
        raise ValueError("weekly_schedule must be an object keyed by weekday")
    if not isinstance(exceptions_json, list):
        raise ValueError("exception_schedule must be a list")
    if len(exceptions_json) > MAX_EXCEPTIONS:
        raise ValueError(f"exception_schedule cannot exceed {MAX_EXCEPTIONS} events")

    typed_values: list[Any] = [schedule.get("schedule_default")]
    for weekday in WEEKDAYS:
        transitions = weekly_json.get(weekday, [])
        if not isinstance(transitions, list):
            raise ValueError(f"weekly_schedule.{weekday} must be a list")
        if len(transitions) > MAX_DAILY_TRANSITIONS:
            raise ValueError(
                f"weekly_schedule.{weekday} cannot exceed {MAX_DAILY_TRANSITIONS} transitions"
            )
        typed_values.extend(
            item.get("value") for item in transitions if isinstance(item, dict)
        )
    for event in exceptions_json:
        if not isinstance(event, dict):
            raise ValueError("schedule exception must be an object")
        transitions = event.get("time_values", [])
        if not isinstance(transitions, list):
            raise ValueError("schedule exception time_values must be a list")
        if len(transitions) > MAX_EXCEPTION_TRANSITIONS:
            raise ValueError(
                f"schedule exception cannot exceed {MAX_EXCEPTION_TRANSITIONS} transitions"
            )
        typed_values.extend(
            item.get("value") for item in transitions if isinstance(item, dict)
        )
    value_types = {
        _type_key(item.get("type", ""))
        for item in typed_values
        if isinstance(item, dict) and _type_key(item.get("type", "")) != "null"
    }
    if len(value_types) > 1:
        raise ValueError("all non-null schedule values must use the same BACnet type")
    references_json = schedule.get("references", [])
    if not isinstance(references_json, list):
        raise ValueError("references must be a list")
    target_types = {
        _REFERENCE_VALUE_TYPES.get(_object_type(reference.get("object_type", "")))
        for reference in references_json
        if isinstance(reference, dict)
    }
    target_types.discard(None)
    if len(target_types) > 1:
        raise ValueError("all schedule targets must use the same BACnet value type")
    if target_types and value_types and target_types != value_types:
        raise ValueError("schedule value type is incompatible with its target objects")
    effective_start = _date_from_string(effective.get("start_date"))
    effective_end = _date_from_string(effective.get("end_date"))
    _validate_date_range(effective_start, effective_end)
    return {
        "object-name": CharacterString(object_name),
        "description": CharacterString(description),
        "effective-period": DateRange(
            startDate=effective_start,
            endDate=effective_end,
        ),
        "weekly-schedule": weekly_schedule_from_json(weekly_json),
        "exception-schedule": exception_schedule_from_json(exceptions_json),
        "schedule-default": typed_value_from_json(schedule.get("schedule_default")),
        "list-of-object-property-references": references_from_json(references_json),
        "priority-for-writing": Unsigned(priority),
        "out-of-service": Boolean(bool(schedule.get("out_of_service", False))),
    }


def calendar_property_to_json(property_name: str, value: Any) -> Any:
    key = _type_key(property_name)
    if key == "date-list":
        return [_calendar_entry_to_json(entry) for entry in value]
    if key == "present-value":
        return bool(value)
    return str(value)


def calendar_write_values(calendar: Any) -> dict[str, Any]:
    if not isinstance(calendar, dict):
        raise ValueError("calendar must be an object")
    object_name = str(calendar.get("object_name") or "").strip()
    description = str(calendar.get("description") or "")
    if not object_name:
        raise ValueError("calendar object_name is required")
    if len(object_name) > 32:
        raise ValueError("calendar object_name cannot exceed 32 characters")
    if len(description) > 64:
        raise ValueError("calendar description cannot exceed 64 characters")
    entries = calendar.get("date_list", [])
    if not isinstance(entries, list):
        raise ValueError("calendar date_list must be a list")
    if len(entries) > 256:
        raise ValueError("calendar date_list cannot exceed 256 entries")
    return {
        "object-name": CharacterString(object_name),
        "description": CharacterString(description),
        "date-list": ListOf(CalendarEntry)(
            [_calendar_entry_from_json(entry) for entry in entries]
        ),
    }
