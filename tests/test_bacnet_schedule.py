from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from edge_agent.bacnet_schedule import (
    calendar_property_to_json,
    calendar_write_values,
    exception_schedule_to_json,
    references_to_json,
    schedule_property_to_json,
    schedule_write_values,
    weekly_schedule_to_json,
)
from edge_agent.mock_bacnet_client import MockBacnetClient
from edge_agent.job_runner import run_job
from edge_agent.models import JobModel
from edge_agent.settings import Settings
from edge_agent.storage import Storage


def schedule_payload() -> dict:
    off = {"type": "enumerated", "value": 0}
    on = {"type": "enumerated", "value": 1}
    return {
        "object_name": "OCC_SCHED",
        "description": "Occupancy schedule",
        "effective_period": {
            "start_date": "2026-01-01",
            "end_date": "2099-12-31",
        },
        "weekly_schedule": {
            "monday": [
                {"time": "07:00:00", "value": on},
                {"time": "18:00:00", "value": off},
            ],
            "tuesday": [],
            "wednesday": [],
            "thursday": [],
            "friday": [],
            "saturday": [],
            "sunday": [],
        },
        "exception_schedule": [
            {
                "period": {"type": "calendar-reference", "object_instance": 1},
                "time_values": [{"time": "00:00:00", "value": off}],
                "priority": 16,
            }
        ],
        "schedule_default": off,
        "references": [
            {
                "object_type": "binary-value",
                "object_instance": 5,
                "property": "present-value",
            }
        ],
        "priority_for_writing": 16,
        "out_of_service": False,
    }


class BacnetScheduleCodecTest(unittest.IsolatedAsyncioTestCase):
    def test_schedule_round_trip_preserves_types_and_structure(self) -> None:
        values = schedule_write_values(schedule_payload())

        self.assertEqual(
            weekly_schedule_to_json(values["weekly-schedule"]),
            schedule_payload()["weekly_schedule"],
        )
        self.assertEqual(
            exception_schedule_to_json(values["exception-schedule"]),
            schedule_payload()["exception_schedule"],
        )
        self.assertEqual(
            references_to_json(values["list-of-object-property-references"]),
            schedule_payload()["references"],
        )
        self.assertEqual(
            schedule_property_to_json("schedule-default", values["schedule-default"]),
            {"type": "enumerated", "value": 0},
        )

    def test_calendar_round_trip_supports_all_entry_choices(self) -> None:
        payload = {
            "object_name": "HOLIDAYS",
            "description": "Observed holidays",
            "date_list": [
                {"type": "date", "date": "2026-12-25"},
                {"type": "date", "date": "*-01-01"},
                {
                    "type": "date-range",
                    "start_date": "2026-07-01",
                    "end_date": "2026-07-03",
                },
                {
                    "type": "week-n-day",
                    "month": None,
                    "week_of_month": None,
                    "day_of_week": 1,
                },
            ],
        }
        values = calendar_write_values(payload)
        self.assertEqual(
            calendar_property_to_json("date-list", values["date-list"]),
            payload["date_list"],
        )

    def test_schedule_rejects_non_increasing_transitions(self) -> None:
        payload = schedule_payload()
        payload["weekly_schedule"]["monday"] = [
            {"time": "18:00", "value": {"type": "enumerated", "value": 0}},
            {"time": "07:00", "value": {"type": "enumerated", "value": 1}},
        ]
        with self.assertRaisesRegex(ValueError, "strictly increasing"):
            schedule_write_values(payload)

    def test_empty_kmc_exception_placeholder_is_ignored(self) -> None:
        payload = schedule_payload()
        payload["exception_schedule"][0]["time_values"] = []
        values = schedule_write_values(payload)

        self.assertEqual(
            schedule_property_to_json(
                "exception-schedule", values["exception-schedule"]
            ),
            [],
        )

    def test_schedule_rejects_controller_limit_overflow(self) -> None:
        payload = schedule_payload()
        payload["weekly_schedule"]["monday"] = [
            {
                "time": f"{hour:02d}:00:00",
                "value": {"type": "enumerated", "value": hour % 2},
            }
            for hour in range(7)
        ]
        with self.assertRaisesRegex(ValueError, "cannot exceed 6 transitions"):
            schedule_write_values(payload)

    def test_schedule_rejects_incompatible_or_input_targets(self) -> None:
        payload = schedule_payload()
        payload["references"][0]["object_type"] = "analog-value"
        with self.assertRaisesRegex(ValueError, "incompatible"):
            schedule_write_values(payload)

        payload = schedule_payload()
        payload["references"][0]["object_type"] = "binary-input"
        with self.assertRaisesRegex(ValueError, "unsupported schedule target"):
            schedule_write_values(payload)

    def test_rejects_reversed_effective_and_calendar_date_ranges(self) -> None:
        payload = schedule_payload()
        payload["effective_period"] = {
            "start_date": "2026-12-31",
            "end_date": "2026-01-01",
        }
        with self.assertRaisesRegex(ValueError, "end must be on or after start"):
            schedule_write_values(payload)

        with self.assertRaisesRegex(ValueError, "end must be on or after start"):
            calendar_write_values(
                {
                    "object_name": "HOLIDAYS",
                    "description": "Observed holidays",
                    "date_list": [
                        {
                            "type": "date-range",
                            "start_date": "2026-12-31",
                            "end_date": "2026-01-01",
                        }
                    ],
                }
            )

    async def test_mock_write_returns_verified_canonical_readback(self) -> None:
        result = await MockBacnetClient().write_schedule(
            2001, 1, schedule_payload(), 5.0, include_readback=True
        )
        self.assertTrue(result["verified"])
        self.assertEqual(result["readback"]["references"][0]["object_instance"], 5)

    async def test_job_runner_dispatches_verified_schedule_and_calendar_writes(self) -> None:
        settings = Settings(
            saas_base_url="https://example.invalid",
            box_id="test-box",
            api_token="test-token",
            request_timeout_seconds=1.0,
        )
        with TemporaryDirectory() as directory:
            storage = Storage(str(Path(directory) / "edge.sqlite"))
            try:
                schedule_result = await run_job(
                    JobModel(
                        job_id="schedule-job",
                        type="write_schedule",
                        payload={
                            "device_instance": 2001,
                            "object_type": "schedule",
                            "object_instance": 1,
                            "schedule": schedule_payload(),
                            "include_readback": True,
                        },
                    ),
                    MockBacnetClient(),
                    storage,
                    settings,
                )
                calendar_result = await run_job(
                    JobModel(
                        job_id="calendar-job",
                        type="write_calendar",
                        payload={
                            "device_instance": 2001,
                            "object_type": "calendar",
                            "object_instance": 1,
                            "calendar": {
                                "object_name": "HOLIDAYS",
                                "description": "Observed holidays",
                                "date_list": [
                                    {"type": "date", "date": "*-12-25"}
                                ],
                            },
                            "include_readback": True,
                            "create_if_missing": True,
                        },
                    ),
                    MockBacnetClient(),
                    storage,
                    settings,
                )
            finally:
                storage.close()

        self.assertEqual(schedule_result.status, "success")
        self.assertTrue(schedule_result.data["verified"])
        self.assertEqual(calendar_result.status, "success")
        self.assertTrue(calendar_result.data["verified"])


if __name__ == "__main__":
    unittest.main()
