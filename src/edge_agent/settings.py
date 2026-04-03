"""Environment configuration (pydantic-settings)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    saas_base_url: str
    box_id: str
    api_token: str

    poll_interval_seconds: float = 5.0
    heartbeat_interval_seconds: float = 30.0
    config_poll_interval_seconds: float = 60.0
    request_timeout_seconds: float = 30.0

    local_db_path: str = "edge_agent.sqlite"
    log_level: str = "INFO"

    bacnet_device_instance: int = 59999
    bacnet_bind_ip: str = ""
    # Subnet prefix for BACnet/IP bind. BACpypes3 treats "ip:port" without mask as /32 → no broadcast.
    bacnet_bind_prefix: int = 24
    bacnet_udp_port: int = 47808
    # Local device object-name (BACpypes `--name`); remote config can override.
    bacnet_device_name: str = "Excelsior"
    # Device object model-name (BACnet property model-name).
    bacnet_model_name: str = "bmOS-edge"
    # Device object vendor-name (BACnet property vendor-name).
    bacnet_vendor_name: str = "bmsOS"
    bacnet_vendor_identifier: int = 999
    # Who-Is handling: "unicast" = I-Am back to requester (BACpypes default); "broadcast" =
    # I-Am to GlobalBroadcast (mapped to subnet broadcast / Original-Broadcast-NPDU on IPv4).
    bacnet_iam_response_mode: Literal["unicast", "broadcast"] = "unicast"
    bacnet_mock: bool = False

    @field_validator("bacnet_iam_response_mode", mode="before")
    @classmethod
    def _coerce_iam_response_mode(cls, v: Any) -> str:
        if v is None or (isinstance(v, str) and not v.strip()):
            return "unicast"
        s = str(v).strip().lower()
        if s in ("unicast", "broadcast"):
            return s
        return "unicast"

    software_version: str = "0.1.17"
    who_is_timeout_seconds: float = 5.0
    read_device_live_max_objects: int = 500
    read_device_live_timeout_seconds: float = 120.0
    saas_max_retries: int = 5
    saas_retry_backoff_seconds: float = 0.5

    # Edge status BACnet binary inputs + SaaS "online" window (align with Laravel config('edge.online_threshold_seconds')).
    edge_status_check_interval_seconds: float = 30.0
    saas_online_threshold_seconds: float = 120.0
    internet_check_url: str = "https://www.google.com/generate_204"
    internet_check_timeout_seconds: float = 5.0

    # Weather (Open-Meteo) poll interval when SaaS omits `weather_poll_interval_seconds` (15–60 min).
    weather_poll_interval_seconds: float = 1800.0

    # Site-local BACnet time refresh (IANA zone from weather lat/lon; system UTC clock).
    # Default 1s so hour/minute/second points stay aligned with wall time; raise if COV/load is an issue.
    site_time_poll_interval_seconds: float = 1.0

    @property
    def saas_base(self) -> str:
        return self.saas_base_url.rstrip("/")
