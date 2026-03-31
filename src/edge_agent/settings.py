"""Environment configuration (pydantic-settings)."""

from __future__ import annotations

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
    bacnet_vendor_identifier: int = 999
    bacnet_mock: bool = False

    software_version: str = "0.1.3"
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

    @property
    def saas_base(self) -> str:
        return self.saas_base_url.rstrip("/")
