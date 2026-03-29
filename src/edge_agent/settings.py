"""Environment configuration (pydantic-settings)."""

from __future__ import annotations

from pydantic import Field
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
    bacnet_udp_port: int = 47808
    bacnet_mock: bool = False

    software_version: str = "0.1.0"
    who_is_timeout_seconds: float = 5.0
    saas_max_retries: int = 5
    saas_retry_backoff_seconds: float = 0.5

    @property
    def saas_base(self) -> str:
        return self.saas_base_url.rstrip("/")
