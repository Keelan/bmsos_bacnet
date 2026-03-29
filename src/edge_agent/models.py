"""Pydantic models, job envelope, BACnet client protocol."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional, Protocol, runtime_checkable

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class RemoteBacnetConfig(BaseModel):
    """Subset of SaaS `bacnet` JSON."""

    device_instance: Optional[int] = None
    bind_ip: Optional[str] = None
    udp_port: Optional[int] = None


class ConfigPullResponse(BaseModel):
    revision: Optional[int] = None
    updated_at: Optional[str] = None
    bacnet: Optional[RemoteBacnetConfig] = None
    unchanged: bool = False


class EffectiveBacnetConfig(BaseModel):
    device_instance: int
    bind_ip: str
    udp_port: int


def merge_bacnet(
    settings_device_instance: int,
    settings_bind_ip: str,
    settings_udp_port: int,
    remote: Optional[RemoteBacnetConfig],
) -> EffectiveBacnetConfig:
    eff = EffectiveBacnetConfig(
        device_instance=settings_device_instance,
        bind_ip=settings_bind_ip,
        udp_port=settings_udp_port,
    )
    if not remote:
        return eff
    if remote.device_instance is not None:
        eff.device_instance = remote.device_instance
    # Empty string from API must not wipe .env bind_ip (common JSON default).
    if remote.bind_ip is not None and remote.bind_ip.strip():
        eff.bind_ip = remote.bind_ip
    if remote.udp_port is not None:
        eff.udp_port = remote.udp_port
    return eff


class JobModel(BaseModel):
    """Laravel may use job_public_id / job_type; fake SaaS uses job_id / type."""

    model_config = ConfigDict(extra="ignore")

    job_id: str = Field(
        validation_alias=AliasChoices(
            "job_id",
            "job_public_id",
            "public_id",
            "id",
        ),
    )
    type: str = Field(
        validation_alias=AliasChoices("type", "job_type"),
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("payload"),
    )


class NextJobResponse(BaseModel):
    job: Optional[JobModel] = None


class JobResultEnvelope(BaseModel):
    job_id: str
    status: Literal["success", "partial_success", "failed"]
    started_at: str
    finished_at: str
    summary: str
    data: dict[str, Any] = Field(default_factory=dict)
    errors: list[dict[str, Any]] = Field(default_factory=list)


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


@runtime_checkable
class BacnetClient(Protocol):
    """Implemented by mock and BACpypes3 clients."""

    async def discover_network(self, who_is_timeout: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        ...

    async def snapshot_network(self, who_is_timeout: float, read_timeout: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        ...

    async def read_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        prop: str,
        read_timeout: float,
    ) -> dict[str, Any]:
        ...

    async def write_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        value: Any,
        priority: Optional[int],
        write_timeout: float,
    ) -> dict[str, Any]:
        ...
