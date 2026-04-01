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
    # BACnet object-name for this device (BACpypes `--name`).
    device_name: Optional[str] = None
    # IP subnet prefix for bind (e.g. 24); required for broadcast Who-Is on many networks.
    bind_prefix: Optional[int] = None
    vendor_identifier: Optional[int] = None


class RemoteAgentTuning(BaseModel):
    """Optional SaaS `agent` JSON — overrides env defaults until next config push."""

    poll_interval_seconds: Optional[float] = None
    heartbeat_interval_seconds: Optional[float] = None
    config_poll_interval_seconds: Optional[float] = None
    edge_status_check_interval_seconds: Optional[float] = None
    who_is_timeout_seconds: Optional[float] = None
    read_device_live_max_objects: Optional[int] = None
    read_device_live_timeout_seconds: Optional[float] = None


class ConfigPullResponse(BaseModel):
    revision: Optional[int] = None
    updated_at: Optional[str] = None
    bacnet: Optional[RemoteBacnetConfig] = None
    agent: Optional[RemoteAgentTuning] = None
    unchanged: bool = False


class EffectiveBacnetConfig(BaseModel):
    device_instance: int
    bind_ip: str
    udp_port: int
    device_name: str
    bind_prefix: int
    vendor_identifier: int


def merge_bacnet(
    settings_device_instance: int,
    settings_bind_ip: str,
    settings_udp_port: int,
    settings_device_name: str,
    settings_bind_prefix: int,
    settings_vendor_identifier: int,
    remote: Optional[RemoteBacnetConfig],
) -> EffectiveBacnetConfig:
    eff = EffectiveBacnetConfig(
        device_instance=settings_device_instance,
        bind_ip=settings_bind_ip,
        udp_port=settings_udp_port,
        device_name=settings_device_name.strip() or "Excelsior",
        bind_prefix=int(settings_bind_prefix),
        vendor_identifier=int(settings_vendor_identifier),
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
    if remote.device_name is not None and remote.device_name.strip():
        eff.device_name = remote.device_name.strip()
    if remote.bind_prefix is not None:
        eff.bind_prefix = int(remote.bind_prefix)
    if remote.vendor_identifier is not None:
        eff.vendor_identifier = int(remote.vendor_identifier)
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


def apply_float_tuning(
    base: float,
    tuning: Optional[RemoteAgentTuning],
    field: str,
    lo: float,
    hi: float,
) -> float:
    if tuning is None:
        return base
    v = getattr(tuning, field, None)
    if v is None:
        return base
    x = float(v)
    return max(lo, min(hi, x))


def apply_int_tuning(
    base: int,
    tuning: Optional[RemoteAgentTuning],
    field: str,
    lo: int,
    hi: int,
) -> int:
    if tuning is None:
        return base
    v = getattr(tuning, field, None)
    if v is None:
        return base
    x = int(v)
    return max(lo, min(hi, x))


@runtime_checkable
class BacnetClient(Protocol):
    """Implemented by mock and BACpypes3 clients."""

    async def discover_network(self, who_is_timeout: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        ...

    async def snapshot_network(self, who_is_timeout: float, read_timeout: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        ...

    async def read_device_live(
        self,
        device_instance: int,
        read_timeout: float,
        max_objects: int,
        deadline_monotonic: Optional[float] = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        ...

    async def read_point(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        prop: str,
        read_timeout: float,
        array_index: Optional[int] = None,
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
        include_readback: bool = False,
    ) -> dict[str, Any]:
        ...

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
        ...

    async def create_object(
        self,
        device_instance: int,
        object_type: str,
        object_instance: Optional[int],
        initial_properties: Optional[list[dict[str, Any]]],
        write_timeout: float,
    ) -> dict[str, Any]:
        ...

    async def delete_object(
        self,
        device_instance: int,
        object_type: str,
        object_instance: int,
        write_timeout: float,
    ) -> dict[str, Any]:
        ...
