"""HTTPS client for SaaS edge API (Bearer auth, retries)."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Optional

import httpx
from pydantic import ValidationError

from edge_agent.models import ConfigPullResponse, NextJobResponse
from edge_agent.settings import Settings

_log = logging.getLogger(__name__)


class SaasClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            base_url=settings.saas_base,
            headers={
                "Authorization": f"Bearer {settings.api_token}",
                "Content-Type": "application/json",
                # Laravel often 302s to login for unauthenticated *web* requests; JSON APIs
                # should return 401 instead when this header is present.
                "Accept": "application/json",
            },
            timeout=httpx.Timeout(settings.request_timeout_seconds),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _post_json(self, path: str, body: dict[str, Any]) -> Any:
        last_exc: Optional[BaseException] = None
        for attempt in range(self._settings.saas_max_retries):
            try:
                r = await self._client.post(path, json=body)
                if r.status_code in (301, 302, 303, 307, 308):
                    loc = r.headers.get("location", "")
                    raise httpx.HTTPStatusError(
                        f"redirect {r.status_code} to {loc!r} (check API route + Accept: application/json + auth)",
                        request=r.request,
                        response=r,
                    )
                if r.status_code >= 500:
                    raise httpx.HTTPStatusError("server error", request=r.request, response=r)
                r.raise_for_status()
                if not r.content:
                    return None
                return r.json()
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as e:
                last_exc = e
                backoff = self._settings.saas_retry_backoff_seconds * (2**attempt)
                backoff += random.uniform(0, 0.25)
                _log.warning("saas_post_retry path=%s attempt=%s err=%s", path, attempt + 1, e)
                await asyncio.sleep(backoff)
        _log.error("saas_post_failed path=%s err=%s", path, last_exc)
        raise last_exc  # type: ignore[misc]

    async def post_result_idempotent(self, job_id: str, body: dict[str, Any]) -> None:
        """POST result with retries; SaaS must treat duplicate job_id as idempotent."""
        path = f"/api/edge/v1/jobs/{job_id}/result"
        await self._post_json(path, body)

    async def heartbeat(self, body: dict[str, Any]) -> None:
        try:
            await self._post_json("/api/edge/v1/heartbeat", body)
        except Exception as e:
            _log.warning("heartbeat_failed err=%s", e)

    async def fetch_config(self, config_revision: Optional[int]) -> ConfigPullResponse:
        try:
            data = await self._post_json(
                "/api/edge/v1/config",
                {"box_id": self._settings.box_id, "config_revision": config_revision},
            )
            if not data:
                return ConfigPullResponse(unchanged=True)
            if data.get("unchanged"):
                return ConfigPullResponse(unchanged=True, revision=data.get("revision"))
            return ConfigPullResponse.model_validate(data)
        except Exception as e:
            _log.warning("fetch_config_failed err=%s", e)
            return ConfigPullResponse(unchanged=True)

    async def next_job(self) -> NextJobResponse:
        data: Any = None
        try:
            data = await self._post_json(
                "/api/edge/v1/jobs/next",
                {
                    "box_id": self._settings.box_id,
                    "hostname": __import__("socket").gethostname(),
                    "software_version": self._settings.software_version,
                },
            )
            resp = NextJobResponse.model_validate(data or {"job": None})
            if resp.job:
                _log.info(
                    "jobs_next_claimed job_id=%s type=%s",
                    resp.job.job_id,
                    resp.job.type,
                )
            return resp
        except ValidationError as e:
            job_blob = data.get("job") if isinstance(data, dict) else None
            _log.warning(
                "jobs_next_validation_failed job_blob=%r errors=%s",
                job_blob,
                e.errors(),
            )
            return NextJobResponse(job=None)
        except Exception as e:
            _log.warning("next_job_failed err=%s", e)
            return NextJobResponse(job=None)
