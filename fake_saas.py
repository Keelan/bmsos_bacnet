"""Minimal fake SaaS for local end-to-end tests (FastAPI).

Run (from repo root):
  pip install fastapi uvicorn
  uvicorn fake_saas:app --host 127.0.0.1 --port 8765

Env (optional):
  FAKE_SAAS_TOKEN=test-token
"""

from __future__ import annotations

import os
import uuid
from collections import deque
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

EXPECTED = os.environ.get("FAKE_SAAS_TOKEN", "test-token")

app = FastAPI(title="Fake Edge SaaS")

# in-memory state
_results: dict[str, dict[str, Any]] = {}
_config: dict[str, Any] = {
    "revision": 1,
    "updated_at": "2026-01-01T00:00:00Z",
    "bacnet": {
        "device_instance": 59999,
        "bind_ip": "",
        "udp_port": 47808,
    },
}
_job_queue: deque[dict[str, Any]] = deque()


def _auth(authorization: Optional[str]) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing bearer")
    token = authorization.split(" ", 1)[1].strip()
    if token != EXPECTED:
        raise HTTPException(403, "bad token")


class HeartbeatIn(BaseModel):
    box_id: str
    software_version: Optional[str] = None
    hostname: Optional[str] = None
    local_ip: Optional[str] = None
    uptime_seconds: Optional[int] = None
    bacnet_config_revision: Optional[int] = None


class ConfigIn(BaseModel):
    box_id: str
    config_revision: Optional[int] = None


class NextJobIn(BaseModel):
    box_id: str
    hostname: Optional[str] = None
    software_version: Optional[str] = None


@app.post("/api/edge/v1/heartbeat")
def heartbeat(
    body: HeartbeatIn,
    authorization: Optional[str] = Header(None),
) -> dict[str, str]:
    _auth(authorization)
    return {"status": "ok"}


@app.post("/api/edge/v1/config")
def pull_config(
    body: ConfigIn,
    authorization: Optional[str] = Header(None),
) -> dict[str, Any]:
    _auth(authorization)
    if body.config_revision == _config["revision"]:
        return {"unchanged": True, "revision": _config["revision"]}
    return {
        "revision": _config["revision"],
        "updated_at": _config["updated_at"],
        "bacnet": _config["bacnet"],
    }


@app.post("/api/edge/v1/jobs/next")
def next_job(
    body: NextJobIn,
    authorization: Optional[str] = Header(None),
) -> dict[str, Any]:
    _auth(authorization)
    if not _job_queue:
        return {"job": None}
    job = _job_queue.popleft()
    return {"job": job}


@app.post("/api/edge/v1/jobs/{job_id}/result")
def job_result(
    job_id: str,
    body: dict[str, Any],
    authorization: Optional[str] = Header(None),
) -> dict[str, str]:
    _auth(authorization)
    _results[job_id] = body
    return {"status": "ok"}


# --- Dev helpers (no auth) ---


class DevEnqueue(BaseModel):
    job_type: str
    payload: dict[str, Any] = {}


@app.post("/dev/enqueue-job")
def dev_enqueue(body: DevEnqueue) -> dict[str, Any]:
    jid = str(uuid.uuid4())
    _job_queue.append(
        {"job_id": jid, "type": body.job_type, "payload": body.payload}
    )
    return {"job_id": jid, "queued": len(_job_queue)}


@app.post("/dev/enqueue-raw")
def dev_enqueue_raw(job: dict[str, Any]) -> dict[str, Any]:
    """Push a full job object: {job_id?, type, payload}."""
    jid = job.get("job_id") or str(uuid.uuid4())
    _job_queue.append(
        {"job_id": jid, "type": job["type"], "payload": job.get("payload") or {}}
    )
    return {"job_id": jid, "queued": len(_job_queue)}


@app.get("/dev/results")
def dev_results() -> dict[str, Any]:
    return {"results": _results}


@app.post("/dev/config")
def dev_set_config(revision: int, bacnet: dict[str, Any]) -> dict[str, str]:
    _config["revision"] = revision
    _config["bacnet"] = bacnet
    return {"status": "ok"}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
