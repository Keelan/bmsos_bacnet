"""SQLite persistence: JSON blobs + write audit."""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Optional

from pydantic import TypeAdapter

from edge_agent.models import RemoteAgentTuning, RemoteBacnetConfig, utc_now_iso


class Storage:
    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )"""
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS write_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                job_id TEXT NOT NULL,
                record TEXT NOT NULL
            )"""
        )
        self._conn.commit()

    def kv_get(self, key: str) -> Optional[str]:
        with self._lock:
            row = self._conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
            return str(row[0]) if row else None

    def kv_set(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    def get_stored_remote_config_dict(self) -> Optional[dict[str, Any]]:
        raw = self.kv_get("remote_config")
        if not raw:
            return None
        return json.loads(raw)

    def get_remote_config_state(self) -> tuple[Optional[int], Optional[RemoteBacnetConfig]]:
        data = self.get_stored_remote_config_dict()
        if not data:
            return None, None
        rev = data.get("revision")
        bacnet = data.get("bacnet")
        cfg = TypeAdapter(RemoteBacnetConfig).validate_python(bacnet) if bacnet else None
        return rev, cfg

    def get_remote_agent_tuning(self) -> Optional[RemoteAgentTuning]:
        data = self.get_stored_remote_config_dict()
        if not data:
            return None
        agent = data.get("agent")
        if not agent or not isinstance(agent, dict):
            return None
        return TypeAdapter(RemoteAgentTuning).validate_python(agent)

    def save_remote_config(
        self,
        revision: int,
        updated_at: str,
        bacnet: dict[str, Any],
        agent: dict[str, Any],
    ) -> None:
        doc: dict[str, Any] = {
            "revision": revision,
            "updated_at": updated_at,
            "bacnet": bacnet,
            "agent": agent,
        }
        payload = json.dumps(doc)
        self.kv_set("remote_config", payload)

    def save_latest_discovery(self, doc: dict[str, Any]) -> None:
        self.kv_set("latest_discovery", json.dumps(doc))

    def save_latest_snapshot(self, doc: dict[str, Any]) -> None:
        self.kv_set("latest_snapshot", json.dumps(doc))

    def append_write_audit(self, job_id: str, record: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO write_audit(ts, job_id, record) VALUES (?,?,?)",
                (utc_now_iso(), job_id, json.dumps(record, default=str)),
            )
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()
