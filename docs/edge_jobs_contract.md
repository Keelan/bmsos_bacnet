# Edge agent job contract (SaaS ↔ Python)

The agent polls **`POST /api/edge/v1/jobs/next`** and posts results to **`POST /api/edge/v1/jobs/{job_id}/result`**.

**Headers:** `Authorization: Bearer {token}`, `Accept: application/json`, `Content-Type: application/json`.

## Result envelope (agent → SaaS)

Laravel maps top-level **`status`** → **`result_status`** internally; the agent should send **`status`**.

| Field | Type | Notes |
|-------|------|--------|
| `job_id` | string (UUID) | Must match URL segment. |
| `status` | string | `success` \| `partial_success` \| `failed` |
| `started_at` | string | ISO8601 UTC |
| `finished_at` | string | ISO8601 UTC |
| `summary` | string | Human-readable |
| `data` | object | Job-specific JSON (may be `{}`) |
| `errors` | array | Objects with structured fields (see per-job notes) |

Duplicate POSTs for an already-terminal job should return **2xx** (idempotent) on the SaaS side.

---

## `discover_network`

**Payload:** `{}` (optional extra keys ignored).

**`data`:** `{ "discovered_at": "<ISO8601>", "devices": [ ... ] }` (device rows as today).

---

## `snapshot_network`

**Payload:** `{}`.

**`data`:** Full snapshot document per [saas_snapshot_ingest.md](saas_snapshot_ingest.md) (`snapshot_format_version`, `snapshot_at`, `devices[]` with `objects[]`).

---

## `read_device_live`

**Payload (snake_case):**

| Key | Required | Notes |
|-----|----------|--------|
| `device_instance` | yes | int |
| `max_objects` | no | Cap how many objects to read; default from agent env `READ_DEVICE_LIVE_MAX_OBJECTS` |
| `timeout_seconds` | no | Wall-clock budget inside the job; default `READ_DEVICE_LIVE_TIMEOUT_SECONDS` |
| `source` | no | SaaS traceability; agent ignores |

**`data` (single-device, not a full-network snapshot):**

| Key | Notes |
|-----|--------|
| `device_instance` | int |
| `read_at` | ISO8601 UTC |
| `objects` | Array of object rows matching snapshot `devices[].objects[]` semantics (see ingest doc) |
| `truncated` | optional bool — true if capped by `max_objects` or time |
| `total_object_count` | optional int — non-device objects on device before cap |
| `returned_object_count` | optional int — rows in `objects` |

**`errors`:** May include `device_instance`, `object_type`, `object_instance`, `message` (and optional `property` for per-property failures).

### Example result (`success`)

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "started_at": "2026-03-29T12:00:00Z",
  "finished_at": "2026-03-29T12:00:05Z",
  "summary": "Read 42 objects",
  "data": {
    "device_instance": 2001,
    "read_at": "2026-03-29T12:00:04Z",
    "objects": [
      {
        "object_type": "analogValue",
        "object_instance": 1,
        "object_name": "SAT",
        "present_value": 21.5,
        "units": "degreesCelsius",
        "out_of_service": false
      }
    ]
  },
  "errors": []
}
```

### Example result (`partial_success`)

Some property reads failed; `objects` may still contain partial rows.

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "partial_success",
  "started_at": "2026-03-29T12:00:00Z",
  "finished_at": "2026-03-29T12:00:06Z",
  "summary": "Read 10 objects",
  "data": {
    "device_instance": 2001,
    "read_at": "2026-03-29T12:00:05Z",
    "objects": []
  },
  "errors": [
    {
      "device_instance": 2001,
      "object_type": "analogValue",
      "object_instance": 99,
      "property": "present-value",
      "message": "unknownObject"
    }
  ]
}
```

### Example result (`failed`)

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "started_at": "2026-03-29T12:00:00Z",
  "finished_at": "2026-03-29T12:00:01Z",
  "summary": "device not found (I-Am)",
  "data": {
    "device_instance": 9999,
    "read_at": "2026-03-29T12:00:01Z",
    "objects": []
  },
  "errors": [
    {
      "device_instance": 9999,
      "message": "device not found (I-Am)"
    }
  ]
}
```

---

## `read_point`

**Payload:**

| Key | Required | Notes |
|-----|----------|--------|
| `device_instance` | yes | |
| `object_type` | yes | |
| `object_instance` | yes | |
| `property` | no | Default: present-value read (`presentValue`); kebab-case `present-value` accepted |
| `source` | no | Ignored by agent |

When **`property`** is omitted or is a present-value read, **`data`** is enriched: `present_value`, `read_at`, `units`, `out_of_service`, `reliability`, multistate/binary labels where applicable. Legacy keys **`value`** (same as `present_value`) and **`property`** are included for older parsers.

Other properties return a single **`value`** + **`property`** + **`read_at`**.

### Example (`success`, present-value enriched)

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "started_at": "2026-03-29T12:00:00Z",
  "finished_at": "2026-03-29T12:00:01Z",
  "summary": "Read OK",
  "data": {
    "device_instance": 2001,
    "object_type": "analogValue",
    "object_instance": 1,
    "property": "presentValue",
    "present_value": 21.5,
    "value": 21.5,
    "read_at": "2026-03-29T12:00:01Z",
    "units": "degreesCelsius",
    "out_of_service": false,
    "object_name": "SAT"
  },
  "errors": []
}
```

---

## `write_point`

**Payload:**

| Key | Required | Notes |
|-----|----------|--------|
| `device_instance` | yes | |
| `object_type` | yes | |
| `object_instance` | yes | |
| `value` | yes | Written to BACnet **present-value** only |
| `priority` | no | 1–16 or omit/null for stack default |
| `include_readback` | no | If true, agent reads present-value after a successful write |
| `source` | no | Ignored by agent |

Do **not** send `property` on `write_point` until both SaaS and agent support arbitrary property writes.

**Readback (canonical):** When `include_readback` is true and the write succeeds, **`data`** also contains:

| Key | Notes |
|-----|--------|
| `present_value_after` | JSON-safe value after write, or `null` if readback failed |
| `read_at` | ISO8601 UTC timestamp of the readback read |

No nested `readback` object.

### Example (`success`, with readback)

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "started_at": "2026-03-29T12:00:00Z",
  "finished_at": "2026-03-29T12:00:01Z",
  "summary": "Write OK",
  "data": {
    "device_instance": 2001,
    "object_type": "binaryValue",
    "object_instance": 2,
    "property": "presentValue",
    "value": 1,
    "priority": 8,
    "present_value_after": 1,
    "read_at": "2026-03-29T12:00:01Z"
  },
  "errors": []
}
```

### Example (`failed`)

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "started_at": "2026-03-29T12:00:00Z",
  "finished_at": "2026-03-29T12:00:01Z",
  "summary": "Write failed",
  "data": {
    "device_instance": 2001,
    "object_type": "analogValue",
    "object_instance": 1,
    "property": "presentValue",
    "value": 99.0,
    "priority": null,
    "error": "writeAccessDenied"
  },
  "errors": [{ "message": "writeAccessDenied" }]
}
```
