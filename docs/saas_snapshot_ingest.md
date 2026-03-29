# SaaS ingest: `snapshot_network` job result `data` payload

The edge agent POSTs job results to **`POST /api/edge/v1/jobs/{job_public_id}/result`**. For **`snapshot_network`**, the JSON field **`data`** is the snapshot document below (also stored under `bacnet_snapshots.raw_data.data` if you wrap like Laravel’s `{ result_status, data, errors }`).

## Top-level (`data`)

| Field | Type | Notes |
|-------|------|--------|
| `snapshot_format_version` | int | **`2`** from current agent; older agents omitted this (treat as `1`). |
| `snapshot_at` | string | ISO8601 UTC, e.g. `2026-03-29T12:00:00Z`. |
| `devices` | array | One element per discovered BACnet device. |

## Each `devices[]` element

Discovery fields (unchanged):

| Field | Type | Notes |
|-------|------|--------|
| `device_instance` | int | BACnet device instance number. |
| `address` | string | BACnet address string from I-Am (often `ip:port` for BACnet/IP). |
| `vendor_id` | int | I-Am vendor ID. |
| `max_apdu` | int | I-Am. |
| `segmentation` | string | I-Am segmentation enum name / string. |
| `last_seen_at` | string | ISO8601 when Who-Is ran. |

**Added / filled when readable** (Device object `read-property` on `("device", device_instance)`):

| Field | Type | Notes |
|-------|------|--------|
| `name` | string | Copy of BACnet **`object-name`** on the Device object (for list UIs). |
| `object_name` | string | Same as `name`. |
| `description` | string | Device **`description`**. |
| `location` | string | Device **`location`**. |
| `vendor_name` | string | Device **`vendor-name`** (string; distinct from I-Am `vendor_id`). |
| `model_name` | string | **`model-name`**. |
| `firmware_revision` | string | **`firmware-revision`**. |
| `application_software_version` | string | **`application-software-version`**. |
| `protocol_version` | int / string | **`protocol-version`** (as returned by stack). |

| Field | Type | Notes |
|-------|------|--------|
| `objects` | array | Child BACnet objects (see below). Extra keys on the device row may appear if you customize the agent; safe to stash in JSON metadata. |

## Snapshot read policy (edge agent)

- Point properties are chosen **per BACnet object type** so `errors[]` stays small: e.g. no `units` on binary / multistate / character-string objects; file, program, trend-log, event-enrollment, notification-class rows only request **`object-name`** and **`description`** (plus **`present-value`** for **calendar**).
- **`schedule`**: **`present-value`** is not read in bulk snapshots (BACpypes often returns constructed data that does not JSON cleanly); **`object_name`** / **`description`** still appear.
- **`reliability`**: Always read for **analog-input** / **analog-output**; for **analog-value**, binary, multistate, character-string, **loop**, and unknown types the agent tries an **optional** read and only adds the field when the device answers (failures are not logged as errors).
- **`to_json_safe`**: Any value that would become a Python `repr` string (e.g. `<… object at 0x…>`) is emitted as JSON **`null`** instead.

## Each `devices[].objects[]` element

Core (existing):

| Field | Type | Notes |
|-------|------|--------|
| `object_type` | string | BACnet type: camelCase (`multiStateValue`) or kebab-case (`multi-state-value`, `binary-input`) depending on stack; SaaS should not assume one spelling. |
| `object_instance` | int | |
| `object_name` | string? | |
| `description` | string? | |
| `units` | string? | Often engineering-units enum name. |
| `present_value` | any | Primitive from BACnet (float, int, enum, etc.). JSON-serialized by agent (`to_json_safe`). |
| `status_flags` | any? | |
| `out_of_service` | bool? | |
| `reliability` | string? | |

**Binary** (`object_type` starts with `binary`, case-insensitive):

| Field | Type | Notes |
|-------|------|--------|
| `active_text` | string? | BACnet **`active-text`**. |
| `inactive_text` | string? | BACnet **`inactive-text`**. |
| `present_value_label` | string? | Resolved label, e.g. inactive → `inactive_text`, active → `active_text`. |

**Multistate** (`object_type` starts with `multiState`):

| Field | Type | Notes |
|-------|------|--------|
| `number_of_states` | int? | **`number-of-states`**. |
| `state_text` | string[] | One label per state, index `0` = state `1`, etc. (BACnet **`state-text`** array). |
| `present_value_label` | string? | `state_text[present_value - 1]` when in range. |

## API / auth (unchanged)

- **`POST /api/edge/v1/heartbeat`**
- **`POST /api/edge/v1/config`**
- **`POST /api/edge/v1/jobs/next`**
- **`POST /api/edge/v1/jobs/{job_id}/result`**

Headers: **`Authorization: Bearer {token}`**, **`Accept: application/json`**, **`Content-Type: application/json`**.

Job envelope fields (unchanged): `job_id` / `job_public_id` aliases on **next** response; result body uses your existing schema (`status`, `summary`, `data`, `errors`, timestamps, etc.).

## Laravel `IngestBacnetSnapshotService` (this repo’s `baapp`)

- Maps **`present_value_label`** into **`present_value_text`** when present (for table display).
- Puts **`active_text`**, **`inactive_text`**, **`states`** (from **`state_text`**), **`number_of_states`** into **`bacnet_objects.metadata`** via `presentValueHintSlice`.
- Device scalars not mapped to columns (e.g. `location`, `model_name`) remain in **`bacnet_devices.metadata`**.
