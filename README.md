# BACnet edge agent (MVP)

Outbound-only agent: heartbeat, remote config, `jobs/next`, BACnet jobs, `result` POST.

## Quick local test (mock BACnet)

```bash
cd /path/to/bmsos_bacnet
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
pip install -e ".[dev]"

# Terminal A — fake SaaS
uvicorn fake_saas:app --host 127.0.0.1 --port 8765

# Terminal B — enqueue a job (after agent is running, or before)
curl -s -X POST http://127.0.0.1:8765/dev/enqueue-job \
  -H "Content-Type: application/json" \
  -d '{"job_type":"discover_network","payload":{}}' 

# Terminal C — agent
cp .env.example .env
python -m edge_agent
```

Inspect results: `curl -s http://127.0.0.1:8765/dev/results | jq`.

## Real BACnet (Linux)

- Set `BACNET_MOCK=false`.
- Set `BACNET_BIND_IP` to the interface IPv4 on the BACnet LAN.
- Set `BACNET_BIND_PREFIX` (e.g. `24`) so the stack binds as `ip/24:47808`. **Bare `ip:port` is treated as /32 by BACpypes3 and breaks Who-Is** (`RuntimeError: no broadcast`). Alternatively put CIDR in `BACNET_BIND_IP` (e.g. `192.168.1.5/24`).

## Update on a staging / production device

```bash
cd /opt/bmsos   # or your install path
git pull
source .venv/bin/activate
pip install -e .
```

Edit **`.env`** (never commit it):

- Add **`BACNET_BIND_PREFIX=24`** (or your real prefix) if `BACNET_BIND_IP` is only an address like `192.168.254.171`.
- Keep **`SAAS_BASE_URL`**, **`BOX_ID`**, **`API_TOKEN`**, etc.

Run:

```bash
python -m edge_agent
```

Or with systemd: `sudo systemctl restart edge-agent` after `git pull` + `pip install -e .`.

You no longer need to export **`BACPYPES_DEVICE_ADDRESS`** manually; the agent sets **`ip/prefix:port`** from `.env`.

## systemd

See [deploy/edge-agent.service](deploy/edge-agent.service); adjust paths and `EnvironmentFile`.
