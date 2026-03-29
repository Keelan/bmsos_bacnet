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

Set `BACNET_MOCK=false`, set `BACNET_BIND_IP` if needed, run on a host with BACnet/IP multicast to the target subnet.

## systemd

See [deploy/edge-agent.service](deploy/edge-agent.service); adjust paths and `EnvironmentFile`.
