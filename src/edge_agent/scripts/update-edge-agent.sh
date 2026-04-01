#!/usr/bin/env bash
# Upgrade edge-agent from PIP_INSTALL_SPEC (see /opt/edge-agent/.install-source) and sync
# SOFTWARE_VERSION in .env. After pip, this file is refreshed from the installed wheel so you
# do not need to manually copy deploy/update-edge-agent.sh on each release.
#
# If this copy is stale (older than the wheel) but pip already upgraded the package, refresh once:
#   /opt/edge-agent/.venv/bin/python -c "from importlib.resources import files; from pathlib import Path; p=files('edge_agent')/'scripts'/'update-edge-agent.sh'; Path('/opt/edge-agent/update-edge-agent.sh').write_bytes(p.read_bytes())" && chmod 700 /opt/edge-agent/update-edge-agent.sh
set -euo pipefail
# shellcheck disable=SC1091
source /opt/edge-agent/.install-source
: "${PIP_INSTALL_SPEC:?missing PIP_INSTALL_SPEC in .install-source}"
# --force-reinstall: same pyproject version from git can otherwise skip replacing site-packages.
/opt/edge-agent/.venv/bin/pip install --upgrade --force-reinstall "$PIP_INSTALL_SPEC"

/opt/edge-agent/.venv/bin/python <<'PY'
from importlib.resources import files
from pathlib import Path

src = files("edge_agent") / "scripts" / "update-edge-agent.sh"
dest = Path("/opt/edge-agent/update-edge-agent.sh")
dest.write_bytes(src.read_bytes())
PY
chmod 700 /opt/edge-agent/update-edge-agent.sh

VER="$(/opt/edge-agent/.venv/bin/python -c 'from importlib.metadata import version; print(version("edge-agent"))')"
ENV_FILE="/opt/edge-agent/.env"
if [[ -f "$ENV_FILE" ]]; then
  if grep -q '^SOFTWARE_VERSION=' "$ENV_FILE"; then
    sed -i "s/^SOFTWARE_VERSION=.*/SOFTWARE_VERSION=${VER}/" "$ENV_FILE"
  else
    printf '\nSOFTWARE_VERSION=%s\n' "$VER" >>"$ENV_FILE"
  fi
fi
systemctl restart edge-agent
