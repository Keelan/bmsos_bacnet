#!/usr/bin/env bash
# Upgrade edge-agent from PIP_INSTALL_SPEC (see .install-source) and sync
# SOFTWARE_VERSION in .env to the installed package version (heartbeat).
#
# Install copies this to /opt/edge-agent/update-edge-agent.sh — keep in sync.
set -euo pipefail
# shellcheck disable=SC1091
source /opt/edge-agent/.install-source
: "${PIP_INSTALL_SPEC:?missing PIP_INSTALL_SPEC in .install-source}"
/opt/edge-agent/.venv/bin/pip install --upgrade "$PIP_INSTALL_SPEC"
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
