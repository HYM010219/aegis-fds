#!/usr/bin/env bash
# ==============================================================================
# Aegis-FDS App Node Soak Logger (K3s Pod Resources)
# Runs every 5 minutes via cron or background daemon
# Output: ~/app_soak_stats.csv
# ==============================================================================
set -euo pipefail

CSV_FILE="${HOME}/app_soak_stats.csv"

# Write header if file does not exist
if [ ! -f "${CSV_FILE}" ]; then
  echo "timestamp,namespace,pod,cpu_cores,memory_bytes" > "${CSV_FILE}"
fi

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Detect k3s kubectl
K8S="sudo k3s kubectl"
if command -v kubectl >/dev/null 2>&1; then
  K8S="kubectl"
fi

${K8S} top pods -n aegis --no-headers 2>/dev/null | while read -r pod cpu mem; do
  echo "${TIMESTAMP},aegis,${pod},${cpu},${mem}" >> "${CSV_FILE}"
done
