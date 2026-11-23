#!/usr/bin/env bash
# ==============================================================================
# Aegis-FDS Data Node Soak Logger (Docker Compose Containers)
# Runs every 5 minutes via cron or background daemon
# Output: ~/data_soak_stats.csv
# ==============================================================================
set -euo pipefail

CSV_FILE="${HOME}/data_soak_stats.csv"

# Write header if file does not exist
if [ ! -f "${CSV_FILE}" ]; then
  echo "timestamp,container_id,container_name,cpu_percent,mem_usage,mem_limit,net_io,block_io" > "${CSV_FILE}"
fi

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

docker stats --no-stream --format \
  "${TIMESTAMP},{{.ID}},{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}}" >> "${CSV_FILE}"
