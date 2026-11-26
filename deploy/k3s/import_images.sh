#!/usr/bin/env bash
# ==============================================================================
# Aegis-FDS K3s Image Import Script (App Node)
# Saves images built via Docker Compose and imports directly into K3s containerd
# ==============================================================================
set -euo pipefail

IMAGES=(
  "aegis-gateway:latest"
  "aegis-spark:latest"
  "aegis-collector:latest"
  "nginx:1.27-alpine"
)
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

echo "=== [1/2] Saving and tagging built images ==="
# Map docker compose built images if needed
docker tag aegis-fds-gateway:latest aegis-gateway:latest 2>/dev/null || true
docker tag aegis-fds-spark:latest aegis-spark:latest 2>/dev/null || true
# Collector is not in docker compose: build it here (build context = repo root)
docker build -f "${REPO_ROOT}/collector/Dockerfile" -t aegis-collector:latest "${REPO_ROOT}"

for img in "${IMAGES[@]}"; do
  if docker image inspect "${img}" >/dev/null 2>&1; then
    echo "Exporting and importing: ${img}..."
    docker save "${img}" | sudo k3s ctr images import -
  else
    echo "Warning: Image ${img} not found locally in Docker daemon."
  fi
done

echo "=== [2/2] Verifying imported images in K3s containerd ==="
sudo k3s ctr images ls | grep -E "aegis|nginx"
echo "Image import completed successfully."
