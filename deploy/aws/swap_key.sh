#!/usr/bin/env bash
# ==============================================================================
# swap_key.sh — Aegis-FDS Alchemy API Key Hot-Swap Script
# Usage:
#   bash swap_key.sh <N>
# Example:
#   bash swap_key.sh 2   # Switches to ALCHEMY_KEY_2 defined in .env
#
# Actions:
#   1. Validates ALCHEMY_KEY_<N> in .env
#   2. Backs up .env with timestamped filename
#   3. Updates ALCHEMY_WSS_URL in .env
#   4. Gracefully restarts aegis-collector (systemd / process) and aegis-gateway
#      without affecting Kafka, Spark, ClickHouse, Mongo, or Redis
#   5. Logs masked key rotation event to ~/soak_events.log
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
LOG_FILE="$HOME/soak_events.log"

if [ $# -lt 1 ]; then
    echo "=================================================================="
    echo "Usage: bash $0 <key_number>"
    echo "Example: bash $0 2"
    echo "  (Requires ALCHEMY_KEY_<N> defined in $ENV_FILE)"
    echo "=================================================================="
    exit 1
fi

KEY_NUM="$1"
TARGET_VAR="ALCHEMY_KEY_${KEY_NUM}"

if [ ! -f "$ENV_FILE" ]; then
    echo "[ERROR] .env file not found at $ENV_FILE"
    exit 1
fi

# 1. Read target key value from .env
RAW_KEY=$(grep "^${TARGET_VAR}=" "$ENV_FILE" | cut -d'=' -f2- | tr -d '\r' | tr -d '"' | tr -d "'" || true)

if [ -z "$RAW_KEY" ]; then
    echo "[ERROR] ${TARGET_VAR} is not set or empty in $ENV_FILE"
    echo "        Please add '${TARGET_VAR}=<your_alchemy_key_or_url>' to .env first."
    exit 1
fi

# Formulate complete WSS URL
if [[ "$RAW_KEY" =~ ^wss:// ]]; then
    NEW_WSS_URL="$RAW_KEY"
else
    NEW_WSS_URL="wss://eth-mainnet.g.alchemy.com/v2/${RAW_KEY}"
fi

# Mask key for secure logging
KEY_LEN=${#RAW_KEY}
if [ "$KEY_LEN" -gt 10 ]; then
    MASKED_KEY="${RAW_KEY:0:6}...${RAW_KEY: -4}"
else
    MASKED_KEY="***"
fi

echo "=================================================================="
echo ">>> [1/4] Backing up .env"
echo "=================================================================="
BACKUP_FILE="${ENV_FILE}.bak.$(date +%Y%m%d_%H%M%S)"
cp -p "$ENV_FILE" "$BACKUP_FILE"
echo "Backup saved to: $BACKUP_FILE"

echo "=================================================================="
echo ">>> [2/4] Updating ALCHEMY_WSS_URL in .env"
echo "=================================================================="
if grep -q "^ALCHEMY_WSS_URL=" "$ENV_FILE"; then
    sed -i "s|^ALCHEMY_WSS_URL=.*|ALCHEMY_WSS_URL=${NEW_WSS_URL}|" "$ENV_FILE"
else
    echo "ALCHEMY_WSS_URL=${NEW_WSS_URL}" >> "$ENV_FILE"
fi
echo "ALCHEMY_WSS_URL updated to Key #${KEY_NUM} (${MASKED_KEY})"

echo "=================================================================="
echo ">>> [3/4] 수집기 재시작 (수집기만 재시작, 수 초 공백 발생, Kafka/Spark 등 무중단)"
echo "=================================================================="

RESTART_START=$(date +%s)
RESTARTED_COLLECTOR=false
if command -v systemctl &>/dev/null && systemctl is-active --quiet aegis-collector 2>/dev/null; then
    echo "Restarting aegis-collector via systemd..."
    sudo systemctl restart aegis-collector
    RESTARTED_COLLECTOR=true
    echo "aegis-collector systemd service restarted."
else
    # Check for standalone python collector process
    COLLECTOR_PIDS=$(pgrep -f "collector_alchemy.py" || true)
    if [ -n "$COLLECTOR_PIDS" ]; then
        echo "Found running collector process PID(s): $COLLECTOR_PIDS"
        echo "Stopping old collector process..."
        kill -15 $COLLECTOR_PIDS 2>/dev/null || true
        sleep 2
        echo "[INFO] Collector process terminated. If running under a loop/screen/tmux, it will reload .env."
        RESTARTED_COLLECTOR=true
    else
        echo "[INFO] No active collector_alchemy process or systemd service detected."
    fi
fi
RESTART_END=$(date +%s)
GAP_SECONDS=$((RESTART_END - RESTART_START))
echo ">> 수집기 재시작 소요 시간: 약 ${GAP_SECONDS}초 (수집 공백 발생, Kafka/Spark 등 데이터 파이프라인 무중단 유지)"

# B. Restart Gateway container if running (ensures config consistency, zero downtime on Kafka/Spark/Mongo)
if docker ps --format '{{.Names}}' | grep -q "^aegis-gateway$"; then
    echo "Restarting aegis-gateway container..."
    docker compose -f "$PROJECT_ROOT/docker-compose.aws.yml" restart gateway
    echo "aegis-gateway restarted successfully."
fi

echo "=================================================================="
echo ">>> [4/4] Logging rotation event to soak_events.log"
echo "=================================================================="
TIMESTAMP_UTC=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
TIMESTAMP_KST=$(TZ="Asia/Seoul" date +"%Y-%m-%d %H:%M:%S KST" 2>/dev/null || date +"%Y-%m-%d %H:%M:%S")

EVENT_LINE="[${TIMESTAMP_UTC} | ${TIMESTAMP_KST}] SWAP_KEY: Switched to Key #${KEY_NUM} (${MASKED_KEY}) | Collector restart gap: ~${GAP_SECONDS}s (Kafka/Spark pipelines untouched) | Backup: $(basename "$BACKUP_FILE")"
echo "$EVENT_LINE" | tee -a "$LOG_FILE"

echo "=================================================================="
echo " Alchemy Key Swap Complete! (Key #${KEY_NUM})"
echo " (수집기만 재시작 완료: 공백 약 ${GAP_SECONDS}초, Kafka/Spark/Mongo 등 무중단)"
echo "=================================================================="
