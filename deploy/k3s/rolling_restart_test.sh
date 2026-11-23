#!/usr/bin/env bash
# ==============================================================================
# AEGIS-FDS Zero-Downtime Rolling Update Verification Script
# Tests Gateway availability during `kubectl rollout restart deployment/gateway`
# Target: 0 failed requests (100% availability during rolling deployment)
# ==============================================================================
set -euo pipefail

TARGET_URL="${1:-http://localhost/ready}"
DURATION_SEC="${2:-30}"
REQ_PER_SEC="${3:-10}"
NAMESPACE="${NAMESPACE:-aegis}"
DEPLOYMENT="${DEPLOYMENT:-gateway}"

# Detect kubectl or k3s kubectl
if command -v kubectl >/dev/null 2>&1; then
  K8S="kubectl"
elif command -v k3s >/dev/null 2>&1; then
  K8S="sudo k3s kubectl"
else
  echo "Error: Neither kubectl nor k3s found." >&2
  exit 1
fi

echo "===================================================================="
echo " Starting Zero-Downtime Rolling Restart Test"
echo " Target URL   : ${TARGET_URL}"
echo " Duration     : ${DURATION_SEC}s"
echo " Target Rate  : ${REQ_PER_SEC} req/s"
echo " Deployment   : ${NAMESPACE}/${DEPLOYMENT}"
echo " K8s Command  : ${K8S}"
echo "===================================================================="

TOTAL_REQ=0
SUCCESS_REQ=0
FAILED_REQ=0
TMP_RES=$(mktemp)

# Background sender loop
send_requests() {
  local end_time=$((SECONDS + DURATION_SEC))
  local delay
  delay=$(awk "BEGIN {print 1.0 / ${REQ_PER_SEC}}")

  while [ $SECONDS -lt $end_time ]; do
    status=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 2 --max-time 3 "${TARGET_URL}" || echo "000")
    echo "${status}" >> "${TMP_RES}"
    sleep "${delay}"
  done
}

# Start sending requests in background
send_requests &
SENDER_PID=$!
echo "Traffic generator started (PID: ${SENDER_PID})"

# Wait 3 seconds for baseline traffic to establish
sleep 3

echo "Triggering rolling restart..."
${K8S} rollout restart deployment/"${DEPLOYMENT}" -n "${NAMESPACE}"

echo "Waiting for rollout to finish..."
${K8S} rollout status deployment/"${DEPLOYMENT}" -n "${NAMESPACE}" --timeout=60s

echo "Rollout completed. Waiting for traffic generation to conclude..."
wait "${SENDER_PID}" || true

# Compute statistics
TOTAL_REQ=$(wc -l < "${TMP_RES}")
SUCCESS_REQ=$(grep -c -E "^(200|301|302|401|403)$" "${TMP_RES}" || true)
FAILED_REQ=$(grep -c -E "^(000|500|502|503|504)$" "${TMP_RES}" || true)
rm -f "${TMP_RES}"

echo "===================================================================="
echo " Test Results"
echo " Total Requests  : ${TOTAL_REQ}"
echo " Successful (OK) : ${SUCCESS_REQ}"
echo " Failed (5xx/Err): ${FAILED_REQ}"
echo "===================================================================="

if [ "${FAILED_REQ}" -eq 0 ] && [ "${TOTAL_REQ}" -gt 0 ]; then
  echo ">>> SUCCESS: Zero-downtime rolling restart verified! 0 failed requests."
  exit 0
else
  echo ">>> FAILURE: ${FAILED_REQ} requests failed during deployment."
  exit 1
fi
