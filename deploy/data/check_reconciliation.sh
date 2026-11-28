#!/usr/bin/env bash
# ==============================================================================
# Aegis-FDS Data Node Reconciliation Verification Script
# Compares Mongo transfers vs ClickHouse transfers & checks Outbox lag
# ==============================================================================
set -euo pipefail

echo "===================================================================="
echo " Aegis-FDS Data Reconciliation Check ($(date '+%Y-%m-%d %H:%M:%S'))"
echo "===================================================================="

# 1. MongoDB ACCEPTED transfer count
MONGO_COUNT=$(docker exec aegis-mongo mongosh --quiet --eval '
  db.getSiblingDB("aegis").transfers.countDocuments({status: "ACCEPTED"})
' | tr -d '[:space:]')

# 2. MongoDB Outbox pending count
OUTBOX_PENDING=$(docker exec aegis-mongo mongosh --quiet --eval '
  db.getSiblingDB("aegis").outbox_events.countDocuments({publish_state: "PENDING"})
' | tr -d '[:space:]')

# 3. ClickHouse ACCEPTED transfer count
CH_COUNT=$(docker exec aegis-clickhouse clickhouse-client --query '
  SELECT count() FROM aegis.transfers WHERE status = "ACCEPTED"
' | tr -d '[:space:]')

echo "MongoDB transfers (ACCEPTED) : ${MONGO_COUNT}"
echo "ClickHouse transfers (ACCEPTED): ${CH_COUNT}"
echo "MongoDB Outbox PENDING events : ${OUTBOX_PENDING}"
echo "--------------------------------------------------------------------"

DIFF=$((MONGO_COUNT - CH_COUNT))
echo "Discrepancy (Mongo - ClickHouse): ${DIFF}"

if [ "${DIFF}" -eq 0 ] && [ "${OUTBOX_PENDING}" -eq 0 ]; then
  echo ">>> STATUS: PERFECT RECONCILIATION (0 lag, 0 dropped events)"
elif [ "${OUTBOX_PENDING}" -gt 0 ]; then
  echo ">>> STATUS: IN-FLIGHT (Outbox is actively publishing ${OUTBOX_PENDING} pending events)"
else
  echo ">>> STATUS: DISCREPANCY DETECTED (${DIFF} events mismatch)"
fi
echo "===================================================================="
