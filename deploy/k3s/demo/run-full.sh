#!/usr/bin/env bash
# Rehearsal: recovery demo, then fresh detection bots and read-only STR verification.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"
K=(sudo k3s kubectl)
mkdir -p .local/demo-results
status=0
bash deploy/k3s/demo/run.sh || status=$?
"${K[@]}" -n aegis logs job/recovery-demo > .local/demo-results/recovery-result.log
if [[ "$status" != 0 ]]; then
  echo 'Recovery test failed. Saved .local/demo-results/recovery-result.log; detection phase not started.' >&2
  exit "$status"
fi
POD=$("${K[@]}" -n aegis get pods -l app=gateway -o jsonpath='{.items[0].metadata.name}')
"${K[@]}" -n aegis exec "$POD" -- mkdir -p /tmp/aegis-demo/bots
"${K[@]}" -n aegis cp bots/run_bots.py "$POD:/tmp/aegis-demo/bots/run_bots.py"
"${K[@]}" -n aegis cp deploy/k3s/demo/verify-crosscheck.py "$POD:/tmp/aegis-demo/verify-crosscheck.py"
SINCE=$("${K[@]}" -n aegis exec "$POD" -- python -c 'from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat())')
"${K[@]}" -n aegis exec "$POD" -- env BOT_BASE_URL=http://gateway:8000 \
  python /tmp/aegis-demo/bots/run_bots.py --once --only L2 L6 L7 --slow 1
"${K[@]}" -n aegis exec "$POD" -- python /tmp/aegis-demo/verify-crosscheck.py \
  --wallet-prefix BOT- --since "$SINCE" --rules L2 L6 L7 --timeout 180
echo 'Fresh L2/L6/L7 detail and STR recompute verified. Open the printed case IDs in the FDS console.'
