#!/usr/bin/env bash
# One invocation on the App host. Requires the demo monitoring setup and app deployment.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"
K=(sudo k3s kubectl)
"${K[@]}" -n aegis get deployment gateway >/dev/null
"${K[@]}" -n monitoring get podmonitor recovery-demo >/dev/null
"${K[@]}" -n aegis patch deployment gateway --type=strategic --patch-file deploy/k3s/demo/gateway-recovery-patch.yaml
identity_enabled=false
cleanup() {
  result=$?
  trap - EXIT
  if [[ "$identity_enabled" == true ]]; then
    "${K[@]}" -n aegis set env deployment/gateway POD_IDENTITY_HEADER=0 || result=1
    "${K[@]}" -n aegis rollout status deployment/gateway --timeout=180s || result=1
  fi
  exit "$result"
}
trap cleanup EXIT
"${K[@]}" -n aegis set env deployment/gateway POD_IDENTITY_HEADER=1
identity_enabled=true
"${K[@]}" -n aegis rollout status deployment/gateway --timeout=180s
"${K[@]}" -n aegis create configmap recovery-demo-script \
  --from-file=scripts/k3s_recovery_demo.py --dry-run=client -o yaml | "${K[@]}" apply -f -
# Replace only the previous demo Job. Its fault is not retried by Kubernetes.
"${K[@]}" -n aegis delete job recovery-demo --ignore-not-found --wait=true
"${K[@]}" apply -f deploy/k3s/demo/recovery-job.yaml
"${K[@]}" -n aegis wait --for=condition=Ready pod -l app=recovery-demo --timeout=120s
echo 'Demo started: 15s baseline, one graceful Gateway pod deletion, 90s traffic.'
echo 'View Grafana dashboard: Aegis Gateway recovery demo'
"${K[@]}" -n aegis logs -f job/recovery-demo
for attempt in {1..15}; do
  phase=$("${K[@]}" -n aegis get pods -l app=recovery-demo -o jsonpath='{.items[0].status.phase}')
  [[ "$phase" == Succeeded || "$phase" == Failed ]] && break
  sleep 1
done
if [[ "$phase" != Succeeded ]]; then
  echo "Demo did not succeed (pod phase: $phase). Inspect JSON summary above." >&2
  exit 1
fi
