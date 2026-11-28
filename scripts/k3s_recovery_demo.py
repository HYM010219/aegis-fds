"""게이트웨이에 부하를 주면서 파드 하나를 종료하고 자동 복구를 확인한다.

    클러스터 안에서 실행하며 파드별 처리 건수·응답 시간·복구 시간을 기록한다.
    요청은 재시도하지 않아 일시 실패도 결과에 남긴다. 이상 데이터 주입 봇은 별도로 실행한다.
"""
import collections
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import ssl
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, build_opener, ProxyHandler, HTTPSHandler


def ready(pod):
    return not pod['metadata'].get('deletionTimestamp') and any(
        c['type'] == 'Ready' and c['status'] == 'True'
        for c in pod.get('status', {}).get('conditions', []))


def outcome(code, body):
    if code in (200, 201) and body.get('status') == 'ACCEPTED':
        return 'accepted'
    # Only a documented FDS response counts as business rejection, not proxy throttling.
    reason = body.get('reason') or ''
    if not isinstance(reason, str):
        reason = ''
    if body.get('status') == 'REJECTED' and (
        (code == 429 and reason.startswith('TEMP_HOLD_')) or
        (code == 403 and (reason.startswith('FROZEN_') or reason in (
                         'BLACKLISTED_SENDER', 'BLACKLISTED_RECEIVER')))):
        return 'fds_rejected'
    return 'failure'


class Kube:
    def __init__(self):
        self.base = 'https://kubernetes.default.svc'
        self.namespace = os.getenv('POD_NAMESPACE', 'aegis')
        self.root = Path('/var/run/secrets/kubernetes.io/serviceaccount')
        self.http = build_opener(ProxyHandler({}), HTTPSHandler(
            context=ssl.create_default_context(cafile=str(self.root / 'ca.crt'))))

    def call(self, path, method='GET', body=None):
        request = Request(self.base + path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization': 'Bearer ' + (self.root / 'token').read_text().strip(),
                     'Content-Type': 'application/json'})
        with self.http.open(request, timeout=5) as response:
            return json.load(response)

    def pods(self):
        return self.call(f'/api/v1/namespaces/{self.namespace}/pods?labelSelector=app%3Dgateway')['items']

    def delete(self, pod):
        # UID precondition prevents deleting a different pod after a name race.
        return self.call(f'/api/v1/namespaces/{self.namespace}/pods/' +
            quote(pod['metadata']['name'], safe=''), 'DELETE',
            {'apiVersion': 'v1', 'kind': 'DeleteOptions',
             'preconditions': {'uid': pod['metadata']['uid']}})


class Demo:
    def __init__(self, kube, output, rps=10, duration=90, fault_after=15, workers=16):
        if not (1 <= rps <= 100 and 1 <= workers <= 64 and
                5 <= fault_after < duration <= 600):
            raise ValueError('Require rps 1..100, workers 1..64, 5 <= fault_after < duration <= 600')
        self.kube, self.output = kube, Path(output)
        self.rps, self.duration, self.fault_after, self.workers = rps, duration, fault_after, workers
        self.target = f'http://gateway.{kube.namespace}.svc:8000/api/v1/transfers'
        self.api_key = os.environ['GATEWAY_API_KEY']
        self.run_id = uuid.uuid4().hex[:12]
        self.lock = threading.Lock()
        self.counts = collections.Counter()
        self.latencies = []
        self.samples = []
        self.pod_events = []
        self.baseline_names = set()
        self.original_names = set()
        self.victim_name = None
        self.state = dict(ready_pods=0, fault_injected=0, recovered=0, recovery_seconds=0, finished=0)
        self.error = None
        self.elapsed_seconds = 0
        self.started_monotonic = None
        self.fault_monotonic = None
        self.recovered_monotonic = None
        self.fault_requested_at = None
        self.fault_acknowledged_at = None
        self.http = build_opener(ProxyHandler({}))

    def request(self, index):
        started = time.monotonic()
        started_at = datetime.now(timezone.utc).isoformat()
        wallet = f'BOT-LOAD-{self.run_id}-{index}'
        payload = {'tx_hash': wallet, 'log_index': 0, 'from': wallet,
                   'to': wallet + '-TO', 'amount': 1, 'block': index,
                   'timestamp': datetime.now(timezone.utc).isoformat()}
        request = Request(self.target, data=json.dumps(payload).encode(),
            headers={'Content-Type': 'application/json', 'X-API-Key': self.api_key,
                     'Connection': 'close'})
        code, body, pod_name, error_type = 0, {}, '', None
        try:
            with self.http.open(request, timeout=3) as response:
                code = response.status
                pod_name = response.headers.get('X-Gateway-Pod', '')
                body = json.load(response)
        except HTTPError as exc:
            code = exc.code
            pod_name = exc.headers.get('X-Gateway-Pod', '')
            try:
                body = json.loads(exc.read())
            except (OSError, ValueError, http.client.HTTPException) as read_error:
                error_type = type(read_error).__name__
        except (OSError, URLError, ValueError, http.client.HTTPException) as exc:
            error_type = type(exc).__name__
        if not isinstance(body, dict):
            body = {}
        result = 'failure' if error_type else outcome(code, body)
        finished = time.monotonic()
        finished_at = datetime.now(timezone.utc).isoformat()
        elapsed = finished - started
        with self.lock:
            if self.fault_monotonic is None or finished < self.fault_monotonic:
                phase = 'baseline'
            elif started < self.fault_monotonic <= finished:
                phase = 'boundary'
            elif self.recovered_monotonic is not None and finished >= self.recovered_monotonic:
                phase = 'recovered'
            else:
                phase = 'recovering'
            self.counts[result] += 1
            self.latencies.append(elapsed)
            self.samples.append(dict(index=index, phase=phase, code=code, result=result,
                                     seconds=elapsed, tx_hash=wallet,
                                     pod_name=pod_name, started_at=started_at,
                                     finished_at=finished_at,
                                     started_seconds=started - self.started_monotonic,
                                     finished_seconds=finished - self.started_monotonic,
                                     error_type=error_type))

    def snapshot(self, pods):
        ready_uids = {p['metadata']['uid'] for p in pods if ready(p)}
        with self.lock:
            self.state['ready_pods'] = len(ready_uids)
            self.pod_events.append({'at': datetime.now(timezone.utc).isoformat(),
                'pods': [{'name': p['metadata']['name'], 'uid': p['metadata']['uid'],
                          'ready': ready(p)} for p in pods]})
        return ready_uids

    def monitor(self, baseline_ready, original, stop, abort):
        """별도 스레드에서 파드 상태를 조회하고 한 번만 장애를 발생시킨다."""
        try:
            while not stop.wait(1):
                pods = self.kube.pods()
                if stop.is_set():
                    return
                current = self.snapshot(pods)
                now = time.monotonic()
                if self.fault_monotonic is None and now - self.started_monotonic >= self.fault_after:
                    with self.lock:
                        served = {s['pod_name'] for s in self.samples
                                  if s['phase'] == 'baseline' and s['result'] == 'accepted'}
                        healthy = (self.counts['failure'] == 0 and self.baseline_names <= served
                                   and all(s['pod_name'] for s in self.samples))
                    if len(current) < 2 or not healthy:
                        raise RuntimeError('Baseline unhealthy; no fault injected')
                    victims = [p for p in pods if ready(p) and p['metadata']['uid'] in original]
                    if not victims:
                        raise RuntimeError('Original pods changed before fault; abort')
                    with self.lock:
                        self.victim_name = victims[0]['metadata']['name']
                        self.fault_monotonic = time.monotonic()
                        self.fault_requested_at = datetime.now(timezone.utc).isoformat()
                    self.kube.delete(victims[0])
                    with self.lock:
                        self.fault_acknowledged_at = datetime.now(timezone.utc).isoformat()
                        self.state['fault_injected'] = 1
                    print(json.dumps({'event': 'fault_injected', 'pod': self.victim_name}), flush=True)
                elif self.fault_monotonic is not None and not self.state['recovered']:
                    if len(current) >= len(baseline_ready) and current - original:
                        with self.lock:
                            self.recovered_monotonic = time.monotonic()
                            self.state['recovered'] = 1
                            self.state['recovery_seconds'] = self.recovered_monotonic - self.fault_monotonic
                        print(json.dumps({'event': 'replacement_ready', **self.state}), flush=True)
        except Exception as exc:
            with self.lock:
                self.error = type(exc).__name__ + ': ' + str(exc)
            abort.set()

    def run(self):
        run_started = time.monotonic()
        stop, abort = threading.Event(), threading.Event()
        monitor = None
        try:
            original_pods = self.kube.pods()
            baseline_ready = self.snapshot(original_pods)
            original = {p['metadata']['uid'] for p in original_pods}
            self.baseline_names = {p['metadata']['name'] for p in original_pods if ready(p)}
            self.original_names = {p['metadata']['name'] for p in original_pods}
            if len(baseline_ready) < 2:
                raise RuntimeError('Need at least two Ready Gateway pods; no fault injected')
            started = time.monotonic()
            self.started_monotonic = started
            next_request, index = started, 0
            pending = set()
            monitor = threading.Thread(target=self.monitor, args=(baseline_ready, original, stop, abort))
            monitor.start()
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as pool:
                while time.monotonic() - started < self.duration and not abort.is_set():
                    now = time.monotonic()
                    for future in list(pending):
                        if future.done():
                            future.result()
                            pending.remove(future)
                    if now >= next_request:
                        if len(pending) < self.workers:
                            pending.add(pool.submit(self.request, index))
                            index += 1
                        else:
                            with self.lock:
                                self.counts['load_skipped'] += 1
                        next_request = now + 1 / self.rps  # Never burst to catch up.
                    time.sleep(0.01)
                for future in pending:
                    future.result()
        except Exception as exc:
            self.error = type(exc).__name__ + ': ' + str(exc)
        finally:
            stop.set()
            if monitor is not None:
                monitor.join()
            self.elapsed_seconds = time.monotonic() - run_started
            with self.lock:
                self.state['finished'] = 1
            self.output.mkdir(parents=True, exist_ok=True)
            report = self.report()
            (self.output / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
            (self.output / 'requests.json').write_text(json.dumps(self.samples), encoding='utf-8')
            (self.output / 'pods.json').write_text(json.dumps(self.pod_events), encoding='utf-8')
            print(json.dumps(report), flush=True)

    def report(self):
        with self.lock:
            values = sorted(self.latencies)
            accepted = sorted(s['seconds'] for s in self.samples if s['result'] == 'accepted')
            total = sum(self.counts[k] for k in ('accepted', 'fds_rejected', 'failure'))
            pod_counts = {}
            phase_counts = {phase: dict(requests=0, accepted=0, fds_rejected=0, failure=0)
                            for phase in ('baseline', 'boundary', 'recovering', 'recovered')}
            for sample in self.samples:
                phase_counts[sample['phase']]['requests'] += 1
                phase_counts[sample['phase']][sample['result']] += 1
                key = sample['pod_name'] or 'unknown'
                counts = pod_counts.setdefault(key, dict(pod_name=sample['pod_name'],
                    requests=0, accepted=0, fds_rejected=0, failure=0, phases={}))
                counts['requests'] += 1
                counts[sample['result']] += 1
                counts['phases'][sample['phase']] = counts['phases'].get(sample['phase'], 0) + 1
            served_baseline = {s['pod_name'] for s in self.samples
                               if s['phase'] == 'baseline' and s['result'] == 'accepted'}
            distribution = bool(self.baseline_names) and self.baseline_names <= served_baseline
            survivor = any(s['phase'] == 'recovering' and s['result'] == 'accepted' and
                           s['pod_name'] in self.baseline_names - {self.victim_name} for s in self.samples)
            replacement_served = any(s['phase'] == 'recovered' and s['result'] == 'accepted' and
                                     s['pod_name'] and s['pod_name'] not in self.original_names for s in self.samples)
            continuity = (bool(self.state['recovered']) and total > 0 and
                          self.counts['failure'] == 0 and self.counts['load_skipped'] == 0 and
                          total >= self.rps * self.duration * 0.8 and
                          distribution and survivor and replacement_served and
                          all(s['pod_name'] for s in self.samples))
            return dict(run_id=self.run_id, target_rps=self.rps, duration_seconds=self.duration,
                elapsed_seconds=self.elapsed_seconds,
                achieved_rps=total / self.elapsed_seconds if self.elapsed_seconds else 0,
                counts={k: self.counts[k] for k in ('accepted', 'fds_rejected', 'failure', 'load_skipped')},
                state=dict(self.state), error=self.error,
                pod_counts=pod_counts, baseline_distribution_pass=distribution,
                phase_counts=phase_counts,
                survivor_handled_during_recovery=survivor,
                replacement_handled_after_recovery=replacement_served,
                fault_requested_at=self.fault_requested_at,
                fault_acknowledged_at=self.fault_acknowledged_at,
                fault_requested_seconds=(self.fault_monotonic - self.started_monotonic
                                         if self.fault_monotonic is not None else None),
                fault_mode='graceful_pod_delete',
                p95_seconds=values[min(len(values)-1, int(len(values)*0.95))] if values else None,
                p95_accepted_seconds=accepted[min(len(accepted)-1, int(len(accepted)*0.95))] if accepted else None,
                request_continuity_pass=continuity and self.error is None,
                scope='Gateway HTTP responses and replacement readiness; not end-to-end DB integrity')

    def metrics(self):
        with self.lock:
            lines = [f'aegis_demo_{k} {v}' for k, v in self.state.items()]
            for result in ('accepted', 'fds_rejected', 'failure', 'load_skipped'):
                lines.append(f'aegis_demo_requests_total{{result="{result}"}} {self.counts[result]}')
            by_pod = collections.Counter((s['pod_name'] or 'unknown', s['result']) for s in self.samples)
            for (name, result), count in sorted(by_pod.items()):
                labels = f'gateway_pod={json.dumps(name)},result={json.dumps(result)}'
                lines.append(f'aegis_demo_pod_requests_total{{{labels}}} {count}')
            accepted = [s['seconds'] for s in self.samples if s['result'] == 'accepted']
            for metric, values in (('request', self.latencies), ('accepted_request', accepted)):
                for bound in (0.05, 0.1, 0.25, 0.5, 1, 2, 5, float('inf')):
                    label = '+Inf' if bound == float('inf') else str(bound)
                    lines.append(f'aegis_demo_{metric}_seconds_bucket{{le="{label}"}} ' +
                                 str(sum(v <= bound for v in values)))
                lines += [f'aegis_demo_{metric}_seconds_count {len(values)}',
                          f'aegis_demo_{metric}_seconds_sum {sum(values)}']
            return '\n'.join(lines) + '\n'


def main():
    demo = Demo(Kube(), '/results', rps=int(os.getenv('DEMO_RPS', '10')),
        duration=int(os.getenv('DEMO_DURATION', '90')),
        fault_after=int(os.getenv('DEMO_FAULT_AFTER', '15')))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/metrics':
                data, content_type = demo.metrics().encode(), 'text/plain; version=0.0.4'
            elif self.path == '/report.json':
                data, content_type = json.dumps(demo.report()).encode(), 'application/json'
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('0.0.0.0', 9109), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    demo.run()
    # Retain the metrics endpoint briefly for the final Grafana scrape.
    time.sleep(int(os.getenv('DEMO_METRICS_HOLD', '120')))
    server.shutdown()
    raise SystemExit(0 if demo.report()['request_continuity_pass'] else 1)


if __name__ == '__main__':
    main()
