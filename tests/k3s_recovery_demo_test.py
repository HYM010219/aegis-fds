"""로컬 HTTP 서버와 모의 쿠버네티스 API로 요청 분산·파드 복구 판정을 확인한다."""
import importlib.util
import http.client
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('recovery_demo', ROOT / 'scripts/k3s_recovery_demo.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def pod(name, is_ready=True):
    return {'metadata': {'name': name, 'uid': name},
            'status': {'conditions': [{'type': 'Ready', 'status': str(is_ready)}]}}


class FakeKube:
    namespace = 'aegis'

    def __init__(self, replacement=True, ready_count=2):
        self.deleted = []
        self.replacement = replacement
        self.ready_count = ready_count

    def pods(self):
        if self.deleted and self.replacement:
            return [pod('gateway-b'), pod('gateway-c')]
        return [pod('gateway-a'), pod('gateway-b', self.ready_count == 2)]

    def delete(self, target):
        self.deleted.append(target['metadata']['uid'])


class RecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                cls.hashes.append(payload['tx_hash'])
                cls.connections.append((self.client_address, self.headers.get('Connection')))
                status = cls.status
                body = json.dumps({'status': 'ACCEPTED' if status == 200 else 'REJECTED',
                                   'reason': None if status == 200 else 'UNAUTHORIZED'}).encode()
                self.send_response(status)
                if cls.identity:
                    if not cls.kube.deleted:
                        name = 'gateway-a' if cls.one_sided or payload['block'] % 2 == 0 else 'gateway-b'
                    elif cls.demo.state['recovered'] and payload['block'] % 2 == 0:
                        name = 'gateway-c'
                    else:
                        name = 'gateway-b'
                    self.send_header('X-Gateway-Pod', name)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def run_demo(self, kube, status=200, identity=True, one_sided=False):
        type(self).status = status
        type(self).hashes = []
        type(self).connections = []
        type(self).identity = identity
        type(self).one_sided = one_sided
        type(self).kube = kube
        with tempfile.TemporaryDirectory() as output, patch.dict(os.environ, {'GATEWAY_API_KEY': 'test'}):
            demo = module.Demo(kube, output, rps=5, duration=8, fault_after=5)
            type(self).demo = demo
            demo.target = f'http://127.0.0.1:{self.server.server_port}/api/v1/transfers'
            demo.run()
            report = json.loads((Path(output) / 'report.json').read_text())
            self.assertEqual(report, demo.report())
            self.assertIn('aegis_demo_requests_total', demo.metrics())
            self.assertEqual(len(self.hashes), len(set(self.hashes)))
            self.assertTrue(all(connection == 'close' for _, connection in self.connections))
            self.assertEqual(len(self.connections), len({address for address, _ in self.connections}))
            samples = json.loads((Path(output) / 'requests.json').read_text())
            self.assertTrue(all('pod_uid' not in sample for sample in samples))
            self.assertTrue(all(sample['tx_hash'].startswith('BOT-LOAD-') for sample in samples))
            self.assertTrue(all(sample['started_at'] <= sample['finished_at'] for sample in samples))
            return report

    def test_replacement_and_post_recovery_traffic_required(self):
        kube = FakeKube()
        report = self.run_demo(kube)
        self.assertEqual(kube.deleted, ['gateway-a'])
        self.assertTrue(report['request_continuity_pass'])
        self.assertEqual(report['counts']['failure'], 0)
        self.assertGreater(report['state']['recovery_seconds'], 0)
        self.assertTrue(report['baseline_distribution_pass'])
        self.assertTrue(report['survivor_handled_during_recovery'])
        self.assertTrue(report['replacement_handled_after_recovery'])
        self.assertEqual(sum(p['requests'] for p in report['pod_counts'].values()),
                         report['counts']['accepted'])
        self.assertIn('gateway-c', report['pod_counts'])

    def test_missing_identity_prevents_disruption(self):
        kube = FakeKube()
        report = self.run_demo(kube, identity=False)
        self.assertEqual(kube.deleted, [])
        self.assertFalse(report['request_continuity_pass'])
        self.assertIn('unknown', report['pod_counts'])

    def test_one_sided_distribution_prevents_disruption(self):
        kube = FakeKube()
        report = self.run_demo(kube, one_sided=True)
        self.assertEqual(kube.deleted, [])
        self.assertFalse(report['baseline_distribution_pass'])

    def test_auth_failure_prevents_disruption(self):
        kube = FakeKube()
        report = self.run_demo(kube, status=401)
        self.assertEqual(kube.deleted, [])
        self.assertFalse(report['request_continuity_pass'])
        self.assertGreater(report['counts']['failure'], 0)

    def test_original_pods_cannot_count_as_replacement(self):
        report = self.run_demo(FakeKube(replacement=False))
        self.assertEqual(report['state']['recovered'], 0)
        self.assertFalse(report['request_continuity_pass'])

    def test_existing_unready_pod_is_not_a_new_replacement(self):
        class ExistingPodKube(FakeKube):
            def pods(self):
                if self.deleted:
                    return [pod('gateway-b'), pod('gateway-c')]
                return [pod('gateway-a'), pod('gateway-b'), pod('gateway-c', False)]

        report = self.run_demo(ExistingPodKube())
        self.assertEqual(report['state']['recovered'], 0)
        self.assertFalse(report['request_continuity_pass'])

    def test_insufficient_ready_pods_prevents_disruption(self):
        kube = FakeKube(ready_count=1)
        report = self.run_demo(kube)
        self.assertFalse(report['request_continuity_pass'])
        self.assertEqual(kube.deleted, [])

    def test_only_documented_business_rejections_are_distinct(self):
        self.assertEqual(module.outcome(429, {'status': 'REJECTED', 'reason': 'TEMP_HOLD_R01'}), 'fds_rejected')
        self.assertEqual(module.outcome(403, {'status': 'REJECTED', 'reason': 'FROZEN_L2'}), 'fds_rejected')
        for code in (401, 422, 429, 500, 503):
            self.assertEqual(module.outcome(code, {}), 'failure')

    def test_slow_kubernetes_poll_does_not_pause_load(self):
        class SlowKube(FakeKube):
            calls = 0

            def pods(self):
                self.calls += 1
                if self.calls == 2:
                    time.sleep(2)
                return super().pods()

        report = self.run_demo(SlowKube())
        self.assertGreaterEqual(report['counts']['accepted'], 36)
        self.assertTrue(report['request_continuity_pass'])

    def test_truncated_response_is_recorded_as_failure(self):
        with tempfile.TemporaryDirectory() as output, patch.dict(os.environ, {'GATEWAY_API_KEY': 'test'}):
            demo = module.Demo(FakeKube(), output, rps=5, duration=8, fault_after=5)
            demo.started_monotonic = time.monotonic()
            with patch.object(demo.http, 'open', side_effect=http.client.IncompleteRead(b'{')):
                demo.request(0)
            self.assertEqual(demo.samples[0]['result'], 'failure')
            self.assertEqual(demo.samples[0]['error_type'], 'IncompleteRead')

    def test_p95_accepted_excludes_fast_failures(self):
        with tempfile.TemporaryDirectory() as output, patch.dict(os.environ, {'GATEWAY_API_KEY': 'test'}):
            demo = module.Demo(FakeKube(), output, rps=5, duration=8, fault_after=5)
            demo.latencies = [0.001] * 99 + [2.0]
            demo.samples = [dict(pod_name='gateway-a', phase='baseline', result='failure', seconds=0.001)] * 99
            demo.samples += [dict(pod_name='gateway-a', phase='baseline', result='accepted', seconds=2.0)]
            report = demo.report()
            self.assertEqual(report['p95_seconds'], 0.001)
            self.assertEqual(report['p95_accepted_seconds'], 2.0)

    def test_phase_uses_finish_time_and_separates_fault_boundary(self):
        cases = [(10, 19, 'baseline'), (10, 30, 'boundary'),
                 (21, 24, 'recovering'), (21, 30, 'recovered')]
        with tempfile.TemporaryDirectory() as output, patch.dict(os.environ, {'GATEWAY_API_KEY': 'test'}):
            for started, finished, expected in cases:
                with self.subTest(expected=expected):
                    demo = module.Demo(FakeKube(), output, rps=5, duration=8, fault_after=5)
                    demo.started_monotonic = 0
                    demo.fault_monotonic = 20
                    demo.recovered_monotonic = 25
                    demo.state['recovered'] = 1
                    response = io.BytesIO(b'{"status":"ACCEPTED"}')
                    response.status = 200
                    response.headers = {'X-Gateway-Pod': 'gateway-b'}
                    with patch.object(demo.http, 'open', return_value=response), \
                         patch.object(module.time, 'monotonic', side_effect=[started, finished]):
                        demo.request(0)
                    self.assertEqual(demo.samples[0]['phase'], expected)
                    self.assertEqual(demo.report()['phase_counts'][expected]['accepted'], 1)

    def test_boundary_failure_remains_in_total_failures(self):
        with tempfile.TemporaryDirectory() as output, patch.dict(os.environ, {'GATEWAY_API_KEY': 'test'}):
            demo = module.Demo(FakeKube(), output, rps=5, duration=8, fault_after=5)
            demo.started_monotonic = 0
            demo.fault_monotonic = 20
            with patch.object(demo.http, 'open', side_effect=http.client.IncompleteRead(b'{')), \
                 patch.object(module.time, 'monotonic', side_effect=[10, 30]):
                demo.request(0)
            report = demo.report()
            self.assertEqual(report['phase_counts']['boundary']['failure'], 1)
            self.assertEqual(report['counts']['failure'], 1)
            self.assertFalse(report['request_continuity_pass'])


if __name__ == '__main__':
    unittest.main()
