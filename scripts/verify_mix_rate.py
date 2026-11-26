"""Deterministic offline checks for mixing ratio and outage/backlog behavior."""
import importlib.util
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bots.rate_control import RealRateGate, record_real_ingress


def main():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)/'state.json'
        now = [100.0]
        def write(count):
            path.write_text(json.dumps({'real_count':count,'updated_at':now[0]}), encoding='utf-8')
        gate = RealRateGate(path, clock=lambda:now[0])
        assert not gate.try_acquire()
        # One real request earns one bot permit (100 real -> 100 bots).
        for count in range(1, 101):
            now[0] += .1
            write(count)
            assert gate.try_acquire()
            assert not gate.try_acquire()
        assert gate.sent == 100

        # A burst of 10 real requests earns 10 paced bot permits.
        now[0] += .1
        write(110)  # 10 * 1.0 = 10 permits
        for _ in range(10):
            assert gate.try_acquire()
            assert not gate.try_acquire()  # min_interval 경과 전 즉시 재호출 시 False (페이싱)
            now[0] += 0.05
        assert not gate.try_acquire()  # All 10 permits have been consumed.
        assert gate.sent == 110

        # 3. 실데이터 중단/지연 검사 (2초 초과 stale feed 시 봇 전면 중지)
        now[0] += 3
        assert not gate.try_acquire()

        # 4. 장애 복구 후 재개 시 백로그 폭발(catch-up burst) 방지
        write(1000)
        assert not gate.try_acquire()  # 단절 중 쌓인 과거 백로그는 버스트로 방출하지 않음

        # Resume the 1:1 ratio after fresh ingress.
        now[0] += .1
        write(1001)
        assert gate.try_acquire()
        now[0] += .1
        write(1002)
        assert gate.try_acquire()
        assert gate.sent == 112
        with patch.dict(os.environ, {'MIX_RATE_STATE':str(path)}):
            for status in (0, 401, 422, 500):
                record_real_ingress(status)
            assert json.loads(path.read_text())['real_count'] == 1002
            for status in (200,201,403,429):
                record_real_ingress(status)
            assert json.loads(path.read_text())['real_count'] == 1006
        for ratio in (0, -1, 1.1):
            try:
                RealRateGate(path, ratio=ratio)
            except ValueError:
                pass
            else:
                raise AssertionError('invalid ratio accepted')
        # Paced scenario consumes a gate before generating current tx timestamp.
        spec = importlib.util.spec_from_file_location('mix_bots', ROOT/'bots/run_bots.py')
        bots = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bots)
        class Gate:
            count = 0
            def wait(self):
                self.count += 1
        fake = Gate()
        cfg = {'pacer':fake}
        with patch.object(bots, 'post', return_value=(200,'')), patch.object(bots, 'OUT_DIR',Path(directory)), \
             patch.object(bots, 'CSV_PATH',Path(directory)/'bots.csv'), patch.object(bots.time,'sleep') as sleep:
            bots.run_one(cfg, 'L2', 2, False, paced=True)
            assert fake.count == 3
            sleep.assert_not_called()
    print('PASS 1:1 pacing, stale feed stop, no backlog burst, status filters and paced L2')


if __name__ == '__main__':
    main()
