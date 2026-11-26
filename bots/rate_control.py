"""Optional local mixer: earn one bot request per completed real FDS request.

One real producer and one bot producer share an atomic counter file.
Burst credits accumulate up to capacity and pace evenly, while pauses/outages
never produce catch-up backlog bursts.
"""
import json
import os
import time
from pathlib import Path


def record_real_ingress(status):
    filename = os.getenv('MIX_RATE_STATE')
    if not filename or status not in (200, 201, 403, 429):
        return
    path = Path(filename)
    try:
        previous = json.loads(path.read_text(encoding='utf-8'))['real_count']
    except (OSError, ValueError, KeyError):
        previous = 0
    temporary = path.with_name(path.name + '.producer.tmp')
    temporary.write_text(json.dumps({'real_count': previous + 1, 'updated_at': time.time()}), encoding='utf-8')
    temporary.replace(path)


class RealRateGate:
    def __init__(self, filename, ratio=1.0, max_credit=10.0, min_interval=0.05,
                 stale_timeout=2.0, clock=time.time, sleep=time.sleep):
        if not 0 < ratio <= 1:
            raise ValueError('bot ratio must be greater than 0 and at most 1')
        self.path = Path(filename)
        self.ratio = ratio
        self.max_credit = float(max_credit)
        self.min_interval = float(min_interval)
        self.stale_timeout = float(stale_timeout)
        self.clock = clock
        self.sleep = sleep
        self.seen = 0
        self.credit = 0.0
        self.sent = 0
        self.last_acquire = -1e9
        self.last_timestamp = None
        self.was_stale = False

    def try_acquire(self):
        try:
            state = json.loads(self.path.read_text(encoding='utf-8'))
            count, timestamp = state['real_count'], state['updated_at']
        except (OSError, ValueError, KeyError):
            return False

        now = self.clock()

        # 1. 실데이터 피드가 끊기거나 멈춘 경우 (stale timeout) 즉시 봇 정지
        if not (0 <= now - timestamp <= self.stale_timeout):
            self.credit = 0.0
            self.was_stale = True
            self.seen = count
            self.last_timestamp = timestamp
            return False

        # 2. 장애/중단 후 재개 시 백로그 누적분을 일시 방출(catch-up burst)하지 않도록 동기화
        if self.was_stale or (self.last_timestamp is not None and (timestamp - self.last_timestamp) > self.stale_timeout):
            self.seen = count
            self.credit = 0.0
            self.was_stale = False
            self.last_timestamp = timestamp
            return False

        self.last_timestamp = timestamp

        # 3. 실데이터 유입량에 비례하여 크레딧 적립 (최대 max_credit까지 버스트 수용)
        delta = max(0, count - self.seen)
        self.seen = count
        if delta > 0:
            self.credit = min(self.max_credit, self.credit + delta * self.ratio)

        # 4. 잔여 크레딧 확인
        if self.credit < 1.0:
            return False

        # 5. 몰린 거래량을 일정 시간(min_interval)에 걸쳐 나누어 방출 (pacing)
        if (now - self.last_acquire) < (self.min_interval - 1e-6):
            return False

        self.credit -= 1.0
        self.sent += 1
        self.last_acquire = now
        return True

    def wait(self):
        while not self.try_acquire():
            self.sleep(0.05)
        stats = self.path.with_name(self.path.name + '.bot.json')
        temporary = stats.with_name(stats.name + '.tmp')
        temporary.write_text(json.dumps({'bot_count': self.sent, 'updated_at': self.clock()}), encoding='utf-8')
        temporary.replace(stats)
