"""Offline R05/L2 boundary and configuration checks. No DB or API calls.

Run using the project's Python environment. Optional R05_LIMIT_USDT override
checks that both engines continue to share the same threshold.
"""
import asyncio
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'fds'))
sys.path.insert(0, str(ROOT/'fds/spark'))
from common.keys import R05_LIMIT_USDT, WHALE_LIMIT_USDT
from app import holds
import config
import rules


async def verify_r05():
    calls = []
    async def whitelist(*args):
        return False
    async def hold(*args):
        calls.append(args)
        return True
    original = holds.is_whitelisted, holds.hold_sender
    holds.is_whitelisted, holds.hold_sender = whitelist, hold
    try:
        limit = Decimal(R05_LIMIT_USDT)
        req = SimpleNamespace(from_address='BOT-R05-BOUNDARY')
        assert await holds._check_r05(None, req, limit-Decimal('0.000001')) is None
        assert calls == []
        for amount in [limit, limit+Decimal('0.000001')]:
            decision = await holds._check_r05(None, req, amount)
            assert decision is not None
        assert len(calls) == 2
    finally:
        holds.is_whitelisted, holds.hold_sender = original


def verify_l2():
    now = datetime.now(timezone.utc)
    limit = Decimal(WHALE_LIMIT_USDT)
    row = {'_id':'BOT-L2-BOUNDARY', 'day_hashes':['boundary'], 'day_first':now,
           'day_last':now, 'day_count':3, 'day_max':limit*Decimal('0.4')}
    assert rules.l2(dict(row, day_sum=limit-Decimal('0.000001')), now) is None
    for total in [limit, limit*Decimal('1.1')]:
        alert = rules.l2(dict(row, day_sum=total), now)
        assert alert and alert['rule_id'] == 'L2'
        assert alert['criteria']['min_day_sum'] == R05_LIMIT_USDT
        assert alert['criteria']['each_below'] == R05_LIMIT_USDT
    assert rules.l2(dict(row, day_sum=limit*2, day_max=limit), now) is None


if __name__ == '__main__':
    assert R05_LIMIT_USDT == WHALE_LIMIT_USDT == config.L2_MIN_DAY_SUM
    assert holds.WHALE_LIMIT == Decimal(WHALE_LIMIT_USDT)
    asyncio.run(verify_r05())
    verify_l2()
    print(f'PASS R05/L2 shared threshold and amount boundaries: {R05_LIMIT_USDT} USDT')
