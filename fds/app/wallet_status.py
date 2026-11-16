"""송금 전에 지갑 상태를 확인한다: 동결, R07 블랙리스트, 보류 중.

    동결           보내는 지갑이 동결됨          → 403 FROZEN_L1 ~ FROZEN_L8 (사유 없으면 FROZEN_MANUAL)
    R07 블랙리스트 보내는 지갑이 블랙리스트       → 403 BLACKLISTED_SENDER
                   받는 지갑이 블랙리스트         → 403 BLACKLISTED_RECEIVER
    보류 중        보내는 지갑이 R01~R06 보류 중  → 429 TEMP_HOLD_Rxx (Retry-After: 남은 초)

R01~R06 계산은 holds.py. 여기 검사는 R룰 계산보다 먼저, Redis 왕복 한 번으로 끝낸다.
동결·보류 중인 지갑도 받는 것은 막지 않는다 (보내는 지갑만 확인).
"""
from dataclasses import dataclass

import redis.asyncio as redis

from common.keys import BLACKLIST_KEY, FROZEN_KEY, FROZEN_REASONS_KEY

# ---------- 응답 사유 ----------
# Spark 2단계 룰. 동결 사유는 FROZEN_L1 ~ FROZEN_L8로 응답한다.
LONG_RULES = ("L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8")

BLACKLISTED_SENDER = "BLACKLISTED_SENDER"      # R07: 보내는 지갑이 블랙리스트
BLACKLISTED_RECEIVER = "BLACKLISTED_RECEIVER"  # R07: 받는 지갑이 블랙리스트


def frozen_reason(rule: str | None) -> str:
    """동결 사유 응답값. 룰 사유가 없는 동결(수동 등록 등)은 FROZEN_MANUAL."""
    return f"FROZEN_{rule}" if rule in LONG_RULES else "FROZEN_MANUAL"


@dataclass
class Block:
    http_code: int
    reason: str
    retry_after: int | None = None   # 보류 중일 때 남은 초


# ---------- 지갑 상태 확인 (main.py가 부르는 함수) ----------

async def check_wallets(r: redis.Redis, from_wallet: str, to_wallet: str, hold_key: str) -> Block | None:
    """동결, R07 블랙리스트, 보류 중인지 확인한다. 막아야 하면 Block, 통과면 None."""
    # 여섯 번의 조회를 한 번의 왕복으로 처리
    pipe = r.pipeline(transaction=False)
    pipe.sismember(FROZEN_KEY, from_wallet)
    pipe.hget(FROZEN_REASONS_KEY, from_wallet)
    pipe.sismember(BLACKLIST_KEY, from_wallet)
    pipe.sismember(BLACKLIST_KEY, to_wallet)
    pipe.get(hold_key)
    pipe.pttl(hold_key)
    sender_frozen, frozen_rule, sender_blacklisted, receiver_blacklisted, hold_rule, hold_ms = await pipe.execute()

    # 동결: 보내는 지갑
    if sender_frozen:
        return Block(403, frozen_reason(frozen_rule))

    # R07 블랙리스트: 보내는 지갑, 받는 지갑
    if sender_blacklisted:
        return Block(403, BLACKLISTED_SENDER)
    if receiver_blacklisted:
        return Block(403, BLACKLISTED_RECEIVER)

    # 보류 중: 보류는 송금만 막는다. 받는 쪽 검사는 하지 않는다.
    if hold_rule:
        seconds = max(1, -(-hold_ms // 1000)) if hold_ms and hold_ms > 0 else 1
        return Block(429, f"TEMP_HOLD_{hold_rule}", retry_after=seconds)
    return None
