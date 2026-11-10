from dataclasses import dataclass

import redis.asyncio as redis

FROZEN_KEY = "fds:frozen_wallets"          # 동결된 지갑
FROZEN_REASONS_KEY = "fds:frozen_reasons"  # 지갑별 동결 사유 (지갑주소 → 룰 ID)
BLACKLIST_KEY = "fds:blacklist_wallets"    # 미리 알려진 위험 주소

# Spark 2단계 룰. 동결 사유는 FROZEN_L1 ~ FROZEN_L8로 응답한다.
LONG_RULES = ("L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8")

BLACKLISTED_SENDER = "BLACKLISTED_SENDER"
BLACKLISTED_RECEIVER = "BLACKLISTED_RECEIVER"


def frozen_reason(rule: str | None) -> str:
    """동결 사유 응답값. 룰 사유가 없는 동결(수동 등록 등)은 FROZEN_MANUAL."""
    return f"FROZEN_{rule}" if rule in LONG_RULES else "FROZEN_MANUAL"


@dataclass
class Block:
    http_code: int
    reason: str
    retry_after: int | None = None   # 보류 중일 때 남은 초


async def check_wallets(r: redis.Redis, from_wallet: str, to_wallet: str, hold_key: str) -> Block | None:
    """동결, 블랙리스트, 보류 중인지 확인한다. 막아야 하면 Block, 통과면 None."""
    # 여섯 번의 조회를 한 번의 왕복으로 처리
    pipe = r.pipeline(transaction=False)
    pipe.sismember(FROZEN_KEY, from_wallet)
    pipe.hget(FROZEN_REASONS_KEY, from_wallet)
    pipe.sismember(BLACKLIST_KEY, from_wallet)
    pipe.sismember(BLACKLIST_KEY, to_wallet)
    pipe.get(hold_key)
    pipe.pttl(hold_key)
    sender_frozen, frozen_rule, sender_blacklisted, receiver_blacklisted, hold_rule, hold_ms = await pipe.execute()

    if sender_frozen:
        return Block(403, frozen_reason(frozen_rule))
    if sender_blacklisted:
        return Block(403, BLACKLISTED_SENDER)
    if receiver_blacklisted:
        return Block(403, BLACKLISTED_RECEIVER)
    if hold_rule:
        # 보류는 송금만 막는다. 받는 쪽 검사는 하지 않는다.
        seconds = max(1, -(-hold_ms // 1000)) if hold_ms and hold_ms > 0 else 1
        return Block(429, f"TEMP_HOLD_{hold_rule}", retry_after=seconds)
    return None
