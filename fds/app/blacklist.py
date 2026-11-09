from enum import Enum

import redis.asyncio as redis

FROZEN_KEY = "fds:frozen_wallets"          # 동결된 지갑
FROZEN_REASONS_KEY = "fds:frozen_reasons"  # 지갑별 동결 사유 (지갑주소 → 룰 ID)
BLACKLIST_KEY = "fds:blacklist_wallets"    # 미리 알려진 위험 주소


class BlockReason(str, Enum):
    FROZEN_R01 = "FROZEN_R01"
    FROZEN_R02 = "FROZEN_R02"
    FROZEN_R03 = "FROZEN_R03"
    FROZEN_R04 = "FROZEN_R04"
    FROZEN_R06 = "FROZEN_R06"
    FROZEN_MANUAL = "FROZEN_MANUAL"  # 수동 등록처럼 룰 사유가 없는 동결
    BLACKLISTED_SENDER = "BLACKLISTED_SENDER"
    BLACKLISTED_RECEIVER = "BLACKLISTED_RECEIVER"


FROZEN_REASONS = {
    "R01": BlockReason.FROZEN_R01,
    "R02": BlockReason.FROZEN_R02,
    "R03": BlockReason.FROZEN_R03,
    "R04": BlockReason.FROZEN_R04,
    "R06": BlockReason.FROZEN_R06,
}


async def check_blacklist(r: redis.Redis, from_wallet: str, to_wallet: str) -> BlockReason | None:
    """막아야 하면 사유를, 통과면 None을 돌려준다."""
    # 네 번의 조회를 한 번의 왕복으로 처리
    pipe = r.pipeline(transaction=False)
    pipe.sismember(FROZEN_KEY, from_wallet)
    pipe.hget(FROZEN_REASONS_KEY, from_wallet)
    pipe.sismember(BLACKLIST_KEY, from_wallet)
    pipe.sismember(BLACKLIST_KEY, to_wallet)
    sender_frozen, frozen_rule, sender_blacklisted, receiver_blacklisted = await pipe.execute()

    if sender_frozen:
        return FROZEN_REASONS.get(frozen_rule, BlockReason.FROZEN_MANUAL)
    if sender_blacklisted:
        return BlockReason.BLACKLISTED_SENDER
    if receiver_blacklisted:
        return BlockReason.BLACKLISTED_RECEIVER
    return None
