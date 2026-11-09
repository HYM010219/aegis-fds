import redis.asyncio as redis

# 화이트리스트를 둘 수 있는 룰. R07(블랙리스트)과 동결은 면제 대상이 아니다.
WHITELIST_RULES = ("R01", "R02", "R03", "R04", "R05", "R06")


def whitelist_key(rule_id: str) -> str:
    """룰마다 Redis Set을 하나씩 쓴다. 예: fds:whitelist:R05"""
    if rule_id not in WHITELIST_RULES:
        raise ValueError(f"화이트리스트가 없는 룰: {rule_id}")
    return f"fds:whitelist:{rule_id}"


async def is_whitelisted(r: redis.Redis, rule_id: str, wallet: str) -> bool:
    """이 지갑이 해당 룰에서 면제되는지 확인한다."""
    return bool(await r.sismember(whitelist_key(rule_id), wallet))
