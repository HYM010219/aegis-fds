import redis.asyncio as redis

from common.keys import whitelist_key


async def is_whitelisted(r: redis.Redis, rule_id: str, wallet: str) -> bool:
    """이 지갑이 해당 룰에서 면제되는지 확인한다."""
    return bool(await r.sismember(whitelist_key(rule_id), wallet))
