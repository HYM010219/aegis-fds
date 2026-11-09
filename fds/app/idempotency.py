IDEMP_TTL_SECONDS = 3600  # 1시간 뒤 자동 삭제


def _idemp_key(sender: str, idem_key: str) -> str:
    return f"idemp:{sender}:{idem_key}"


async def claim_idempotency(r, sender: str, idem_key: str) -> bool:
    """처음 온 요청이면 True, 이미 처리 중이거나 처리된 키면 False."""
    ok = await r.set(_idemp_key(sender, idem_key), "1", nx=True, ex=IDEMP_TTL_SECONDS)
    return bool(ok)


async def release_idempotency(r, sender: str, idem_key: str) -> None:
    """처리가 실패했을 때 키를 지워서, 같은 키로 재시도할 수 있게 함."""
    await r.delete(_idemp_key(sender, idem_key))