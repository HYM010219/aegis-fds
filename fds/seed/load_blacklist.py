import os
from pathlib import Path

import redis

KEY = "fds:blacklist_wallets"
LIST_FILE = Path(__file__).parent / "blacklist.txt"

r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, decode_responses=True)

wallets = [line.strip() for line in LIST_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
added = r.sadd(KEY, *wallets)
print(f"추가 {added}건 / 전체 {r.scard(KEY)}건")