import os
from pathlib import Path

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
FROZEN_KEY = "fds:frozen_wallets"
SEED_FILE = Path(__file__).parent / "frozen.txt"


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    wallets = []
    for line in SEED_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            wallets.append(line)

    if not wallets:
        print("frozen.txt에 등록할 지갑이 없어요")
        return

    added = r.sadd(FROZEN_KEY, *wallets)
    print(f"추가 {added}건 / 전체 {r.scard(FROZEN_KEY)}건")


if __name__ == "__main__":
    main()