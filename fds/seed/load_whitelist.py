import os
import sys
from pathlib import Path

import redis

RULES = {"R01", "R02", "R03", "R04", "R05", "R06"}
LIST_FILE = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "whitelist.txt"

entries = []
for number, line in enumerate(LIST_FILE.read_text(encoding="utf-8").splitlines(), start=1):
    line = line.split("#", 1)[0].strip()  # 주석과 빈 줄 제외
    if not line:
        continue
    parts = line.split()
    rules = {rule.strip().upper() for rule in parts[1].split(",")} if len(parts) == 2 else set()
    if not rules or not rules <= RULES:
        sys.exit(f"{number}번째 줄 형식 오류: {line} (예: 0xabc R01,R03)")
    entries.append((parts[0], rules))

r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, decode_responses=True)

for wallet, rules in entries:
    for rule in rules:
        r.sadd(f"fds:whitelist:{rule}", wallet)

for rule in sorted(RULES):
    print(f"{rule}: 전체 {r.scard(f'fds:whitelist:{rule}')}건")
