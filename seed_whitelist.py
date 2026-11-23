#!/usr/bin/env python3
"""
Aegis-FDS 화이트리스트 시드 스크립트 (PoC)

CSV를 읽어 룰별 Redis Set 키에 주소를 넣는다.
  - 시드 A: 실데이터 거래소 핫월렛·허브 지갑  -> 보통 rules = ALL (R01~R06 + L)
  - 시드 B: bot3 시연 지갑 룰별 면제           -> 예: L3 지갑 rules = R02

여러 번 실행해도 결과가 같다 (SADD는 이미 있는 값은 무시).

사용 예
  python seed_whitelist.py whitelist_seed.csv --dry-run
  python seed_whitelist.py whitelist_seed.csv
  python seed_whitelist.py whitelist_seed.csv --remove sim_wallets.txt   # 231개 일괄 면제 해제 후 시드

환경 변수
  REDIS_URL (기본 redis://localhost:6379/0)
"""
import argparse
import csv
import os
import re
import sys

import redis  # pip install redis

ALL_RULES = ["R01", "R02", "R03", "R04", "R05", "R06", "L"]
KEY = "fds:whitelist:{}"
BLACKLIST_KEY = "fds:blacklist_wallets"
ADDR_RE = re.compile(r"^0x[0-9a-f]{40}$")


def parse_rules(raw: str) -> list[str]:
    raw = raw.strip().upper()
    if raw == "ALL":
        return ALL_RULES
    rules = [r.strip() for r in raw.split(";") if r.strip()]
    bad = [r for r in rules if r not in ALL_RULES]
    if bad:
        raise ValueError(f"알 수 없는 룰 {bad} (허용: {ALL_RULES} 또는 ALL). R07은 면제 대상 아님")
    return rules


def load_csv(path: str):
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            addr = row["address"].strip().lower()
            if not ADDR_RE.match(addr):
                print(f"[건너뜀] {i}행 주소 형식 오류: {row['address']}", file=sys.stderr)
                continue
            rows.append({
                "address": addr,
                "group": row.get("group", "").strip(),
                "rules": parse_rules(row["rules"]),
                "source": row.get("source", "").strip(),
            })
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path")
    ap.add_argument("--remove", help="모든 화이트리스트 키에서 뺄 주소 목록 파일 (한 줄에 하나)")
    ap.add_argument("--dry-run", action="store_true", help="Redis에 쓰지 않고 계획만 출력")
    args = ap.parse_args()

    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    r.ping()

    # 1) 일괄 면제 해제 (예: 시뮬레이터 지갑 231개)
    if args.remove:
        with open(args.remove, encoding="utf-8") as f:
            targets = [ln.strip().lower() for ln in f if ln.strip()]
        for rule in ALL_RULES:
            if args.dry_run:
                print(f"[dry-run] SREM {KEY.format(rule)} x{len(targets)}")
            elif targets:
                removed = r.srem(KEY.format(rule), *targets)
                print(f"SREM {KEY.format(rule)}: {removed}개 제거")

    # 2) 시드 넣기
    rows = load_csv(args.csv_path)
    pipe = r.pipeline()
    for row in rows:
        if r.sismember(BLACKLIST_KEY, row["address"]):
            print(f"[경고] 블랙리스트와 겹침, 넣지 않음: {row['address']} ({row['source']})")
            continue
        for rule in row["rules"]:
            if args.dry_run:
                print(f"[dry-run] SADD {KEY.format(rule)} {row['address']}  # {row['group']} {row['source']}")
            else:
                pipe.sadd(KEY.format(rule), row["address"])
    if not args.dry_run:
        pipe.execute()

    # 3) 결과 요약
    for rule in ALL_RULES:
        print(f"{KEY.format(rule)}: {r.scard(KEY.format(rule))}개")


if __name__ == "__main__":
    main()
