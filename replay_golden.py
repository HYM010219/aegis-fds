#!/usr/bin/env python3
"""
replay_golden.py — 30분 골든 데이터셋 1배속 과학적 리플레이 스크립트

사용법:
    python replay_golden.py --round-prefix A
    python replay_golden.py --round-prefix B
"""

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import requests

def parse_iso(ts_str: str) -> datetime:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))

def get_gateway_key(base_dir: Path) -> str:
    env_file = base_dir / ".env"
    if env_file.exists():
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("GATEWAY_API_KEY="):
                    return line.split("=", 1)[1].strip()
    return os.getenv("GATEWAY_API_KEY", "")

def main():
    parser = argparse.ArgumentParser(description="Replay golden on-chain dataset at 1x speed with salted hashes.")
    parser.add_argument("--round-prefix", "-r", type=str, default="A", help="Replay round prefix (e.g. A, B)")
    parser.add_argument("--golden-file", "-f", type=str, default="data/onchain/golden_r3_30min.jsonl", help="Golden dataset JSONL")
    parser.add_argument("--gateway-url", "-u", type=str, default="http://localhost:8000/api/v1/transfers", help="FastAPI transfers endpoint")
    parser.add_argument("--speed", "-s", type=float, default=1.0, help="Playback speed multiplier (1.0 = real-time)")
    parser.add_argument("--min-amount", type=float, default=0.01, help="Minimum amount to send (default 0.01 USDT)")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    golden_path = base_dir / args.golden_file
    if not golden_path.exists():
        print(f"[ERROR] 골든 데이터셋 파일을 찾을 수 없습니다: {golden_path}")
        sys.exit(1)

    api_key = get_gateway_key(base_dir)
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key

    # T0 파일 기록
    t0_epoch_ms = int(time.time() * 1000)
    t0_file = base_dir / f".retest_t0_replay_{args.round_prefix.lower()}"
    with open(t0_file, "w", encoding="utf-8") as f:
        f.write(str(t0_epoch_ms))

    t0_dt_kst = datetime.fromtimestamp(t0_epoch_ms / 1000.0, timezone.utc).astimezone()
    print("=" * 70)
    print(f"=== Aegis-FDS 리플레이 {args.round_prefix}회차 시작 ===")
    print(f"  T0 (ms)        : {t0_epoch_ms} ({t0_dt_kst.strftime('%Y-%m-%d %H:%M:%S %Z')})")
    print(f"  T0 기록 파일   : {t0_file}")
    print(f"  입력 데이터셋  : {golden_path}")
    print(f"  재생 속도      : {args.speed}배속")
    print(f"  게이트웨이 URL : {args.gateway_url}")
    print(f"  해시 솔트 규칙 : sha256('{args.round_prefix}_' + original_tx_hash + '_' + log_index)")
    print("=" * 70, flush=True)

    # 1단계: 유효 트랜잭션 전수 로드
    records = []
    with open(golden_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if rec.get("removed"):
                continue
            if rec.get("amount", 0.0) < args.min_amount:
                continue
            records.append(rec)

    total_records = len(records)
    print(f"[*] 전송 대상 트랜잭션 로드 완료: {total_records:,}건", flush=True)
    if total_records == 0:
        print("[ERROR] 전송 대상 트랜잭션이 없습니다.")
        sys.exit(1)

    first_collected_at = parse_iso(records[0]["collected_at"])
    last_collected_at = parse_iso(records[-1]["collected_at"])
    total_data_seconds = (last_collected_at - first_collected_at).total_seconds()
    print(f"[*] 데이터셋 시간 범위: {records[0]['collected_at']} ~ {records[-1]['collected_at']} ({total_data_seconds:.1f}초, 약 {total_data_seconds / 60:.1f}분)")
    print(f"[*] 예상 리플레이 소요 시간: {total_data_seconds / args.speed:.1f}초 (약 {(total_data_seconds / args.speed) / 60:.1f}분)", flush=True)

    session = requests.Session()
    stats = {
        "sent": 0,
        "200": 0,
        "429": 0,
        "403": 0,
        "other": 0,
        "errors": 0,
    }

    start_wall_time = time.time()
    last_report_time = start_wall_time

    for idx, rec in enumerate(records):
        # 1배속 타이밍 동기화
        rec_time = parse_iso(rec["collected_at"])
        delta_data = (rec_time - first_collected_at).total_seconds()
        target_wall_time = start_wall_time + (delta_data / args.speed)
        now_wall_time = time.time()
        sleep_needed = target_wall_time - now_wall_time
        if sleep_needed > 0:
            time.sleep(sleep_needed)

        # 회차별 Salted tx_hash 생성
        orig_hash = rec.get("tx_hash", "")
        log_idx = rec.get("log_index", 0)
        salt_source = f"{args.round_prefix}_{orig_hash}_{log_idx}"
        salted_hash = "0x" + hashlib.sha256(salt_source.encode("utf-8")).hexdigest()

        # 게이트웨이 ±30초 검증 통과를 위한 현재 UTC 시각
        curr_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {
            "tx_hash": salted_hash,
            "log_index": log_idx,
            "from": rec["from"],
            "to": rec["to"],
            "amount": round(rec["amount"], 2),
            "block": rec["block"],
            "timestamp": curr_ts,
        }

        try:
            resp = session.post(args.gateway_url, json=payload, headers=headers, timeout=5)
            status = resp.status_code
            stats["sent"] += 1
            if status in (200, 201):
                stats["200"] += 1
            elif status == 429:
                stats["429"] += 1
            elif status == 403:
                stats["403"] += 1
            else:
                stats["other"] += 1
        except Exception as e:
            stats["sent"] += 1
            stats["errors"] += 1

        # 주기적 진행률 보고 (10초 주기 또는 마지막)
        curr_time = time.time()
        if curr_time - last_report_time >= 10.0 or idx == total_records - 1:
            elapsed = curr_time - start_wall_time
            pct = ((idx + 1) / total_records) * 100
            tps = stats["sent"] / max(1.0, elapsed)
            print(
                f"[리플레이 {args.round_prefix}] {idx + 1:,}/{total_records:,} ({pct:5.1f}%) | "
                f"경과: {elapsed:5.1f}s / {total_data_seconds:5.1f}s | "
                f"200(통과): {stats['200']:,} | 429(보류): {stats['429']:,} | 403(동결/차단): {stats['403']:,} | "
                f"오류: {stats['errors']} | TPS: {tps:4.1f}",
                flush=True
            )
            last_report_time = curr_time

    total_wall_elapsed = time.time() - start_wall_time
    print("=" * 70)
    print(f"=== 리플레이 {args.round_prefix}회차 전송 완료 ===")
    print(f"  총 전송 건수    : {stats['sent']:,}건")
    print(f"  200 통과        : {stats['200']:,}건 ({stats['200'] / stats['sent'] * 100:.1f}%)")
    print(f"  429 보류(HOLD)  : {stats['429']:,}건 ({stats['429'] / stats['sent'] * 100:.1f}%)")
    print(f"  403 동결/차단   : {stats['403']:,}건 ({stats['403'] / stats['sent'] * 100:.1f}%)")
    print(f"  기타/오류       : 기타 {stats['other']}건 / 실패 {stats['errors']}건")
    print(f"  실제 소요 시간  : {total_wall_elapsed:.1f}초 (약 {total_wall_elapsed / 60:.1f}분)")
    print(f"  평균 전송 속도  : {stats['sent'] / max(1.0, total_wall_elapsed):.2f} TPS")
    print("=" * 70, flush=True)

if __name__ == "__main__":
    main()
