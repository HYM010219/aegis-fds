#!/usr/bin/env python3
"""
Aegis-FDS 이상거래 봇 주입기 (bot2 · bot3) — 시연·측정용

- 모든 봇 지갑은 "BOT-" 접두어 (보내는 쪽·받는 쪽 모두). 실제 메인넷 주소는 쓰지 않는다.
- DB 구조는 바꾸지 않는다. 게이트웨이 POST /api/v1/transfers 와 관리자 API(화이트·블랙리스트)만 쓴다.
- 회차마다 기록을 CSV로 남긴다 (bot_runs/bots.csv, 깃 제외).
- 시간 창은 정의서 v2.4 demo 프로파일 기준 (1단계 5분 / L1·L3·L4·L5 20분 / L2·L6·L7·L8 5분).

사용법 (저장소 맨 위 폴더에서)
  python bots/run_bots.py --dry-run                 # 보내지 않고 계획만 출력
  python bots/run_bots.py --once                    # 시나리오 16개를 1회씩 실행 → 기대값 비교표
  python bots/run_bots.py --once --only L8 R04      # 일부만
  python bots/run_bots.py --continuous              # 각 3회 보장 후 60:40 가중 반복 (Ctrl+C로 중지)
  옵션: --slow 2.0 (시나리오 간격 배수, 실데이터 비율과 별개) · --round-gap 30
  실데이터와 1:1로 섞기: python bots/stream_all.py (--paced 자동 적용)

환경 변수 (없으면 저장소 루트 .env 에서 읽음, 값은 출력하지 않음)
  GATEWAY_API_KEY, ADMIN_API_KEY, BOT_BASE_URL (기본 http://localhost:8000)
"""
import argparse
import csv
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "bot_runs"
CSV_PATH = OUT_DIR / "bots.csv"

# 시연 핵심: L2(분산 송금) 비중 대폭 상향, L8, L6
FOCUS = {"L8", "L2", "L6"}
FOCUS_SHARE = 0.75
MIN_RUNS_EACH = 3


# ---------------- 설정 ----------------

def load_env():
    env = {}
    p = ROOT / ".env"
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    get = lambda k, d="": os.getenv(k) or env.get(k, d)
    return {
        "base": get("BOT_BASE_URL", "http://localhost:8000").rstrip("/"),
        "gw_key": get("GATEWAY_API_KEY"),
        "admin_key": get("ADMIN_API_KEY"),
    }


# ---------------- HTTP ----------------

def post(cfg, path, body, admin=False):
    headers = {"Content-Type": "application/json"}
    headers["X-Admin-Key" if admin else "X-API-Key"] = cfg["admin_key"] if admin else cfg["gw_key"]
    req = urllib.request.Request(cfg["base"] + path, data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, ""
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8", "replace")[:300]
        except Exception:
            detail = ""
        return e.code, detail
    except Exception as e:  # 연결 실패 등
        return f"ERR:{type(e).__name__}", ""


# ---------------- 시나리오 정의 ----------------
# 단계 종류
#   ("tx", from, to, amount, expected_http, gap_after_sec)
#   ("whitelist", wallet, [rules])       관리자 API로 면제 등록 (정의서 시드 B 방식)
#   ("blacklist", wallet)                관리자 API로 블랙리스트 등록

def W(rid, scn, role):
    return f"BOT-{scn}-{rid}-{role}"


def scenarios(rid):
    w = lambda scn, role: W(rid, scn, role)
    S = {}
    # ---- 1단계 R01~R07 (게이트웨이) ----
    S["R01"] = dict(case=None, steps=[("tx", w("R01", "A"), w("R01", "B"), 100, 200, 5)] * 5
                    + [("tx", w("R01", "A"), w("R01", "B"), 100, 429, 0)])          # 5번째 200+보류, 6번째 429
    S["R02"] = dict(case=None, steps=[("tx", w("R02", "A"), w("R02", "B"), 950, 200, 5),
                                      ("tx", w("R02", "A"), w("R02", "B"), 950, 200, 5),
                                      ("tx", w("R02", "A"), w("R02", "B"), 950, 429, 0)])
    S["R03"] = dict(case=None, steps=[("tx", w("R03", f"S{i}"), w("R03", "Z"), 500, 200, 5) for i in range(1, 6)]
                    + [("tx", w("R03", "Z"), w("R03", "X"), 100, 429, 0)])           # 받는 지갑 Z 보류 확인
    S["R04"] = dict(case=None, steps=[("tx", w("R04", "A"), w("R04", f"B{i}"), 100, 200 if i < 5 else 429, 5)
                                      for i in range(1, 6)])
    S["R05"] = dict(case=None, steps=[("tx", w("R05", "A"), w("R05", "B"), 100000, 429, 0)])
    # 금액은 1,000 미만: 1,000 이상이면 A→B→A 순환으로 L8 자동 동결(403)이 먼저 걸린다
    S["R06"] = dict(case=None, steps=[("tx", w("R06", "P"), w("R06", "Q"), 500, 200, 5),
                                      ("tx", w("R06", "Q"), w("R06", "P"), 500, 200, 5),
                                      ("tx", w("R06", "P"), w("R06", "Q"), 100, 429, 0)])
    S["R07"] = dict(case=None, steps=[("blacklist", w("R07", "X")),
                                      ("tx", w("R07", "A"), w("R07", "X"), 100, 403, 0)])
    # ---- 2단계 L1~L8 (Spark, 게이트웨이 응답 + 이후 사건 생성) ----
    S["L1"] = dict(case="L1 자동 동결", steps=[("tx", w("L1", "A"), w("L1", "B"), 100000, 429, 200)] * 2
                   + [("tx", w("L1", "A"), w("L1", "B"), 100000, 429, 0)])           # R05 보류 3회 (보류 3분 지나서 다시)
    S["L2"] = dict(case="L2 자동 동결", steps=[("tx", w("L2", "A"), w("L2", "B"), 40000, 200, 20),
                                             ("tx", w("L2", "A"), w("L2", "C"), 40000, 200, 20),
                                             ("tx", w("L2", "A"), w("L2", "D"), 30000, 200, 0)])
    S["L3"] = dict(case="L3 자동 동결", steps=[("whitelist", w("L3", "A"), ["R01", "R02"])]
                   + [("tx", w("L3", "A"), w("L3", "B"), 950, 200, 20)] * 10)
    l4 = []
    for i in range(1, 4):  # 1,000 이상 받고 1분 안에 80% 이상 다시 송금 × 3회
        l4 += [("tx", w("L4", f"S{i}"), w("L4", "M"), 1500, 200, 15),
               ("tx", w("L4", "M"), w("L4", f"T{i}"), 1300, 200, 45)]
    S["L4"] = dict(case="L4 검토 대기", steps=l4)
    S["L5"] = dict(case="L5 검토 대기 (CRITICAL)", steps=[("tx", w("L5", "X"), w("L5", "B"), 2000, 200, 10),
                                                       ("blacklist", w("L5", "X"))])
    S["L6"] = dict(case="L6 검토 대기", steps=[("whitelist", w("L6", "A"), ["R01"])]
                   + [("tx", w("L6", "A"), w("L6", "B"), 100, 200, 3)] * 22)
    S["L7"] = dict(case="L7 검토 대기", steps=[("whitelist", w("L7", "A"), ["R01", "R04"])]
                   + [("tx", w("L7", "A"), w("L7", f"N{i:02d}"), 100, 200, 3) for i in range(1, 23)])
    S["L8"] = dict(case="L8 자동 동결", steps=[("tx", w("L8", "A"), w("L8", "B"), 1500, 200, 10),
                                             ("tx", w("L8", "B"), w("L8", "C"), 1400, 200, 10),
                                             ("tx", w("L8", "C"), w("L8", "A"), 1300, 200, 0)])
    # ---- bot2 정상 대량 패턴 (면제 지갑, 모두 통과해야 정상) ----
    S["BOT2_HOT"] = dict(case=None, steps=[("whitelist", w("HOT", "01"), ["R01", "R02", "R03", "R04", "R05", "R06", "L"])]
                         + [("tx", w("HOT", "01"), w("HOT", f"U{i}"), 3000, 200, 3) for i in range(1, 7)])
    return S


ALL = list(scenarios("x").keys())


# ---------------- 실행 ----------------

def run_one(cfg, scn, slow, dry, paced=False):
    rid = uuid4().hex[:6]
    spec = scenarios(rid)[scn]
    run_id = f"bot-{scn}-{rid}"
    start = datetime.now(timezone.utc).astimezone()
    expected, actual, wallets = [], [], set()
    aborted = ""
    for st in spec["steps"]:
        kind = st[0]
        if kind == "whitelist":
            wallets.add(st[1])
            body = {"operator": "bot", "memo": "bot seed", "rationale": f"{run_id} 시드 B 면제", "wallet": st[1], "rules": st[2]}
            code, detail = ("DRY", "") if dry else post(cfg, "/api/v1/admin/whitelist", body, admin=True)
            print(f"  [면제] {st[1]} {st[2]} → {code} {detail}")
            if not dry and code not in (200, 201):
                aborted = f"면제 등록 실패 {code}"
                break
            continue
        if kind == "blacklist":
            wallets.add(st[1])
            body = {"operator": "bot", "memo": "bot seed", "rationale": f"{run_id} 시연용 블랙리스트", "wallets": [st[1]]}
            code, detail = ("DRY", "") if dry else post(cfg, "/api/v1/admin/blacklist", body, admin=True)
            print(f"  [블랙] {st[1]} → {code} {detail}")
            if not dry and code not in (200, 201):
                aborted = f"블랙리스트 등록 실패 {code}"
                break
            continue
        _, frm, to, amt, exp, gap = st
        if not dry and cfg.get("pacer"):
            cfg["pacer"].wait()
        wallets.update([frm, to])
        body = {"tx_hash": f"BOT-TX-{uuid4().hex}", "log_index": 0, "from": frm, "to": to,
                "amount": amt, "block": int(time.time()), "timestamp": datetime.now(timezone.utc).isoformat()}
        code = "DRY" if dry else post(cfg, "/api/v1/transfers", body)[0]
        expected.append(exp)
        actual.append(code)
        mark = "" if dry else ("OK" if code == exp else "≠")
        print(f"  {frm} → {to} {amt:>8,} USDT  기대 {exp}  실제 {code} {mark}")
        if gap and not dry:
            # In the mixer the real-ingress gate supplies timing. Keep long HOLD
            # expiry waits (L1), but avoid arbitrary delays reducing the 2:1 mix.
            if not paced or gap >= 180:
                time.sleep(gap * (1 if paced else slow))
    end = datetime.now(timezone.utc).astimezone()
    match = "DRY" if dry else (f"ABORTED({aborted})" if aborted else ("OK" if expected == actual else "MISMATCH"))
    row = {"run_id": run_id, "scenario": scn, "group": "FOCUS" if scn in FOCUS else "OTHER",
           "start": start.strftime("%Y-%m-%d %H:%M:%S"), "end": end.strftime("%Y-%m-%d %H:%M:%S"),
           "expected_http": " ".join(map(str, expected)), "actual_http": " ".join(map(str, actual)),
           "gateway_match": match, "expected_case": spec["case"] or "-", "wallets": " ".join(sorted(wallets))}
    if not dry:
        OUT_DIR.mkdir(exist_ok=True)
        new = not CSV_PATH.exists()
        with CSV_PATH.open("a", newline="", encoding="utf-8-sig") as f:
            wr = csv.DictWriter(f, fieldnames=list(row.keys()))
            if new:
                wr.writeheader()
            wr.writerow(row)
    print(f"= {run_id} 게이트웨이 {match}  사건 기대: {row['expected_case']}\n")
    return row


def pick_weighted(names):
    # L2 비중 집중 상향 (전체의 약 40~50% 빈도로 L2 선택)
    if "L2" in names and random.random() < 0.45:
        return "L2"
    focus = [n for n in names if n in FOCUS]
    other = [n for n in names if n not in FOCUS]
    if focus and (not other or random.random() < FOCUS_SHARE):
        return random.choice(focus)
    return random.choice(other)


def main():
    ap = argparse.ArgumentParser(description="Aegis-FDS 이상거래 봇 주입기 (BOT- 접두어)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="보내지 않고 계획만 출력")
    mode.add_argument("--once", action="store_true", help="시나리오마다 1회 실행")
    mode.add_argument("--continuous", action="store_true", help="각 3회 보장 후 60:40 반복")
    ap.add_argument("--only", nargs="*", choices=ALL, help="일부 시나리오만")
    ap.add_argument("--slow", type=float, default=2.0, help="시나리오 간격 배수 (실데이터 대비 비율과는 별개)")
    ap.add_argument("--paced", action="store_true", help="실데이터 유입량을 따라 주입 (MIX_RATE_STATE 필요)")
    ap.add_argument("--round-gap", type=float, default=30, help="회차 사이 쉬는 초 (연속 모드)")
    a = ap.parse_args()

    names = a.only or ALL
    cfg = load_env()
    if a.paced:
        if not os.getenv("MIX_RATE_STATE"):
            sys.exit("--paced requires MIX_RATE_STATE; use bots/stream_all.py")
        from rate_control import RealRateGate
        cfg["pacer"] = RealRateGate(os.environ["MIX_RATE_STATE"], float(os.getenv("MIX_BOT_RATIO", "1.0")))
    if not a.dry_run and not (cfg["gw_key"] and cfg["admin_key"]):
        sys.exit("GATEWAY_API_KEY / ADMIN_API_KEY가 없습니다 (환경 변수 또는 루트 .env)")
    if a.slow > 3:
        print("주의: --slow 3 초과면 L4(1분)·R룰(5분) 시간 창을 넘을 수 있습니다.")

    print(f"대상 {a.dry_run and '계획' or cfg['base']} · 시나리오 {len(names)}개 · 간격 ×{a.slow}\n")
    if a.dry_run or a.once:
        for n in names:
            print(f"### {n}")
            run_one(cfg, n, a.slow, a.dry_run, a.paced)
        return

    # 연속 모드: 먼저 각 시나리오 실행 (L2 비중 추가), 그다음 가중 반복
    queue = names * MIN_RUNS_EACH + ["L2"] * 6
    random.shuffle(queue)
    try:
        while True:
            n = queue.pop() if queue else pick_weighted(names)
            print(f"### {n} {'(최소 횟수 보장)' if queue else ''}")
            run_one(cfg, n, a.slow, False, a.paced)
            time.sleep(a.round_gap)
    except KeyboardInterrupt:
        print(f"\n중지. 기록: {CSV_PATH}")


if __name__ == "__main__":
    main()
