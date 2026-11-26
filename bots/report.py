#!/usr/bin/env python3
"""
봇 성적표 — bot_runs/bots.csv(회차 기록) + MongoDB fds_cases(사건)를 맞대어 룰별 결과를 낸다.

판정 기준
- 1단계 R01~R07: 게이트웨이 응답이 기대값과 같으면 잡힘 (R룰은 정의서상 사건을 만들지 않음)
- 2단계 L1~L8: 그 회차의 BOT- 지갑에 "그 룰" 사건이 생기면 잡힘. 같이 걸린 다른 룰은 '겸침'으로 따로 표시
- bot2(정상 대량 패턴): 공격이 아님. 면제 지갑이라 전부 통과해야 정상 (오탐 확인용)
- L2·L6·L7은 Spark 재계산(recompute.status)도 표시
- 읽기 전용 (DB를 바꾸지 않음)

사용법 (저장소 맨 위 폴더에서, docker compose가 떠 있어야 함)
  python bots/report.py                          # 오늘 0시 이후 회차
  python bots/report.py --since "2026-11-26 14:00"
  결과: 화면 + bot_runs/report_YYYYMMDD_HHMM.md
"""
import argparse
import csv
import json
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "bot_runs" / "bots.csv"

R_RULES = ["R01", "R02", "R03", "R04", "R05", "R06", "R07"]
L_RULES = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8"]
NAME = {"R01": "초고빈도 송금", "R02": "소액 쪼개기", "R03": "자금 집금", "R04": "자금 분산",
        "R05": "고액 송금", "R06": "핑퐁 거래", "R07": "블랙리스트 거래",
        "L1": "반복 보류", "L2": "쪼개서 대량 송금", "L3": "장기 소액 쪼개기", "L4": "받자마자 재송금",
        "L5": "위험 지갑 접촉", "L6": "활동 급증", "L7": "새 상대 집중", "L8": "순환 거래",
        "BOT2_HOT": "핫월렛 정상 대량"}
EXPECT_ACTION = {"L1": "WALLET_FROZEN", "L2": "WALLET_FROZEN", "L3": "WALLET_FROZEN", "L8": "WALLET_FROZEN",
                 "L4": "REVIEW_PENDING", "L5": "REVIEW_PENDING", "L6": "REVIEW_PENDING", "L7": "REVIEW_PENDING"}
ACTION_KO = {"WALLET_FROZEN": "자동 동결", "REVIEW_PENDING": "검토 대기"}
RECOMPUTE_RULES = {"L2", "L6", "L7"}


def load_runs(since):
    if not CSV_PATH.exists():
        sys.exit(f"{CSV_PATH} 이 없습니다. 먼저 python bots/run_bots.py --once 를 돌리세요.")
    runs = defaultdict(list)
    with CSV_PATH.open(encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            try:
                r["_ts"] = datetime.strptime(r["start"], "%Y-%m-%d %H:%M:%S")
            except (KeyError, ValueError):
                continue
            if r["_ts"] < since or r.get("gateway_match", "").startswith("ABORTED"):
                continue
            runs[r["scenario"]].append(r)
    for v in runs.values():
        v.sort(key=lambda x: x["_ts"])
    return runs


def load_cases(wallets):
    if not wallets:
        return []
    js = ("print(JSON.stringify(db.fds_cases.find({violator_address:{$in:%s}},"
          "{_id:0,rule_id:1,violator_address:1,action_taken:1,'recompute.status':1}).toArray()))"
          % json.dumps(sorted(wallets)))
    cmd = ["docker", "compose", "exec", "-T", "mongo", "mongosh", "aegis", "--quiet", "--eval", js]
    out = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    if out.returncode != 0:
        sys.exit("Mongo 조회 실패 (docker compose가 떠 있는지 확인):\n" + out.stderr[-500:])
    return json.loads(out.stdout.strip().splitlines()[-1])


def judge(scn, run, cases):
    """한 회차 판정 → (잡힘 여부, 사건 조치, 재계산 상태, 겸침 룰들)"""
    mine = [c for c in cases if c.get("violator_address") in set(run["wallets"].split())]
    gw_ok = run["gateway_match"] == "OK"
    if scn in L_RULES:
        hit = [c for c in mine if c.get("rule_id") == scn]
        action = hit[0].get("action_taken", "") if hit else ""
        rec = (hit[0].get("recompute") or {}).get("status", "없음") if (hit and scn in RECOMPUTE_RULES) else ""
        side = sorted({c.get("rule_id") for c in mine if c.get("rule_id") != scn})
        return gw_ok and bool(hit), action, rec, side
    return gw_ok, "", "", []


def main():
    ap = argparse.ArgumentParser(description="봇 성적표")
    ap.add_argument("--since", help='이 시각 이후 회차만 (예: "2026-11-26 14:00"). 기본: 오늘 0시')
    a = ap.parse_args()
    since = (datetime.strptime(a.since, "%Y-%m-%d %H:%M") if a.since
             else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))

    runs = load_runs(since)
    wallets = {w for v in runs.values() for r in v for w in r["wallets"].split()}
    cases = load_cases(wallets)

    table, stage = [], {"R": [0, 0], "L": [0, 0]}
    run_hit = run_total = 0
    missing, missed = [], []
    for scn in R_RULES + L_RULES:
        st = "R" if scn in R_RULES else "L"
        stage[st][1] += 1
        rs = runs.get(scn, [])
        if not rs:
            missing.append(scn)
            table.append((scn, NAME[scn], "1단계 게이트웨이" if st == "R" else "2단계 Spark", "0", "0", "미실행", "", "", ""))
            continue
        results = [judge(scn, r, cases) for r in rs]
        n_hit = sum(1 for x in results if x[0])
        run_hit += n_hit
        run_total += len(rs)
        ok_any = n_hit > 0
        stage[st][0] += 1 if ok_any else 0
        if not ok_any:
            missed.append(scn)
        last_ok, action, rec, side = results[-1]
        if st == "R":
            detail = f"응답 {rs[-1]['actual_http']}"
        else:
            detail = ACTION_KO.get(action, action) if action else "사건 없음"
            if action and action != EXPECT_ACTION[scn]:
                detail += f" (기대 {ACTION_KO[EXPECT_ACTION[scn]]})"
        verdict = "잡힘" if last_ok else ("게이트웨이 불일치" if rs[-1]["gateway_match"] != "OK" else "놓침")
        side_all = sorted({s for x in results for s in x[3]})
        table.append((scn, NAME[scn], "1단계 게이트웨이" if st == "R" else "2단계 Spark",
                      str(len(rs)), str(n_hit), verdict, detail, rec, ",".join(side_all)))

    # bot2 (정상 패턴)
    b2 = runs.get("BOT2_HOT", [])
    b2_ok = sum(1 for r in b2 if r["gateway_match"] == "OK")
    b2_tx = sum(len(r["actual_http"].split()) for r in b2)

    caught = stage["R"][0] + stage["L"][0]
    lines = [f"# 봇 성적표", "",
             f"- 대상: {since:%Y-%m-%d %H:%M} 이후 회차 (기록: bot_runs/bots.csv, 사건: MongoDB fds_cases)",
             f"- 작성: {datetime.now():%Y-%m-%d %H:%M}", "",
             "## 요약", "",
             f"**재현율 {caught}/15** (공격 15종 중 탐지 · 1단계 {stage['R'][0]}/{stage['R'][1]} · 2단계 {stage['L'][0]}/{stage['L'][1]})",
             f"- 넣은 회차 {run_total}회 중 {run_hit}회 탐지"
             + (f" · 미실행 {', '.join(missing)}" if missing else "")
             + (f" · 놓침 {', '.join(missed)}" if missed else "")
             + (f" · bot2 정상 {b2_tx}건 모두 통과(오탐 0)" if b2 and b2_ok == len(b2) else ""),
             "",
             "## 룰별 결과", "",
             "| 룰 | 이름 | 단계 | 넣은 회차 | 잡힌 회차 | 최근 결과 | 최근 사건/응답 | Spark 재계산 | 겸침 |",
             "|---|---|---|---|---|---|---|---|---|"]
    lines += ["| " + " | ".join(row) + " |" for row in table]
    lines += ["", "## 읽는 법", "",
              "- **룰 기준**: 룰마다 최소 한 번 잡혔는가. 15/15 = 15개 공격 유형을 모두 탐지함",
              "- **회차 기준**: 같은 공격을 여러 번 넣었을 때 매번 잡았는가 (안정성)",
              "- **겸침**: 그 회차 지갑에 같이 생긴 다른 룰 사건. 재현율에는 넣지 않음 (예: L7 시나리오에서 L6 동시 적중)",
              "- **Spark 재계산**: L2·L6·L7 사건에 Spark가 원본 거래로 다시 계산한 결과. MATCH = 판정 값과 일치",
              "- 시드 B 면제: L3=R01·R02, L6=R01, L7=R01·R04, bot2=전체 — 1단계를 피했다고 가정하고 2단계만 따로 시험",
              "- 봇 지갑은 모두 BOT- 접두어 (실데이터 0x 주소와 분리). 정의서 기준(demo 기간) 공격 각본에 대한 재현율이며, 실제 범죄 데이터로 잰 값이 아님"]
    text = "\n".join(lines)
    print(text)
    out = ROOT / "bot_runs" / f"report_{datetime.now():%Y%m%d_%H%M}.md"
    out.parent.mkdir(exist_ok=True)
    out.write_text(text + "\n", encoding="utf-8")
    print(f"\n저장: {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()