#!/usr/bin/env python3
"""
실데이터 + 이상거래 봇을 동시에 계속 흘리기 (Aegis-FDS 시연·측정용)

- 실데이터: 기본은 메인넷 녹화본(골든 30분)을 끝까지 재생한 뒤 이름을 바꿔 다시 재생 (반복)
           --live 를 주면 녹화본 대신 실시간 수집기(collector_alchemy.py)를 켠다
- 봇: bots/run_bots.py --continuous (BOT- 지갑, 각 3회 보장 후 60:40)
- Ctrl+C 한 번으로 둘 다 멈춘다

사용법 (저장소 맨 위 폴더에서)
  python bots/stream_all.py                      # 녹화본 1배속 반복 + 봇
  python bots/stream_all.py --real-speed 0.5     # 녹화본 0.5배속
  python bots/stream_all.py --live               # 실시간 수집기 + 봇 (ALCHEMY_WSS_URL 필요)
  python bots/stream_all.py --bot-slow 2 --round-gap 30

주의: 시작 전에 면제 12곳 등록(seed_whitelist.py)과 화면 백엔드 연결을 확인한다.
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable


def start_real(a, n):
    if a.live:
        return subprocess.Popen([PY, "collector/collector_alchemy.py"], cwd=ROOT)
    # 같은 이름으로 다시 재생하면 중복으로 막히므로 회차마다 이름을 바꾼다
    return subprocess.Popen([PY, "replay_golden.py", "-r", f"{a.prefix}{n}", "-s", str(a.real_speed)], cwd=ROOT)


def main():
    ap = argparse.ArgumentParser(description="실데이터 + 봇 동시 주입")
    ap.add_argument("--live", action="store_true", help="녹화본 대신 실시간 수집기")
    ap.add_argument("--real-speed", type=float, default=1.0, help="녹화본 재생 배속 (기본 1.0)")
    ap.add_argument("--prefix", default="LOOP", help="녹화본 회차 이름 앞부분 (기본 LOOP → LOOP1, LOOP2 …)")
    ap.add_argument("--bot-slow", type=float, default=2.0, help="봇 거래 간격 배수 (기본 2.0)")
    ap.add_argument("--round-gap", type=float, default=30, help="봇 회차 사이 쉬는 초")
    a = ap.parse_args()

    if not a.live and not (ROOT / "replay_golden.py").exists():
        sys.exit("replay_golden.py 가 저장소 맨 위에 없습니다")
    if not a.live and not (ROOT / "data/onchain/golden_r3_30min.jsonl").exists():
        sys.exit("data/onchain/golden_r3_30min.jsonl 이 없습니다")

    n = 1
    real = start_real(a, n)
    bots = subprocess.Popen([PY, "bots/run_bots.py", "--continuous",
                             "--slow", str(a.bot_slow), "--round-gap", str(a.round_gap)], cwd=ROOT)
    print(f"[시작] 실데이터={'실시간 수집기' if a.live else f'녹화본 {a.prefix}{n}'} · 봇 연속 모드 (Ctrl+C로 중지)")
    try:
        while True:
            time.sleep(5)
            if bots.poll() is not None:
                print(f"[봇 종료됨 code={bots.returncode}] 다시 시작")
                bots = subprocess.Popen([PY, "bots/run_bots.py", "--continuous",
                                         "--slow", str(a.bot_slow), "--round-gap", str(a.round_gap)], cwd=ROOT)
            if real.poll() is not None:
                if a.live:
                    print(f"[수집기 종료됨 code={real.returncode}] 5초 뒤 다시 시작")
                else:
                    n += 1
                    print(f"[녹화본 끝] 다음 회차 {a.prefix}{n} 재생")
                real = start_real(a, n)
    except KeyboardInterrupt:
        print("\n[중지 중] 실데이터·봇 종료")
        for p in (real, bots):
            if p.poll() is None:
                p.terminate()
        for p in (real, bots):
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        print("[중지 완료] 봇 기록: bot_runs/bots.csv")


if __name__ == "__main__":
    main()
