#!/usr/bin/env python3
"""
Aegis-FDS 실데이터·이상거래 봇 혼합 실행기

- 실시간 수집 또는 녹화 파일 재생과 봇 주입기를 함께 실행한다.
- 기본 목표는 실데이터 1건당 봇 1건(100%). 실데이터 유입에 맞춰 봇 전송을 허용한다.
- 10초마다 실데이터·봇 건수와 초당 처리량, 비율을 bot_runs/mix-*.csv에 기록한다.
- 실시간 모드는 수집기를 직접 실행하므로 Kubernetes 수집기는 중지한 상태로 사용한다.

사용법 (저장소 맨 위 폴더에서)
  python bots/stream_all.py --live
  python bots/stream_all.py --input data/onchain/usdt_20261124.jsonl --bot-ratio 1.0
  옵션: --real-speed 1 (녹화 재생 배속) · --only L2 L6 L7 (실행할 봇 선택)

환경 변수 (없으면 저장소 루트 .env에서 읽음)
  GATEWAY_API_KEY, ADMIN_API_KEY, BOT_BASE_URL
  실시간 수집: ALCHEMY_WSS_URL
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from run_bots import ALL, load_env

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable


def read_count(path, key):
    try:
        return json.loads(path.read_text(encoding='utf-8')).get(key, 0)
    except (OSError, ValueError):
        return 0


def start_real(args, number, env):
    if args.live:
        return subprocess.Popen([PY, 'collector/collector_alchemy.py', '--record', str(args.record_dir)], cwd=ROOT, env=env)
    return subprocess.Popen([PY, 'replay_golden.py', '-r', f'{args.prefix}{number}',
        '-s', str(args.real_speed), '--input', str(args.input), '--gateway-url', env['FASTAPI_URL']], cwd=ROOT, env=env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--real-speed', type=float, default=1)
    parser.add_argument('--prefix', default='LOOP')
    parser.add_argument('--input', type=Path, default=ROOT/'data/onchain/golden_r3_30min.jsonl', help='Replay JSONL file, including an AWS collector recording')
    parser.add_argument('--bot-ratio', type=float, default=1.0, help='bot / real ingress target')
    parser.add_argument('--only', nargs='+', choices=ALL, default=['L2','L6','L7','L8','BOT2_HOT'])
    parser.add_argument('--record-dir', type=Path, default=ROOT/'data/onchain')
    args = parser.parse_args()
    if not 0 < args.bot_ratio <= 1 or args.real_speed <= 0:
        parser.error('require 0 < bot-ratio <= 1 and real-speed > 0')
    args.input = args.input.resolve()
    if not args.live and not args.input.is_file():
        parser.error(f'missing replay input: {args.input}; supply --input or use --live')
    cfg = load_env()
    if not cfg['gw_key'] or not cfg['admin_key']:
        parser.error('GATEWAY_API_KEY / ADMIN_API_KEY required in environment or .env')
    env = dict(os.environ)
    dotenv = ROOT/'.env'
    if dotenv.exists():
        for line in dotenv.read_text(encoding='utf-8-sig').splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                key, value = line.split('=', 1)
                env.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    if args.live and (not env.get('ALCHEMY_WSS_URL') or 'CHANGE_ME' in env['ALCHEMY_WSS_URL']):
        parser.error('set a valid ALCHEMY_WSS_URL before --live')
    out = ROOT/'bot_runs'
    out.mkdir(exist_ok=True)
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + f'-{os.getpid()}'
    state = out/f'mix-{run}.json'
    state.write_text(json.dumps({'real_count':0, 'updated_at':0}), encoding='utf-8')
    env.update(MIX_RATE_STATE=str(state), MIX_BOT_RATIO=str(args.bot_ratio),
        GATEWAY_API_KEY=cfg['gw_key'], ADMIN_API_KEY=cfg['admin_key'],
        FASTAPI_URL=cfg['base']+'/api/v1/transfers', BOT_BASE_URL=cfg['base'])
    number = 1
    real = bots = None
    csv_path = out/f'mix-{run}.csv'
    bot_state = state.with_name(state.name+'.bot.json')
    try:
        bots = subprocess.Popen([PY, 'bots/run_bots.py', '--continuous', '--paced',
            '--round-gap', '0', '--only', *args.only], cwd=ROOT, env=env)
        real = start_real(args, number, env)
        print(f'[mix] bot/real target={args.bot_ratio:.2f}; real ingress pause => bots pause', flush=True)
        previous = (0, 0)
        last = time.monotonic()
        with csv_path.open('w', newline='', encoding='utf-8') as target:
            writer = csv.writer(target)
            writer.writerow(['at_utc','real_total','bot_permits','real_tps','bot_tps','bot_real_ratio'])
            while True:
                time.sleep(10)
                if bots.poll() is not None:
                    raise RuntimeError(f'bot stopped: code={bots.returncode}; fix before restarting mix')
                if real.poll() is not None:
                    if args.live or real.returncode != 0:
                        raise RuntimeError(f'real producer stopped: code={real.returncode}')
                    number += 1
                    real = start_real(args, number, env)
                now = time.monotonic()
                counts = (read_count(state, 'real_count'), read_count(bot_state, 'bot_count'))
                r, b = ((counts[i]-previous[i])/(now-last) for i in range(2))
                ratio = b/r if r else None
                writer.writerow([datetime.now(timezone.utc).isoformat(), *counts,
                    round(r,3), round(b,3), round(ratio,3) if ratio is not None else ''])
                target.flush()
                print(f'[mix] real={r:.2f}/s bot={b:.2f}/s bot/real={ratio if ratio is not None else "paused"}; totals={counts}', flush=True)
                previous, last = counts, now
    except KeyboardInterrupt:
        print('\n[mix] stopping producers', flush=True)
    finally:
        for process in (real, bots):
            if process is not None and process.poll() is None:
                process.terminate()
        for process in (real, bots):
            if process is not None:
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        print(f'[mix] measurements: {csv_path}', flush=True)


if __name__ == '__main__':
    main()
