"""
Aegis-FDS 온체인 수집기 (이더리움 메인넷 USDT Transfer)

모드
  기본           : 게이트웨이(FASTAPI_URL)로 전송 (기존 동작)
  --record PATH  : 전송하면서 원본도 JSONL로 저장
  --record-only  : 게이트웨이로 보내지 않고 JSONL 저장만 (기본 경로 data/onchain/)

환경 변수
  ALCHEMY_WSS_URL  (필수) wss://eth-mainnet.g.alchemy.com/v2/<키>  ← 코드에 키를 넣지 않는다
  FASTAPI_URL      (선택) 기본 http://localhost:8000/api/v1/transfers
  GATEWAY_API_KEY  (전송 모드에서 필요)

저장 형식 (하루 한 파일, UTC 기준: usdt_YYYYMMDD.jsonl)
  거래 한 줄: type=transfer, 원본 값(tx_hash·log_index·block·from·to·amount_raw) + 블록 시각 + 출처 정보
  상태 줄   : usdt_YYYYMMDD.meta.jsonl 에 연결·끊김(유실 구간 추적)·분당 통계
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Optional local mixed-demo accounting; disabled outside bots/stream_all.py.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bots.rate_control import record_real_ingress

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

USDT_CONTRACT_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7"
USDT_DECIMALS = 6
TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NETWORK = "ethereum-mainnet"
SOURCE = "alchemy_wss"
COLLECTOR_VERSION = "2.3"

# 게이트웨이로 보낼 때만 적용하는 최소 금액 (0원 Drop). 녹화는 전부 저장한다.
MIN_SEND_AMOUNT = 0.01

KNOWN_EXCHANGES = {
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance 14",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "Binance 15",
    "0xdfd5293d8e347dff59e90ef0cdb985e32943e777": "Binance 16",
    "0x5e032243d507c743b061ef021e4e07414fc712df": "Upbit 1",
    "0x0c6d7a736a27f2614f157793c74533d602832299": "Upbit 2",
    "0x97b98c99cdaa2b344d12f2c55359a35c0eb7163c": "Bithumb 1",
    "0x503828976d22510aad0201ac7ec88293211d23da": "Coinbase 1",
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "Kraken 4",
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "OKX 3",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_address(topic_hex: str) -> str:
    return "0x" + topic_hex.replace("0x", "")[-40:].lower()


def parse_amount_raw(data_hex: str) -> int:
    try:
        return int(data_hex or "0x0", 16)
    except ValueError:
        return 0


class DailyJsonl:
    """UTC 날짜가 바뀌면 새 파일로 넘어가는 JSONL 기록기. 한 줄씩 바로 flush한다."""

    def __init__(self, base_dir: Path, suffix: str):
        self.base_dir = base_dir
        self.suffix = suffix
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._day = None
        self._fh = None

    def write(self, obj: dict) -> None:
        day = utc_now().strftime("%Y%m%d")
        if day != self._day:
            if self._fh:
                self._fh.close()
            self._day = day
            self._fh = open(self.base_dir / f"usdt_{day}{self.suffix}", "a", encoding="utf-8")
        self._fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self._fh.flush()

    def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None


class Collector:
    def __init__(self, wss_url: str, send: bool, record_dir: Path | None, fastapi_url: str):
        self.wss_url = wss_url
        self.send = send
        self.fastapi_url = fastapi_url
        self.data = DailyJsonl(record_dir, ".jsonl") if record_dir else None
        self.meta = DailyJsonl(record_dir, ".meta.jsonl") if record_dir else None
        self.block_ts: dict[int, int] = {}      # 블록 번호 → 블록 시각(초). newHeads 구독으로 채움
        self.sub_logs = None
        self.sub_heads = None
        self.http = None
        self.stats = {"received": 0, "recorded": 0, "removed": 0, "sent": 0, "accepted": 0, "rejected": 0, "skipped_small": 0}
        self._last_report = time.time()
        self._last_block = None
        if self.send:
            import requests  # 전송 모드에서만 필요
            self.http = requests.Session()

    # ---------- 기록 ----------
    def log_meta(self, kind: str, **fields) -> None:
        line = {"type": kind, "at": iso(utc_now()), **fields}
        if self.meta:
            self.meta.write(line)
        print(f"[{line['at']}] {kind} {json.dumps(fields, ensure_ascii=False)}", flush=True)

    def build_record(self, log: dict) -> dict | None:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        block = int(log.get("blockNumber", "0x0"), 16)
        amount_raw = parse_amount_raw(log.get("data"))
        from_addr = parse_address(topics[1])
        to_addr = parse_address(topics[2])
        ts = self.block_ts.get(block)
        return {
            "type": "transfer",
            "network": NETWORK,
            "contract": USDT_CONTRACT_ADDRESS,
            "tx_hash": log.get("transactionHash", ""),
            "log_index": int(log.get("logIndex", "0x0"), 16),
            "block": block,
            "block_hash": log.get("blockHash"),
            "block_time": iso(datetime.fromtimestamp(ts, timezone.utc)) if ts else None,
            "from": from_addr,
            "to": to_addr,
            "amount_raw": str(amount_raw),          # 정밀도 손실 없는 원본 정수
            "decimals": USDT_DECIMALS,
            "amount": amount_raw / 10 ** USDT_DECIMALS,
            "from_label": KNOWN_EXCHANGES.get(from_addr),
            "to_label": KNOWN_EXCHANGES.get(to_addr),
            "removed": bool(log.get("removed", False)),   # 체인 재구성으로 취소된 로그면 true
            "collected_at": iso(utc_now()),
            "source": SOURCE,
            "collector_version": COLLECTOR_VERSION,
        }

    # ---------- 게이트웨이 전송 ----------
    def post_gateway(self, rec: dict) -> int:
        api_key = os.getenv("GATEWAY_API_KEY", "")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["X-API-Key"] = api_key
        payload = {
            "tx_hash": rec["tx_hash"],
            "log_index": rec["log_index"],
            "from": rec["from"],
            "to": rec["to"],
            "amount": round(rec["amount"], 2),
            "block": rec["block"],
            # 게이트웨이는 ±30초 검사 → 출금 요청으로 재생하므로 현재 시각
            "timestamp": iso(utc_now()),
        }
        try:
            status = self.http.post(self.fastapi_url, json=payload, headers=headers, timeout=5).status_code
            record_real_ingress(status)
            return status
        except Exception:
            return 0

    # ---------- 메시지 처리 ----------
    async def handle_message(self, data: dict) -> None:
        params = data.get("params") or {}
        sub = params.get("subscription")
        result = params.get("result")
        if result is None:
            return

        if sub == self.sub_heads:
            num = int(result.get("number", "0x0"), 16)
            self.block_ts[num] = int(result.get("timestamp", "0x0"), 16)
            if len(self.block_ts) > 200:           # 오래된 블록 시각은 버림
                for k in sorted(self.block_ts)[:-100]:
                    self.block_ts.pop(k, None)
            return

        if sub != self.sub_logs:
            return
        rec = self.build_record(result)
        if rec is None:
            return
        self.stats["received"] += 1
        self._last_block = rec["block"]
        if rec["removed"]:
            self.stats["removed"] += 1

        if self.data:
            self.data.write(rec)
            self.stats["recorded"] += 1

        if self.send and not rec["removed"]:
            if rec["amount"] < MIN_SEND_AMOUNT:
                self.stats["skipped_small"] += 1
            else:
                code = await asyncio.to_thread(self.post_gateway, rec)
                self.stats["sent"] += 1
                self.stats["accepted" if code in (200, 201) else "rejected"] += 1

        if time.time() - self._last_report >= 60:
            self.log_meta("stats", last_block=self._last_block, **self.stats)
            self._last_report = time.time()

    async def subscribe(self, ws, req_id: int, params: list) -> str:
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": req_id, "method": "eth_subscribe", "params": params}))
        while True:  # 구독 응답이 오기 전에 다른 알림이 섞일 수 있음
            msg = json.loads(await ws.recv())
            if msg.get("id") == req_id:
                if "error" in msg:
                    raise RuntimeError(f"구독 실패: {msg['error']}")
                return msg["result"]
            await self.handle_message(msg)

    async def run(self) -> None:
        import websockets

        retry_delay, max_delay = 2, 30
        disconnected_at = None
        while True:
            try:
                async with websockets.connect(self.wss_url, ping_interval=10, ping_timeout=10, close_timeout=5) as ws:
                    self.sub_heads = await self.subscribe(ws, 1, ["newHeads"])
                    self.sub_logs = await self.subscribe(
                        ws, 2, ["logs", {"address": USDT_CONTRACT_ADDRESS, "topics": [TRANSFER_EVENT_TOPIC]}]
                    )
                    self.log_meta(
                        "connected",
                        mode="record-only" if not self.send else ("send+record" if self.data else "send"),
                        gap_from=disconnected_at,       # 끊겼던 구간: 이 사이 블록은 유실 (To-Be: eth_getLogs 백필)
                        resume_after_block=self._last_block,
                    )
                    disconnected_at = None
                    retry_delay = 2
                    while True:
                        await self.handle_message(json.loads(await ws.recv()))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # 연결 끊김 포함
                if disconnected_at is None:
                    disconnected_at = iso(utc_now())
                self.log_meta("disconnected", error=type(e).__name__, detail=str(e)[:200], last_block=self._last_block)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 1.5, max_delay)

    def close(self) -> None:
        self.log_meta("stopped", last_block=self._last_block, **self.stats)
        for w in (self.data, self.meta):
            if w:
                w.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Aegis-FDS 온체인 USDT 수집기")
    ap.add_argument("--record-only", action="store_true", help="게이트웨이로 보내지 않고 파일 저장만")
    ap.add_argument("--record", metavar="DIR", help="원본 JSONL 저장 폴더 (기본: --record-only일 때 data/onchain)")
    args = ap.parse_args()

    wss = os.getenv("ALCHEMY_WSS_URL", "").strip()
    if not wss:
        print("❌ ALCHEMY_WSS_URL 환경 변수가 없습니다. 키는 코드에 넣지 말고 환경 변수로 주세요.", flush=True)
        sys.exit(1)

    record_dir = Path(args.record) if args.record else (Path("data/onchain") if args.record_only else None)
    send = not args.record_only
    fastapi_url = os.getenv("FASTAPI_URL", "http://localhost:8000/api/v1/transfers")

    print("=" * 72, flush=True)
    print(f"🛡️ Aegis-FDS 온체인 수집기 v{COLLECTOR_VERSION}", flush=True)
    print(f"   전송: {'끔' if not send else fastapi_url}", flush=True)
    print(f"   녹화: {record_dir.resolve() if record_dir else '끔'}", flush=True)
    print("=" * 72, flush=True)

    c = Collector(wss, send, record_dir, fastapi_url)
    try:
        asyncio.run(c.run())
    except KeyboardInterrupt:
        pass
    finally:
        c.close()


if __name__ == "__main__":
    main()
