"""1단계 즉시 판단: 지갑별 최근 5분 기록으로 R01~R04, R06을 계산하고 3분 보류를 건다.

지갑마다 Redis Sorted Set에 거래를 쌓는다 (점수 = 거래 시각 ms).
거래가 올 때마다 그 시각부터 5분 전까지를 정확히 센다. 슬라이딩 윈도우는 쓰지 않는다.
"""
from dataclasses import dataclass, field

import redis.asyncio as redis

from app.blacklist import FROZEN_KEY
from app.whitelist import whitelist_key

LOOKBACK_MS = 5 * 60 * 1000     # 거래 시각 기준 최근 5분
HOLD_MS = 3 * 60 * 1000         # 보류 3분
HOLD_SECONDS = HOLD_MS // 1000
KEEP_MS = LOOKBACK_MS + 60_000  # 시각 오차(±30초) 때문에 1분 더 보관

R01_MIN_SENDS = 5               # R01 초고빈도: 송금 5건 이상
R02_MIN_SPLITS = 3              # R02 쪼개기: 900~990 USDT 송금 3회 이상
R02_MIN, R02_MAX = 900, 990
R03_MIN_SENDERS = 5             # R03 집금: 서로 다른 보낸 지갑 5개 이상
R04_MIN_RECEIVERS = 5           # R04 분산: 서로 다른 받는 지갑 5개 이상


def hold_key(wallet: str) -> str:
    """보류 중인 지갑. 값은 보류한 룰, 3분 뒤 자동 삭제."""
    return f"fds:hold:{wallet}"


def _window_key(kind: str, wallet: str) -> str:
    return f"fds:w:{kind}:{wallet}"


# 검사와 기록을 Redis 안에서 한 번에 처리한다 (동시 요청에도 개수가 꼬이지 않음)
# KEYS: 1 send:{from} 2 split:{from} 3 out:{from} 4 in:{to} 5 out:{to}
#       6 hold:{from} 7 hold:{to} 8 동결 목록
#       9~13 화이트리스트 R01 R02 R03 R04 R06
# ARGV: 1 from 2 to 3 거래ID 4 거래 시각 5 현재 시각 6 900~990 여부
#       7 5분 8 보류 시간 9 보관 시간 10~13 R01 R02 R03 R04 기준
EVALUATE_LUA = """
local from, to, tx = ARGV[1], ARGV[2], ARGV[3]
local ts, now = tonumber(ARGV[4]), tonumber(ARGV[5])
local is_split = ARGV[6] == '1'
local hold_ms, keep = tonumber(ARGV[8]), tonumber(ARGV[9])
local lo = ts - tonumber(ARGV[7])

-- 5분 넘게 지난 기록 정리
for i = 1, 5 do redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', now - keep) end

-- 동시에 들어온 다른 요청이 먼저 보류를 걸었으면 그 사유로 막는다
local held = redis.call('GET', KEYS[6])
if held then return {'HELD', held, 0, 0, 0, 0} end

-- 이번 거래를 포함한 5분 개수
local function count_with(key, member)
  local n = redis.call('ZCOUNT', key, lo, '+inf')
  local s = redis.call('ZSCORE', key, member)
  if (not s) or tonumber(s) < lo then n = n + 1 end
  return n
end
local function exempt(key, wallet) return redis.call('SISMEMBER', key, wallet) == 1 end

-- 1) 보내는 지갑 룰: 걸리면 이 거래는 거절하고 기록하지 않는다
local sends = count_with(KEYS[1], tx)
local splits = 0
if is_split then splits = count_with(KEYS[2], tx) end
local receivers = count_with(KEYS[3], to)

local hits = {}
if sends >= tonumber(ARGV[10]) and not exempt(KEYS[9], from) then table.insert(hits, 'R01') end
if is_split and splits >= tonumber(ARGV[11]) and not exempt(KEYS[10], from) then table.insert(hits, 'R02') end
if receivers >= tonumber(ARGV[13]) and not exempt(KEYS[12], from) then table.insert(hits, 'R04') end
if #hits > 0 then
  redis.call('SET', KEYS[6], hits[1], 'PX', hold_ms)
  return {'HOLD_SENDER', table.concat(hits, ','), sends, splits, receivers, 0}
end

-- 2) 통과: 5분 기록에 추가 (서로 다른 지갑은 가장 최근 시각으로 갱신)
redis.call('ZADD', KEYS[1], ts, tx)
if is_split then redis.call('ZADD', KEYS[2], ts, tx) end
redis.call('ZADD', KEYS[3], 'GT', ts, to)
redis.call('ZADD', KEYS[4], 'GT', ts, from)
for i = 1, 4 do redis.call('PEXPIRE', KEYS[i], keep) end

-- 3) 받는 지갑 룰: 거래는 통과시키고 받는 지갑만 보류한다 (입금은 항상 허용)
local senders = redis.call('ZCOUNT', KEYS[4], lo, '+inf')
local rhits = {}
if from ~= to and redis.call('SISMEMBER', KEYS[8], to) == 0 then
  if senders >= tonumber(ARGV[12]) and not exempt(KEYS[11], to) then table.insert(rhits, 'R03') end
  -- R06 핑퐁: 받는 지갑(A)이 5분 안에 보내는 지갑(B)에게 먼저 보낸 적이 있음
  local back = redis.call('ZSCORE', KEYS[5], from)
  if back and tonumber(back) >= lo and not exempt(KEYS[13], to) then table.insert(rhits, 'R06') end
end
-- 이미 보류 중인 받는 지갑은 다시 보류하지 않는다
if #rhits > 0 and redis.call('SET', KEYS[7], rhits[1], 'PX', hold_ms, 'NX') then
  return {'HOLD_RECEIVER', table.concat(rhits, ','), sends, splits, receivers, senders}
end
return {'PASS', '', sends, splits, receivers, senders}
"""


@dataclass
class Decision:
    kind: str                    # PASS, HOLD_SENDER, HOLD_RECEIVER, HELD
    rules: list[str] = field(default_factory=list)
    evidence: dict = field(default_factory=dict)

    @property
    def rule(self) -> str | None:
        return self.rules[0] if self.rules else None


@dataclass
class Hold:
    """새로 걸린 보류 하나. fds_holds와 fds.holds.v1에 그대로 기록한다."""
    wallet: str
    rules: list[str]
    side: str                    # SENDER: 보낸 지갑 보류(거래 거절), RECEIVER: 받는 지갑 보류(거래 통과)
    evidence: dict

    @property
    def rule(self) -> str:
        return self.rules[0]


_script = None


def tx_id(req) -> str:
    return f"{req.tx_hash}:{req.log_index}"


async def evaluate(r: redis.Redis, req, amount, now_ms: int) -> Decision:
    """R01~R04, R06을 계산한다. 보내는 지갑이 걸리면 HOLD_SENDER, 받는 지갑이 걸리면 HOLD_RECEIVER."""
    global _script
    if _script is None:
        _script = r.register_script(EVALUATE_LUA)

    sender, receiver = req.from_address, req.to_address
    keys = [
        _window_key("send", sender),
        _window_key("split", sender),
        _window_key("out", sender),
        _window_key("in", receiver),
        _window_key("out", receiver),
        hold_key(sender),
        hold_key(receiver),
        FROZEN_KEY,
        *(whitelist_key(rule) for rule in ("R01", "R02", "R03", "R04", "R06")),
    ]
    ts_ms = int(req.timestamp.timestamp() * 1000)
    args = [
        sender, receiver, tx_id(req), ts_ms, now_ms,
        1 if R02_MIN <= amount <= R02_MAX else 0,
        LOOKBACK_MS, HOLD_MS, KEEP_MS,
        R01_MIN_SENDS, R02_MIN_SPLITS, R03_MIN_SENDERS, R04_MIN_RECEIVERS,
    ]
    kind, rules, sends, splits, receivers, senders = await _script(keys=keys, args=args)
    evidence = {
        "send_count_5m": int(sends),
        "split_count_5m": int(splits),
        "receiver_count_5m": int(receivers),
        "sender_count_5m": int(senders),
    }
    return Decision(kind, rules.split(",") if rules else [], evidence)


async def hold_sender(r: redis.Redis, wallet: str, rule: str) -> bool:
    """R05처럼 Lua 밖에서 거는 보류. 이미 보류 중이면 False."""
    return bool(await r.set(hold_key(wallet), rule, px=HOLD_MS, nx=True))


async def undo_record(r: redis.Redis, req, receiver_held: bool) -> None:
    """Mongo 저장이 실패한 거래를 5분 기록과 받는 지갑 보류에서 되돌린다 (최선의 노력)."""
    pipe = r.pipeline(transaction=False)
    pipe.zrem(_window_key("send", req.from_address), tx_id(req))
    pipe.zrem(_window_key("split", req.from_address), tx_id(req))
    if receiver_held:
        pipe.delete(hold_key(req.to_address))
    await pipe.execute()
