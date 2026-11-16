"""1단계 즉시 판단: R01~R06을 계산하고 3분 보류를 건다. (R07 블랙리스트는 wallet_status.py)

    R01 초고빈도   5분 안에 송금 5건 이상                → 보내는 지갑 보류, 거래 거절
    R02 쪼개기     5분 안에 900~990 USDT 송금 3회 이상   → 보내는 지갑 보류, 거래 거절
    R03 집금       5분 안에 서로 다른 5개 지갑에게서 받음 → 받는 지갑 보류, 거래는 통과
    R04 분산       5분 안에 서로 다른 5개 지갑에게 보냄  → 보내는 지갑 보류, 거래 거절
    R05 대량 송금  한 번에 50,000 USDT 이상              → 보내는 지갑 보류, 거래 거절
    R06 핑퐁       A→B 뒤 5분 안에 B→A                   → 받는 지갑(A) 보류, 거래는 통과

R01~R04·R06은 지갑별 최근 5분 기록이 필요해서 Redis Lua 하나로 계산한다 (동시 요청에도 개수가 꼬이지 않음).
지갑마다 Redis Sorted Set에 거래를 쌓고 (점수 = 거래 시각 ms), 거래가 올 때마다 그 시각부터 5분 전까지 정확히 센다.
R05는 금액만 보면 되어 Lua 밖에서 먼저 판단한다.
"""
from dataclasses import dataclass, field
from decimal import Decimal

import redis.asyncio as redis

from app.whitelist import is_whitelisted
from common.keys import FROZEN_KEY, WHALE_LIMIT_USDT, whitelist_key

# ---------- 공통: 기간 ----------
LOOKBACK_MS = 5 * 60 * 1000     # 거래 시각 기준 최근 5분
HOLD_MS = 3 * 60 * 1000         # 보류 3분
HOLD_SECONDS = HOLD_MS // 1000
KEEP_MS = LOOKBACK_MS + 60_000  # 시각 오차(±30초) 때문에 1분 더 보관

# ---------- 룰별 기준 ----------
R01_MIN_SENDS = 5               # R01 초고빈도: 송금 5건 이상
R02_MIN_SPLITS = 3              # R02 쪼개기: 900~990 USDT 송금 3회 이상
R02_MIN, R02_MAX = 900, 990
R03_MIN_SENDERS = 5             # R03 집금: 서로 다른 보낸 지갑 5개 이상
R04_MIN_RECEIVERS = 5           # R04 분산: 서로 다른 받는 지갑 5개 이상
WHALE_LIMIT = Decimal(WHALE_LIMIT_USDT)  # R05 대량 송금: 5만 이상 (금액 비교는 Decimal로)
# R06 핑퐁은 기준 숫자 없음 (5분 안에 되돌려 보냈는지만 본다)

# Lua 안에서 화이트리스트를 확인하는 룰 (R05는 Lua 밖에서 확인)
LUA_WHITELIST_RULES = ("R01", "R02", "R03", "R04", "R06")


# ---------- Redis 키 (게이트웨이만 씀) ----------

def hold_key(wallet: str) -> str:
    """보류 중인 지갑. 값은 보류한 룰, 3분 뒤 자동 삭제."""
    return f"fds:hold:{wallet}"


def _window_key(kind: str, wallet: str) -> str:
    """지갑별 최근 5분 기록. send: 송금, split: 900~990 송금, out: 받은 지갑들, in: 보낸 지갑들."""
    return f"fds:w:{kind}:{wallet}"


# ---------- R01~R04·R06: Redis Lua ----------
# 검사와 기록을 Redis 안에서 한 번에 처리한다. KEYS·ARGV 순서는 evaluate()의 keys·args와 같다.
EVALUATE_LUA = """
-- 입력 (번호 대신 이름으로 쓴다)
local K_SEND_FROM, K_SPLIT_FROM, K_OUT_FROM = KEYS[1], KEYS[2], KEYS[3]  -- 보내는 지갑의 5분 기록
local K_IN_TO, K_OUT_TO = KEYS[4], KEYS[5]                               -- 받는 지갑의 5분 기록
local K_HOLD_FROM, K_HOLD_TO, K_FROZEN = KEYS[6], KEYS[7], KEYS[8]
local W_R01, W_R02, W_R03, W_R04, W_R06 = KEYS[9], KEYS[10], KEYS[11], KEYS[12], KEYS[13]  -- 화이트리스트

local from, to, tx = ARGV[1], ARGV[2], ARGV[3]
local ts, now = tonumber(ARGV[4]), tonumber(ARGV[5])
local is_split = ARGV[6] == '1'
local lookback, hold_ms, keep = tonumber(ARGV[7]), tonumber(ARGV[8]), tonumber(ARGV[9])
local R01_MIN, R02_MIN, R03_MIN, R04_MIN = tonumber(ARGV[10]), tonumber(ARGV[11]), tonumber(ARGV[12]), tonumber(ARGV[13])
local lo = ts - lookback

-- 0) 준비: 5분 넘게 지난 기록 정리
for i = 1, 5 do redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', now - keep) end

-- 동시에 들어온 다른 요청이 먼저 보류를 걸었으면 그 사유로 막는다
local held = redis.call('GET', K_HOLD_FROM)
if held then return {'HELD', held, 0, 0, 0, 0} end

local function count_with(key, member)  -- 이번 거래를 포함한 5분 개수
  local n = redis.call('ZCOUNT', key, lo, '+inf')
  local s = redis.call('ZSCORE', key, member)
  if (not s) or tonumber(s) < lo then n = n + 1 end
  return n
end
local function exempt(key, wallet) return redis.call('SISMEMBER', key, wallet) == 1 end

-- 1) 보내는 지갑 룰: 걸리면 이 거래는 거절하고 기록하지 않는다
local sends = count_with(K_SEND_FROM, tx)
local splits = 0
if is_split then splits = count_with(K_SPLIT_FROM, tx) end
local receivers = count_with(K_OUT_FROM, to)

local hits = {}
-- R01 초고빈도: 5분 송금 수
if sends >= R01_MIN and not exempt(W_R01, from) then table.insert(hits, 'R01') end
-- R02 쪼개기: 5분 900~990 송금 수
if is_split and splits >= R02_MIN and not exempt(W_R02, from) then table.insert(hits, 'R02') end
-- R04 분산: 5분 동안 보낸 서로 다른 지갑 수
if receivers >= R04_MIN and not exempt(W_R04, from) then table.insert(hits, 'R04') end

if #hits > 0 then
  redis.call('SET', K_HOLD_FROM, hits[1], 'PX', hold_ms)
  return {'HOLD_SENDER', table.concat(hits, ','), sends, splits, receivers, 0}
end

-- 2) 통과: 5분 기록에 추가 (서로 다른 지갑은 가장 최근 시각으로 갱신)
redis.call('ZADD', K_SEND_FROM, ts, tx)
if is_split then redis.call('ZADD', K_SPLIT_FROM, ts, tx) end
redis.call('ZADD', K_OUT_FROM, 'GT', ts, to)
redis.call('ZADD', K_IN_TO, 'GT', ts, from)
for i = 1, 4 do redis.call('PEXPIRE', KEYS[i], keep) end

-- 3) 받는 지갑 룰: 거래는 통과시키고 받는 지갑만 보류한다 (입금은 항상 허용, 동결 지갑은 제외)
local senders = redis.call('ZCOUNT', K_IN_TO, lo, '+inf')
local rhits = {}
if from ~= to and redis.call('SISMEMBER', K_FROZEN, to) == 0 then
  -- R03 집금: 5분 동안 받는 지갑에게 보낸 서로 다른 지갑 수
  if senders >= R03_MIN and not exempt(W_R03, to) then table.insert(rhits, 'R03') end
  -- R06 핑퐁: 받는 지갑(A)이 5분 안에 보내는 지갑(B)에게 먼저 보낸 적이 있음
  local back = redis.call('ZSCORE', K_OUT_TO, from)
  if back and tonumber(back) >= lo and not exempt(W_R06, to) then table.insert(rhits, 'R06') end
end
-- 이미 보류 중인 받는 지갑은 다시 보류하지 않는다
if #rhits > 0 and redis.call('SET', K_HOLD_TO, rhits[1], 'PX', hold_ms, 'NX') then
  return {'HOLD_RECEIVER', table.concat(rhits, ','), sends, splits, receivers, senders}
end
return {'PASS', '', sends, splits, receivers, senders}
"""


# ---------- 판단 결과 ----------

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


# ---------- R01~R06 판단 (main.py가 부르는 함수) ----------

async def evaluate(r: redis.Redis, req, amount, now_ms: int) -> Decision:
    """R01~R06을 판단한다. R05를 먼저 보고, 아니면 Lua로 R01~R04·R06.
    보내는 지갑이 걸리면 HOLD_SENDER, 받는 지갑이 걸리면 HOLD_RECEIVER, 이미 보류 중이면 HELD."""
    whale = await _check_r05(r, req, amount)
    if whale:
        return whale
    return await _check_r01_to_r06(r, req, amount, now_ms)


async def _check_r05(r: redis.Redis, req, amount) -> Decision | None:
    """R05 대량 송금: 5만 이상이면 거래를 거절하고 보내는 지갑을 보류한다. 해당 없으면 None."""
    if amount < WHALE_LIMIT or await is_whitelisted(r, "R05", req.from_address):
        return None
    if await hold_sender(r, req.from_address, "R05"):
        return Decision("HOLD_SENDER", ["R05"], {"amount": str(amount)})
    # 동시에 들어온 다른 요청이 먼저 보류를 건 경우
    return Decision("HELD", ["R05"])


_script = None


async def _check_r01_to_r06(r: redis.Redis, req, amount, now_ms: int) -> Decision:
    """R01~R04·R06: 위의 Lua를 실행한다 (R05 제외)."""
    global _script
    if _script is None:
        _script = r.register_script(EVALUATE_LUA)

    sender, receiver = req.from_address, req.to_address
    keys = [
        _window_key("send", sender),     # 1 K_SEND_FROM
        _window_key("split", sender),    # 2 K_SPLIT_FROM
        _window_key("out", sender),      # 3 K_OUT_FROM
        _window_key("in", receiver),     # 4 K_IN_TO
        _window_key("out", receiver),    # 5 K_OUT_TO
        hold_key(sender),                # 6 K_HOLD_FROM
        hold_key(receiver),              # 7 K_HOLD_TO
        FROZEN_KEY,                      # 8 K_FROZEN
        *(whitelist_key(rule) for rule in LUA_WHITELIST_RULES),  # 9~13 W_R01 W_R02 W_R03 W_R04 W_R06
    ]
    ts_ms = int(req.timestamp.timestamp() * 1000)
    args = [
        sender, receiver, tx_id(req),                 # 1 from, 2 to, 3 tx
        ts_ms, now_ms,                                # 4 ts, 5 now
        1 if R02_MIN <= amount <= R02_MAX else 0,     # 6 is_split
        LOOKBACK_MS, HOLD_MS, KEEP_MS,                # 7 lookback, 8 hold_ms, 9 keep
        R01_MIN_SENDS, R02_MIN_SPLITS, R03_MIN_SENDERS, R04_MIN_RECEIVERS,  # 10~13 R01_MIN R02_MIN R03_MIN R04_MIN
    ]
    kind, rules, sends, splits, receivers, senders = await _script(keys=keys, args=args)
    evidence = {
        "send_count_5m": int(sends),
        "split_count_5m": int(splits),
        "receiver_count_5m": int(receivers),
        "sender_count_5m": int(senders),
    }
    return Decision(kind, rules.split(",") if rules else [], evidence)


# ---------- 보류 도우미 ----------

def tx_id(req) -> str:
    return f"{req.tx_hash}:{req.log_index}"


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
