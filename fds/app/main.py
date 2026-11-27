"""게이트웨이: 송금 요청을 받아 1단계(R01~R07)를 판단하고, 통과하면 Mongo와 outbox에 저장한다.

    룰 위치  R01~R06: holds.py / R07(+동결, 보류 중): wallet_status.py
    직원 API(수동 동결·이상 없음·동결 해제): admin.py / 관제 조회 API: admin_views.py
    STR 초안: str_draft.py / 실시간 경보 WebSocket: live_alerts.py / [EVAL] 평가: eval_poc.py
    처리 순서는 create_transfer()의 0) ~ 5) 주석을 보면 된다.
"""
import asyncio
import json
import logging
import os
import secrets
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import BackgroundTasks, FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, BaseModel, Field
from pymongo.errors import DuplicateKeyError
from redis.exceptions import RedisError

from app import admin, admin_views, live_alerts, str_draft
from app import eval_poc  # [EVAL] PoC 평가 전용 (운영에서 뺄 때 이 줄과 아래 eval_poc.install 줄 삭제)
from app.holds import HOLD_SECONDS, Hold, evaluate, hold_key, undo_record
from app.idempotency import claim_idempotency, release_idempotency
from app.mongo_client import mongo_client
from app.pod_identity import PodIdentityMiddleware
from app.redis_client import redis_client
from app.store import ensure_indexes, execute_transfer, parse_amount, save_rejected, save_sender_hold
from app.wallet_status import BLACKLISTED_RECEIVER, check_wallets

# ---------- 게이트웨이 보안 ----------

API_KEY = os.getenv("GATEWAY_API_KEY")
if not API_KEY:
    # 키 없이 실수로 열린 채 실행되는 것을 막는다
    raise RuntimeError("GATEWAY_API_KEY가 설정되지 않았습니다")


def is_valid_key(value: str | None) -> bool:
    # 비교 시간으로 키를 추측하지 못하게 compare_digest를 쓴다
    return value is not None and secrets.compare_digest(value.encode(), API_KEY.encode())


MAX_CLOCK_SKEW = timedelta(seconds=30)  # 현재 시각과 허용하는 차이

# ---------- 로그 설정 ----------

os.makedirs("logs", exist_ok=True)
logger = logging.getLogger("aegis.gateway")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    fmt = logging.Formatter("%(message)s")
    fh = logging.FileHandler("logs/gateway.log", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)


def log_result(req, http_code: int, body: dict, **extra):
    record = {
        "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "http_code": http_code,
        **body,
        **extra,
        "request": req.model_dump(mode="json", by_alias=True),
    }
    line = json.dumps(record, ensure_ascii=False)
    if http_code < 400:
        logger.info(line)
    else:
        logger.warning(line)


# ---------- 앱 시작·종료 ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await redis_client.ping()
    await ensure_indexes()
    await mongo_client.admin.command("ping")
    logger.info('{"event": "startup", "redis": "ok", "mongo": "ok"}')
    # 관제 화면 실시간 경보: 새 케이스·케이스 변경을 지켜본다 → live_alerts.py
    watcher = asyncio.create_task(live_alerts.watch_cases())
    yield
    watcher.cancel()
    await redis_client.aclose()
    await mongo_client.close()

app = FastAPI(title="AEGIS-FDS Gateway", lifespan=lifespan)
app.add_middleware(PodIdentityMiddleware)
# 직원 API (수동 동결·이상 없음·동결 해제·지갑 상태)  → admin.py
admin.install(app)
admin_views.install(app)
# STR 작성 근거 초안 (케이스 증거 그대로 + 계산 출처, 분석관 의견 저장)  → str_draft.py
str_draft.install(app)
# 실시간 경보 WebSocket /ws/admin  → live_alerts.py
live_alerts.install(app)
# [EVAL] 봇 정답 라벨·룰별 성적표. EVAL_ENABLED=0이면 주소가 생기지 않는다 → app/eval_poc.py
eval_poc.install(app)


# ---------- 쿠버네티스 헬스체크 프로브 (무인증) ----------

@app.get("/healthz")
async def healthz():
    """Liveness probe: 프로세스 생존 및 FastAPI 웹서버 응답 확인."""
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    """Readiness probe: Redis 및 MongoDB 정상 핑 확인."""
    try:
        await redis_client.ping()
        await mongo_client.admin.command("ping")
        return {"status": "ready"}
    except Exception as e:
        logger.error(f"Readiness check failed: {type(e).__name__} {e}")
        return JSONResponse(status_code=503, content={"status": "not_ready", "reason": type(e).__name__})


# ---------- 요청 형식 ----------

class TransferRequest(BaseModel):
    # Kafka 인입 규격 필드. from은 파이썬 예약어라 별칭으로 받는다.
    tx_hash: str = Field(min_length=1)
    # 거래 안의 전송 순번. 수집기가 보내기 전까지 임시로 선택값으로 둔다.
    log_index: int | None = Field(default=None, ge=0, le=2**31 - 1)
    from_address: str = Field(alias="from", min_length=1)
    to_address: str = Field(alias="to", min_length=1)
    amount: float
    block: int = Field(ge=0, le=2**63 - 1)
    timestamp: AwareDatetime
    exchange_name: str | None = None


# ---------- 거절 응답 ----------

def reject(req, http_code, reason, background_tasks, retry_after=None, block_reason=None):
    body = {"status": "REJECTED", "reason": reason}
    if retry_after:
        # 보류 중이면 남은 초를 헤더(Retry-After)와 본문에 같이 준다 (프론트는 본문만 읽어도 됨)
        body["retry_after"] = retry_after
    log_result(req, http_code, body)
    # 응답을 먼저 보내고, 그다음 Mongo에 기록
    background_tasks.add_task(save_rejected, req, http_code, body, block_reason)
    headers = {"Retry-After": str(retry_after)} if retry_after else None
    return JSONResponse(status_code=http_code, content=body, headers=headers)


async def reject_with_hold(req, hold: Hold):
    """보내는 지갑 보류: 거래 거절(429) + 보류 기록 + 보류 이벤트."""
    body = {"status": "REJECTED", "reason": f"TEMP_HOLD_{hold.rule}", "retry_after": HOLD_SECONDS}
    log_result(req, 429, body, hold=hold.rules)
    # 보류 기록은 L1 계산에 쓰이므로 응답 전에 저장한다
    await save_sender_hold(req, 429, body, hold)
    return JSONResponse(status_code=429, content=body, headers={"Retry-After": str(HOLD_SECONDS)})


# ---------- 송금 API ----------

@app.post("/api/v1/transfers")
async def create_transfer(
    req: TransferRequest,
    background_tasks: BackgroundTasks,
    x_api_key: str | None = Header(default=None),
):
    # 0) 요청 검사: API 키(401) / 금액(422) / 시각(422)
    if not is_valid_key(x_api_key):
        return JSONResponse(status_code=401, content={"status": "REJECTED", "reason": "UNAUTHORIZED"})

    # 잘못된 금액이 집계로 넘어가지 않게 막는다
    amount = parse_amount(req.amount)
    if amount is None:
        return reject(req, 422, "INVALID_AMOUNT", background_tasks)

    # 현재 시각과 어긋난 거래 거절
    if abs(datetime.now(timezone.utc) - req.timestamp) > MAX_CLOCK_SKEW:
        return reject(req, 422, "TIMESTAMP_OUT_OF_RANGE", background_tasks)

    # 1) 지갑 상태: 동결(403) / R07 블랙리스트(403) / 보류 중(429)  → wallet_status.py
    try:
        block = await check_wallets(redis_client, req.from_address, req.to_address, hold_key(req.from_address))
    except RedisError:
        return reject(req, 503, "FDS_UNAVAILABLE", background_tasks)
    if block:
        # 블랙리스트 지갑에 보내려 한 경우만 Spark에 알린다 (보낸 지갑 분석)
        block_reason = block.reason if block.reason == BLACKLISTED_RECEIVER else None
        return reject(req, block.http_code, block.reason, background_tasks,
                      retry_after=block.retry_after, block_reason=block_reason)

    # 2) 중복 요청(409): 같은 tx_hash + log_index. R룰 개수가 두 번 오르지 않도록 계산 전에 확인
    idem_key = f"{req.tx_hash}:{req.log_index}"
    try:
        first_time = await claim_idempotency(redis_client, req.from_address, idem_key)
    except RedisError:
        return reject(req, 503, "FDS_UNAVAILABLE", background_tasks)
    if not first_time:
        return reject(req, 409, "DUPLICATE_REQUEST", background_tasks)

    async def release():
        # 거절된 거래는 보류가 끝난 뒤 같은 tx로 다시 보낼 수 있어야 한다
        try:
            await release_idempotency(redis_client, req.from_address, idem_key)
        except RedisError:
            pass

    # 3) R01~R06 판단  → holds.py (R05를 먼저 보고, 아니면 Lua로 R01~R04·R06)
    try:
        decision = await evaluate(redis_client, req, amount, int(time.time() * 1000))
    except RedisError:
        await release()
        return reject(req, 503, "FDS_UNAVAILABLE", background_tasks)

    # 4) 판단 결과 처리
    # 보내는 지갑이 걸림 (R02·R04·R05 또는 6건째 이후 R01): 거래 거절(429) + 보류 기록
    if decision.kind == "HOLD_SENDER":
        await release()
        return await reject_with_hold(req, Hold(req.from_address, decision.rules, "SENDER", decision.evidence))
    # 동시에 들어온 다른 요청이 먼저 보류를 건 경우 (429)
    if decision.kind == "HELD":
        await release()
        return reject(req, 429, f"TEMP_HOLD_{decision.rule}", background_tasks, retry_after=HOLD_SECONDS)

    sender_hold = None
    receiver_hold = None
    if decision.kind in ("HOLD_SENDER_PASS", "HOLD_BOTH_PASS"):
        sender_hold = Hold(req.from_address, ["R01"], "SENDER", decision.evidence)
    if decision.kind in ("HOLD_RECEIVER", "HOLD_BOTH_PASS"):
        receiver_rules = [r for r in decision.rules if r != "R01"]
        receiver_hold = Hold(req.to_address, receiver_rules, "RECEIVER", decision.evidence)

    # 5) 통과: 원천 데이터와 outbox 저장. 커밋이 끝난 뒤에 200으로 응답
    now = datetime.now(timezone.utc)
    body = {"status": "ACCEPTED", "reason": None}
    if sender_hold:
        until = now + timedelta(seconds=HOLD_SECONDS)
        body["hold_applied"] = True
        body["hold_rule"] = "R01"
        body["hold_until"] = until.isoformat()

    try:
        await execute_transfer(req, body, receiver_hold=receiver_hold, sender_hold=sender_hold)
    except Exception as e:
        if isinstance(e, DuplicateKeyError):
            # Redis 키가 사라져도 Mongo 고유 인덱스가 중복 저장을 막는다
            http_code, reason = 409, "DUPLICATE_REQUEST"
        else:
            logger.error(f"송금 처리 실패 {type(e).__name__} tx_hash={req.tx_hash} log_index={req.log_index}")
            http_code, reason = 503, "FDS_UNAVAILABLE"
            await release()
            # 저장되지 않은 거래는 5분 기록과 받는 지갑 보류에서 되돌린다
            try:
                await undo_record(redis_client, req, receiver_hold is not None)
            except RedisError:
                pass
        return reject(req, http_code, reason, background_tasks)

    log_extra = {}
    if sender_hold:
        log_extra["sender_hold"] = sender_hold.rules
    if receiver_hold:
        log_extra["receiver_hold"] = receiver_hold.rules

    log_result(req, 200, body, **log_extra)
    return body
