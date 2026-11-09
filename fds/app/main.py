import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import BackgroundTasks, FastAPI
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, BaseModel, Field
from pymongo.errors import DuplicateKeyError
from redis.exceptions import RedisError

from app.blacklist import check_blacklist
from app.redis_client import redis_client
from app.idempotency import claim_idempotency, release_idempotency
from app.mongo_client import mongo_client
from app.whitelist import is_whitelisted
from app.store import (
    WHALE_LIMIT,
    ensure_indexes,
    execute_transfer,
    parse_amount,
    save_rejected,
)
import secrets
from fastapi import Header

#  게이트웨이 보안
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


def log_result(req, http_code: int, body: dict):
    record = {
        "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "http_code": http_code,
        **body,
        "request": req.model_dump(mode="json", by_alias=True),
    }
    line = json.dumps(record, ensure_ascii=False)
    if http_code < 400:
        logger.info(line)
    else:
        logger.warning(line)


# ---------- 앱 ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await redis_client.ping()
    await ensure_indexes()
    await mongo_client.admin.command("ping")
    logger.info('{"event": "startup", "redis": "ok", "mongo": "ok"}')
    yield
    await redis_client.aclose()
    await mongo_client.close()

app = FastAPI(title="AEGIS-FDS Gateway", lifespan=lifespan)

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


def reject(req, http_code, reason, background_tasks):
    body = {"status": "REJECTED", "reason": reason}
    log_result(req, http_code, body)
    # 응답을 먼저 보내고, 그다음 Mongo에 기록
    background_tasks.add_task(save_rejected, req, http_code, body)
    return JSONResponse(status_code=http_code, content=body)


@app.post("/api/v1/transfers")
async def create_transfer(
    req: TransferRequest,
    background_tasks: BackgroundTasks,
    x_api_key: str | None = Header(default=None),
):
    if not is_valid_key(x_api_key):
        return JSONResponse(status_code=401, content={"status": "REJECTED", "reason": "UNAUTHORIZED"})

    # 금액 검사 (잘못된 금액이 Spark 집계로 넘어가지 않게 막는다)
    amount = parse_amount(req.amount)
    if parse_amount(req.amount) is None:
        return reject(req, 422, "INVALID_AMOUNT", background_tasks)

    # 현재 시각과 어긋난 거래 거절 (미래 시각이 Spark 탐지를 멈추게 하는 것도 막는다)
    if abs(datetime.now(timezone.utc) - req.timestamp) > MAX_CLOCK_SKEW:
        return reject(req, 422, "TIMESTAMP_OUT_OF_RANGE", background_tasks)

    # 1) 동결 / 블랙리스트
    try:
        reason = await check_blacklist(redis_client, req.from_address, req.to_address)
    except RedisError:
        return reject(req, 503, "FDS_UNAVAILABLE", background_tasks)

    if reason:
        return reject(req, 403, reason.value, background_tasks)
    
    # R05 대량 송금 (잔액을 알 수 없어 금액 기준으로 막는다)
    if amount >= WHALE_LIMIT:
        try:
            exempt = await is_whitelisted(redis_client, "R05", req.from_address)
        except RedisError:
            return reject(req, 503, "FDS_UNAVAILABLE", background_tasks)

        if not exempt:
            return reject(req, 403, "WHALE_TRANSFER", background_tasks)

    # 2) R01 중복 요청 (같은 tx_hash + log_index)
    idem_key = f"{req.tx_hash}:{req.log_index}"
    try:
        first_time = await claim_idempotency(redis_client, req.from_address, idem_key)
    except RedisError:
        return reject(req, 503, "FDS_UNAVAILABLE", background_tasks)

    if not first_time:
        return reject(req, 409, "DUPLICATE_REQUEST", background_tasks)

    # 3) 원천 데이터와 outbox 저장. 커밋이 끝난 뒤에 200으로 응답
    body = {"status": "ACCEPTED", "reason": None}
    try:
        await execute_transfer(req, body)
    except DuplicateKeyError:
        # Redis 키가 사라져도 Mongo 고유 인덱스가 중복 저장을 막는다
        return reject(req, 409, "DUPLICATE_REQUEST", background_tasks)
    except Exception as e:
        logger.error(f"송금 처리 실패 {type(e).__name__} tx_hash={req.tx_hash} log_index={req.log_index}")
        try:
            await release_idempotency(redis_client, req.from_address, idem_key)
        except RedisError:
            pass
        return reject(req, 503, "FDS_UNAVAILABLE", background_tasks)

    log_result(req, 200, body)
    return body