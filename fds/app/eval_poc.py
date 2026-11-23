"""[EVAL] PoC 평가 전용: 봇 정답 라벨을 받아 룰별 Recall·Precision·오탐률을 계산한다.

    ⚠ 실제 운영에는 필요 없다 (운영에서는 '이 거래가 진짜 사기인지' 정답이 없음).
      끄기:  EVAL_ENABLED=0  (docker-compose.yml gateway)  → 아래 주소가 아예 생기지 않는다
      빼기:  이 파일 + 코드에서 '[EVAL]' 표시가 붙은 줄을 지우면 된다 (main.py, docker-compose.yml, .env.example, README)
      운영 코드(게이트웨이 판정·Spark·경보 소비자)는 이 파일을 모른다. 이 파일은 운영 결과를 읽기만 한다.

    POST /api/v1/eval/labels        봇(bot2·bot3)이 보낸 시나리오의 정답 (X-API-Key = GATEWAY_API_KEY, 봇이 이미 가진 키)
    GET  /api/v1/admin/eval/score   정답과 실제 탐지를 맞대어 룰별 성적표 (X-Admin-Key = ADMIN_API_KEY)

정답 한 줄: 이 지갑은 공격(is_fraud=true)이고 노린 룰은 target_rule이다 / 또는 정상(is_fraud=false)이다.
탐지 결과는 운영이 남긴 기록에서 읽는다: L1~L8 fds_cases, R01~R06 fds_holds, R07 transfers(BLACKLISTED_*).

성적표 (지갑 기준, 정답표에 있는 지갑만 센다 — 실데이터 지갑은 정답이 없으므로 제외)
    caught       target_rule이 X인 공격 지갑 중 X로 잡힌 수
    missed       target_rule이 X인 공격 지갑 중 X로 안 잡힌 수
    false_alarm  정상 지갑인데 X로 잡힌 수
    recall       caught / (caught + missed)
    precision    caught / (caught + false_alarm)
    also_hit     다른 룰을 노린 공격 지갑이 X에도 같이 걸린 수 (예: R04 시나리오가 R01에도 걸림) — 정답·오답으로 세지 않고 따로 보여준다
    normal.fpr   정상 지갑 중 어떤 룰에든 걸린 비율
"""
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, FastAPI, Header, Query
from pydantic import AwareDatetime, BaseModel, Field, StringConstraints
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

from app.admin import AdminError, require_admin_key
from app.mongo_client import db

# [EVAL] 켜고 끄기. 기본은 켬(PoC). 운영에서는 0.
EVAL_ENABLED = os.getenv("EVAL_ENABLED", "1") == "1"
GATEWAY_API_KEY = os.getenv("GATEWAY_API_KEY") or ""

RULES = [f"R0{i}" for i in range(1, 8)] + [f"L{i}" for i in range(1, 9)]
Rule = Literal[tuple(RULES)]
Wallet = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]


# ---------- 정답 라벨 받기 (봇 → 게이트웨이) ----------

async def require_gateway_key(x_api_key: str | None = Header(default=None)):
    if x_api_key is None or not secrets.compare_digest(x_api_key.encode(), GATEWAY_API_KEY.encode()):
        raise AdminError(401, "UNAUTHORIZED")


class Label(BaseModel):
    wallet: Wallet                                   # 결과를 확인할 지갑 (R03은 받는 지갑, R06·L8은 출발 지갑 A)
    is_fraud: bool
    target_rule: Rule | None = None                  # 공격이면 노린 룰 (정상이면 비움)
    scenario: str | None = Field(default=None, max_length=200)
    tx_hash: str | None = Field(default=None, max_length=200)
    sent_at: AwareDatetime | None = None             # 봇이 보낸 시각 (없으면 받은 시각)


class LabelBatch(BaseModel):
    labels: list[Label] = Field(min_length=1, max_length=1000)


label_router = APIRouter(prefix="/api/v1/eval", dependencies=[Depends(require_gateway_key)])
score_router = APIRouter(prefix="/api/v1/admin/eval", dependencies=[Depends(require_admin_key)])
_indexed = False


async def _ensure_index():
    global _indexed
    if not _indexed:
        await db.eval_labels.create_index([("wallet", 1), ("target_rule", 1), ("scenario", 1)], unique=True)
        await db.eval_labels.create_index("sent_at")
        _indexed = True


@label_router.post("/labels")
async def add_labels(body: LabelBatch):
    """같은 (지갑, 노린 룰, 시나리오)는 한 줄로 합친다 (봇이 다시 보내도 중복 없음)."""
    now = datetime.now(timezone.utc)
    ops = [
        UpdateOne(
            {"wallet": lb.wallet, "target_rule": lb.target_rule, "scenario": lb.scenario},
            {"$set": {"is_fraud": lb.is_fraud, "tx_hash": lb.tx_hash, "sent_at": lb.sent_at or now,
                      "received_at": now}},
            upsert=True,
        )
        for lb in body.labels
    ]
    try:
        await _ensure_index()
        result = await db.eval_labels.bulk_write(ops, ordered=False)
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")
    return {"status": "OK", "received": len(ops), "new": result.upserted_count}


# ---------- 성적표 (관제 S4) ----------

async def _detections(wallets, since, until):
    """지갑별로 실제 걸린 룰 집합 (운영 기록을 읽기만 함)."""
    hits = {w: set() for w in wallets}
    ws = list(wallets)
    async for c in db.fds_cases.find({"violator_address": {"$in": ws}, "detected_at": {"$gte": since, "$lte": until}},
                                     {"violator_address": 1, "rule_id": 1}):
        hits[c["violator_address"]].add(c["rule_id"])
    async for h in db.fds_holds.find({"wallet": {"$in": ws}, "held_at": {"$gte": since, "$lte": until}},
                                     {"wallet": 1, "rules": 1, "rule": 1}):
        hits[h["wallet"]].update(h.get("rules") or [h.get("rule")])
    async for t in db.transfers.find({"from": {"$in": ws}, "reason": {"$in": ["BLACKLISTED_SENDER", "BLACKLISTED_RECEIVER"]},
                                      "created_at": {"$gte": since, "$lte": until}}, {"from": 1}):
        hits[t["from"]].add("R07")
    return hits


def _ratio(a, b):
    return round(a / b, 4) if b else None


@score_router.get("/score")
async def score(
    since: AwareDatetime | None = Query(default=None),  # 시험 시작 (없으면 24시간 전)
    until: AwareDatetime | None = Query(default=None),  # 시험 끝 (없으면 지금)
):
    until = until or datetime.now(timezone.utc)
    since = since or until - timedelta(hours=24)
    try:
        labels = await db.eval_labels.find({"sent_at": {"$gte": since, "$lte": until}}).to_list()
        hits = await _detections({lb["wallet"] for lb in labels}, since, until)
    except PyMongoError:
        raise AdminError(503, "FDS_UNAVAILABLE")

    fraud = [lb for lb in labels if lb["is_fraud"] and lb.get("target_rule")]
    normal = {lb["wallet"] for lb in labels if not lb["is_fraud"]}
    rows = []
    for rule in RULES:
        targets = [lb for lb in fraud if lb["target_rule"] == rule]
        caught = sum(1 for lb in targets if rule in hits[lb["wallet"]])
        false_alarm = sum(1 for w in normal if rule in hits[w])
        also_hit = sum(1 for lb in fraud if lb["target_rule"] != rule and rule in hits[lb["wallet"]])
        if not targets and not false_alarm and not also_hit:
            continue
        rows.append({
            "rule": rule,
            "labeled": len(targets),
            "caught": caught,
            "missed": len(targets) - caught,
            "false_alarm": false_alarm,
            "recall": _ratio(caught, len(targets)),
            "precision": _ratio(caught, caught + false_alarm),
            "also_hit": also_hit,
            "missed_wallets": [lb["wallet"] for lb in targets if rule not in hits[lb["wallet"]]][:20],
        })
    flagged = [w for w in normal if hits[w]]
    return {
        "status": "OK",
        "since": since.isoformat(),
        "until": until.isoformat(),
        "labels": len(labels),
        "rules": rows,
        "normal": {"wallets": len(normal), "flagged": len(flagged), "fpr": _ratio(len(flagged), len(normal)),
                   "flagged_wallets": sorted(flagged)[:20]},
    }


def install(app: FastAPI):
    """[EVAL] main.py에서 부른다. EVAL_ENABLED=0이면 아무 주소도 만들지 않는다."""
    if EVAL_ENABLED:
        app.include_router(label_router)
        app.include_router(score_router)
