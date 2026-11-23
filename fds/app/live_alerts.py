"""관제 화면 실시간 경보 (WebSocket): 새 케이스·케이스 변경을 바로 보낸다.

    WS /ws/admin   (헤더 X-Admin-Key 필요 — 브라우저는 헤더를 못 붙이므로 Vite 프록시·웹 백엔드가 붙여 연결)
    보내는 메시지: {"type": "hello"} / {"type": "case_created" | "case_updated", "case": {...요약}}

케이스 변경은 Mongo change stream(fds_cases)으로 감시한다 (main.py lifespan에서 watch_cases 시작, 복제 세트 필요).
지금 프론트는 5초 폴링을 쓰므로(web/src/admin/data/hooks.js useLiveSync), 연결은 프론트 쪽에서 바꿀 때 쓰면 된다.
"""
import asyncio
import logging
import secrets
from datetime import datetime

from bson import ObjectId
from bson.decimal128 import Decimal128
from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect
from pymongo.errors import PyMongoError

from app.admin import ADMIN_API_KEY
from app.mongo_client import db

logger = logging.getLogger("aegis.gateway")
router = APIRouter()  # WebSocket은 키를 함수 안에서 직접 확인한다


def install(app: FastAPI):
    """main.py에서 부른다."""
    app.include_router(router)


def _plain(value):
    """Mongo 값을 JSON으로 보낼 수 있게 바꾼다 (ObjectId → 문자열, 시각 → ISO, Decimal128 → 소수)."""
    if isinstance(value, dict):
        return {("id" if k == "_id" else k): _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return value


# ---------- 실시간 경보 (WebSocket) ----------
# 관제 화면 → (웹 백엔드나 Vite 프록시가 X-Admin-Key를 붙여) → WS /api/v1/admin/ws/alerts
# 새 케이스(case_created)와 케이스 변경(case_updated: 경보 합침, 직원 처리)을 바로 보낸다.

_clients = set()


def _summary(doc):
    keep = ("_id", "rule_id", "rule_name", "risk_level", "violator_address", "review_status", "action_taken",
            "detected_at", "updated_at")
    return _plain({k: doc.get(k) for k in keep})


async def _broadcast(message):
    for ws in list(_clients):
        try:
            await ws.send_json(message)
        except Exception:  # 끊긴 화면은 빼고 계속 보낸다
            _clients.discard(ws)


async def watch_cases():
    """fds_cases 변경을 지켜보다가 접속 중인 화면에 보낸다 (main.py lifespan에서 시작). Mongo 복제 세트가 필요하다."""
    pipeline = [{"$match": {"operationType": {"$in": ["insert", "update"]}}}]
    while True:
        try:
            async with await db.fds_cases.watch(pipeline, full_document="updateLookup") as stream:
                async for change in stream:
                    doc = change.get("fullDocument")
                    if doc:
                        kind = "case_created" if change["operationType"] == "insert" else "case_updated"
                        await _broadcast({"type": kind, "case": _summary(doc)})
        except asyncio.CancelledError:
            raise
        except PyMongoError as e:
            logger.warning(f"케이스 변경 감시 재시작 {type(e).__name__}")
            await asyncio.sleep(3)


@router.websocket("/ws/admin")
async def ws_alerts(ws: WebSocket):
    key = ws.headers.get("x-admin-key")
    if not ADMIN_API_KEY or key is None or not secrets.compare_digest(key.encode(), ADMIN_API_KEY.encode()):
        await ws.close(code=1008)  # 정책 위반 (키 없음·틀림)
        return
    await ws.accept()
    _clients.add(ws)
    await ws.send_json({"type": "hello", "clients": len(_clients)})
    try:
        while True:
            await ws.receive_text()  # 화면이 보내는 메시지는 무시 (연결 유지용)
    except WebSocketDisconnect:
        pass
    finally:
        _clients.discard(ws)
