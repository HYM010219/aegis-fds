"""알려진 주소 목록 (common/entities.json) 읽기.

게이트웨이(관제 API)와 Spark가 같은 파일을 쓴다. Spark 이미지가 Python 3.8이라 3.8 문법만 쓴다.
주소는 소문자로 비교한다. 파일이 없거나 깨져도 서비스는 계속 돈다 (이름 표시만 빠짐).
"""
import json
import logging
import pathlib

PATH = pathlib.Path(__file__).with_name("entities.json")
CATEGORIES = ("EXCHANGE", "DEX", "LENDING", "BRIDGE", "OTHER")
logger = logging.getLogger(__name__)


def _load():
    try:
        rows = json.loads(PATH.read_text(encoding="utf-8")).get("entities", [])
    except (OSError, ValueError) as e:
        logger.warning("entities.json 읽기 실패: %s", e)
        return {}
    out = {}
    for r in rows:
        addr = (r.get("address") or "").lower()
        if not addr.startswith("0x") or not r.get("name"):
            continue
        if r.get("category") not in CATEGORIES:
            r = dict(r, category="OTHER")
        out[addr] = r
    return out


ENTITIES = _load()


def get(address):
    """주소 → 엔터티 dict (address, name, category, source, source_url, verified_at) 또는 None."""
    return ENTITIES.get((address or "").lower())


def label(address):
    e = get(address)
    return e["name"] if e else None
