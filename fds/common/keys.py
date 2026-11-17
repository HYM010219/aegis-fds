"""여러 서비스가 같이 쓰는 이름과 값을 한 곳에 모은 파일.

게이트웨이·경보 소비자(fds/app), Spark(fds/spark)가 모두 여기서 가져다 쓴다.
값을 바꾸면 모든 곳에 같이 반영된다.

한 서비스 안에서만 쓰는 키(보류·5분 기록·중복 요청 키, Spark 대기 목록 키)는 각 파일에 둔다.
Spark 이미지가 Python 3.8이라 3.8 문법만 쓴다.
"""

# ---------- Kafka 토픽 ----------
TRANSFER_TOPIC = "transfer.events.v1"   # 통과한 거래: 게이트웨이 → Spark 계기 ②, ClickHouse
REJECTED_TOPIC = "transfer.rejected.v1"  # 거절된 요청: 게이트웨이 → ClickHouse (Spark는 읽지 않음)
HOLD_TOPIC = "fds.holds.v1"              # 1단계 보류, R07 차단: 게이트웨이 → Spark 계기 ①, ClickHouse
ALERT_TOPIC = "fds.alerts.v1"            # L1~L8 경보: Spark → 경보 소비자, ClickHouse
FREEZE_TOPIC = "fds.freezes.v1"          # 동결(FREEZE)·해제(UNFREEZE): 경보 소비자·직원 API → Spark 계기 ③, ClickHouse

# ---------- Redis 키 (여러 서비스가 같이 읽고 씀) ----------
FROZEN_KEY = "fds:frozen_wallets"          # 동결된 지갑 (Set)
FROZEN_REASONS_KEY = "fds:frozen_reasons"  # 지갑별 동결 사유 (Hash: 지갑 → 룰 ID)
BLACKLIST_KEY = "fds:blacklist_wallets"    # 미리 알려진 위험 주소 (Set)
# 직원이 동결을 해제한 시각 (Hash: 지갑 → ms). Spark는 이 시각 이후 거래만 다시 분석한다 (해제 직후 재동결 방지).
UNFROZEN_AT_KEY = "fds:unfrozen_at"

# 화이트리스트를 둘 수 있는 룰.
# R01~R06: 게이트웨이 1단계 보류 면제 / L: Spark 2단계(L1~L8) 분석 면제
WHITELIST_RULES = ("R01", "R02", "R03", "R04", "R05", "R06", "L")


def whitelist_key(rule_id):
    """룰마다 Redis Set을 하나씩 쓴다. 예: fds:whitelist:R05, fds:whitelist:L"""
    if rule_id not in WHITELIST_RULES:
        raise ValueError(f"화이트리스트가 없는 룰: {rule_id}")
    return f"fds:whitelist:{rule_id}"


# ---------- 룰 기준 중 여러 곳이 같이 쓰는 값 ----------
# R05 대량 송금 기준 (USDT). L2(쪼개서 대량 송금)는 R05를 피하는 패턴이라 같은 값을 기준으로 쓴다.
WHALE_LIMIT_USDT = 50_000
