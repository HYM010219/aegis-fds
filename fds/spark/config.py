"""Spark 2단계 설정: 기간, L1~L8 기준값, 처리량 제한, Spark만 쓰는 Redis 키.

기준을 바꿀 때는 이 파일만 보면 된다. (토픽 이름·동결/블랙리스트 키·R05 기준은 common/keys.py)
Spark 이미지가 Python 3.8이라 3.8 문법만 쓴다.
"""
import os
from datetime import timedelta

from common.keys import WHALE_LIMIT_USDT, whitelist_key

TICK_TOPIC = "tick"  # 거래가 없어도 대기 목록을 처리하도록 stream.py가 넣는 시계 신호

# ---------- 기간 (PERIOD_PROFILE=prod|demo) ----------
PROFILES = {
    #        장기(L1·L3·L4·L5·L6·L7)  하루(L2·L6·L7·L8)      경유(L4)               재분석 간격(계기 ②)
    "prod": {"long": timedelta(days=14), "day": timedelta(hours=24), "hour": timedelta(hours=1), "cooldown_ms": 60_000},
    "demo": {"long": timedelta(minutes=20), "day": timedelta(minutes=5), "hour": timedelta(minutes=1), "cooldown_ms": 15_000},
}
PROFILE_NAME = os.getenv("PERIOD_PROFILE", "prod")
if PROFILE_NAME not in PROFILES:
    raise ValueError(f"PERIOD_PROFILE은 prod 또는 demo: {PROFILE_NAME}")
PERIOD = PROFILES[PROFILE_NAME]

# ---------- 룰별 기준 (초안, 시험 결과로 조정) ----------
# L1 반복 보류
L1_MIN_HOLDS = 3              # 2주 안에 1단계 보류 3회 이상
# L2 쪼개서 대량 송금 (R05 회피)
L2_MIN_DAY_SUM = WHALE_LIMIT_USDT  # R05와 동일 기준: 기본 누적 10만 이상, 각 거래 10만 미만
# L3 장기 쪼개기 (R02 회피)
L3_MIN_SPLITS = 10            # 2주 안에 900~990 송금 10회 이상
SPLIT_MIN, SPLIT_MAX = 900, 990
# L4 경유 지갑
L4_FORWARD_RATIO = 0.8        # 받은 금액의 80% 이상을 1시간 안에 다시 송금
L4_MIN_TIMES = 3              # 2주 안에 3회 이상
# L5 위험 지갑 접촉
L5_MEDIUM_SUM = 10_000        # 동결 지갑과 2주 누적 1만 이상이면 MEDIUM
# L6 급격한 활동 증가
L6_RATIO = 10                 # 오늘 거래 수가 평소 하루 평균의 10배 이상
L6_MIN_COUNT = 20             # 최소 20건
# L7 새 상대 집중
L7_MIN_NEW = 20               # 하루 안에 처음 거래하는 상대 20개 이상
# L8 순환 거래
L8_RETURN_RATIO = 0.8         # 처음 보낸 금액의 80% 이상이 돌아옴
L8_MAX_FANOUT = 200           # 상대가 이보다 많은 지갑은 더 넓혀 가지 않는다 (거래소, 허브)
# L4·L8 공통
BIG_AMOUNT = 1_000            # L4는 1,000 이상 받은 거래, L8은 1,000 이상 거래만 따라간다 (부하 제한)

RULE_NAMES = {
    "L1": "REPEATED_HOLD",
    "L2": "SPLIT_DAILY_SUM",
    "L3": "LONG_SMURFING",
    "L4": "PASS_THROUGH",
    "L5": "RISK_EXPOSURE",
    "L6": "ACTIVITY_SPIKE",
    "L7": "NEW_COUNTERPARTY_BURST",
    "L8": "CIRCULAR_FLOW",
}

# ---------- 처리량 제한 ----------
MAX_WALLETS_PER_BATCH = 2_000  # 한 배치에서 전체 분석할 최대 지갑 수 (넘으면 다음 배치로)
RECHECK_PER_BATCH = 500        # 계기 ③: 한 배치에서 L5를 다시 계산할 상대 지갑 수
MAX_HASHES = 50                # 경보에 담는 거래 해시 수

# ---------- Spark 근거 재계산 (crosscheck.py) ----------
# 판단에는 쓰지 않는다. L2·L6·L7 경보가 뜨면 원본 거래를 Spark로 다시 계산해 근거 묶음(recompute)을 붙인다.
CROSSCHECK_ENABLED = os.getenv("CROSSCHECK_ENABLED", "1") == "1"   # 0이면 이전과 똑같이 동작
CROSSCHECK_RULES = {"L2", "L6", "L7"}
CROSSCHECK_MONEY_TOLERANCE = 0.01  # 금액 비교 허용 오차(USDT). 판정 값은 float, 재계산은 Decimal이라 미세한 차이는 같다고 본다
CROSSCHECK_MAX_TXS = 500           # recompute에 담는 구성 거래 수 상한 (사건 문서 크기 제한, 넘으면 txs_truncated)

# ---------- Redis 키 (Spark만 씀. 동결·블랙리스트 키는 common/keys.py) ----------
WHITELIST_L_KEY = whitelist_key("L")         # 2단계 분석 면제 (거래소 등)
ANALYZED_PREFIX = "fds:analyzed:"            # 최근에 분석한 지갑 (재분석 간격 TTL)
PENDING_KEY = "fds:analyze_pending"          # 간격 때문에 미룬 지갑 (점수 = 분석할 시각 ms)
RECHECK_KEY = "fds:recheck_l5"               # 계기 ③: L5를 다시 볼 상대 지갑 (점수 = 넣은 시각 ms)
BLACKLIST_SEEN_KEY = "fds:blacklist_seen"    # 이미 계기 ③을 처리한 블랙리스트
CONFIRMED_SEEN_KEY = "fds:frozen_confirmed_seen"  # 이미 계기 ③을 처리한 '확인된 동결'

# L5 동결 지갑 노출의 근거
#   all (기본)     : 블랙리스트 + 모든 동결 (자동 동결 포함). L5는 검토 대기라 판단은 직원이 한다
#   confirmed      : 블랙리스트 + 직원이 확인한 동결만 (자동 동결 오탐의 번짐을 줄이는 비교 측정용)
# L1 보류 카운트 대상 (E안 스위치)
#   sender (기본): 보내는 쪽(side == SENDER) 보류만 합산. 받기만 해서 걸린 보류(R03·R06)로는 동결하지 않는다
#   all          : 보내는 쪽 + 받는 쪽 모든 보류 합산 (이전 동작, 비교 측정용)
L1_HOLD_SIDE = os.getenv("L1_HOLD_SIDE", "sender").lower()
if L1_HOLD_SIDE not in ("all", "sender"):
    raise ValueError(f"L1_HOLD_SIDE는 'all' 또는 'sender'여야 합니다: {L1_HOLD_SIDE}")

L5_EXPOSURE = os.getenv("L5_EXPOSURE", "all")
if L5_EXPOSURE not in ("confirmed", "all"):
    raise ValueError(f"L5_EXPOSURE는 confirmed 또는 all: {L5_EXPOSURE}")
