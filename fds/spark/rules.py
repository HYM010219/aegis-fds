from pyspark.sql.functions import (
    array,
    col,
    collect_set,
    count,
    expr,
    greatest,
    lit,
    max as max_,
    min as min_,
    size,
    struct,
    sum as sum_,
    unix_timestamp,
    window,
)

# 5분 범위를 15초 간격으로 옮겨 가며 계산한다.
WINDOW = "5 minutes"
SLIDE = "15 seconds"


def group_by_window(events, wallet, *aggregations):
    """모든 집계 룰에 필요한 거래 해시와 첫/마지막 거래 시각을 함께 구한다."""
    return events.groupBy(
        window(col("timestamp"), WINDOW, SLIDE), col(wallet).alias("violator_address")
    ).agg(
        collect_set("tx_hash").alias("tx_hashes"),
        min_("timestamp").alias("first_tx_at"),
        max_("timestamp").alias("last_tx_at"),
        *aggregations,
    )


def to_result(df, rule_id, rule_name, **evidence):
    """룰마다 같은 모양으로 경보를 만든다."""
    return df.select(
        lit(rule_id).alias("rule_id"),
        lit(rule_name).alias("rule_name"),
        col("violator_address"),
        col("tx_hashes"),
        col("first_tx_at"),
        col("last_tx_at"),
        struct(*[c.alias(name) for name, c in evidence.items()]).alias("evidence"),
    )


def detect_r01(events):
    """R01 초고빈도 송금: 같은 지갑이 5분 안에 5건 이상 송금."""
    grouped = group_by_window(events, "from", count("*").alias("tx_count"))
    # 첫 건부터 마지막 건까지 걸린 시간 기준의 초당 건수 (같은 초에 몰리면 1초로 계산)
    seconds = greatest(unix_timestamp("last_tx_at") - unix_timestamp("first_tx_at"), lit(1))
    return to_result(
        grouped.where(col("tx_count") >= 5),
        "R01",
        "RAPID_FIRE",
        tx_count_5m=col("tx_count"),
        frequency_burst=col("tx_count") / seconds,
    )


def detect_r02(events):
    """R02 스머핑: 같은 지갑이 5분 안에 900~990 USDT를 3회 이상 송금."""
    in_range = events.where(col("amount").between(900, 990))
    grouped = group_by_window(
        in_range, "from", count("*").alias("split_count"), sum_("amount").alias("sum_amount")
    )
    return to_result(
        grouped.where(col("split_count") >= 3),
        "R02",
        "SMURFING",
        split_count=col("split_count"),
        sum_amount_5m=col("sum_amount"),
    )


def detect_r03(events):
    """R03 자금 집금: 한 지갑이 5분 안에 서로 다른 5개 이상의 지갑에서 입금받음."""
    # 스트리밍에서는 countDistinct를 쓸 수 없어 집합의 크기로 센다.
    grouped = group_by_window(events, "to", collect_set("from").alias("senders"))
    return to_result(
        grouped.where(size("senders") >= 5),
        "R03",
        "FAN_IN",
        fan_in_count=size("senders"),
        collector_wallet=col("violator_address"),
    )


def detect_r04(events):
    """R04 자금 분산: 한 지갑이 5분 안에 서로 다른 5개 이상의 지갑으로 송금."""
    grouped = group_by_window(events, "from", collect_set("to").alias("receivers"))
    return to_result(
        grouped.where(size("receivers") >= 5),
        "R04",
        "FAN_OUT",
        fan_out_count=size("receivers"),
        mule_wallets=col("receivers"),
    )


def detect_r06(events):
    """R06 핑퐁: A→B 뒤 5분 안에 B→A."""
    pairs = events.alias("a").join(
        events.alias("b"),
        expr("""
            a.`from` = b.`to`
            AND a.`to` = b.`from`
            AND a.`from` <> a.`to`
            AND b.timestamp >= a.timestamp
            AND b.timestamp <= a.timestamp + INTERVAL 5 MINUTES
        """),
    )
    # 같은 시각의 왕복이 두 번 잡히지 않도록 한 방향만 남긴다.
    pairs = pairs.where("""
        a.timestamp < b.timestamp
        OR a.tx_hash < b.tx_hash
        OR (a.tx_hash = b.tx_hash AND a.log_index < b.log_index)
    """)
    return pairs.select(
        lit("R06").alias("rule_id"),
        lit("WASH_TRADING").alias("rule_name"),
        col("a.from").alias("violator_address"),
        col("a.to").alias("counterparty_address"),
        array(col("a.tx_hash"), col("b.tx_hash")).alias("tx_hashes"),
        col("a.timestamp").alias("first_tx_at"),
        col("b.timestamp").alias("last_tx_at"),
        struct(
            col("b.tx_hash").alias("pair_tx_hash"),
            (unix_timestamp(col("b.timestamp")) - unix_timestamp(col("a.timestamp"))).alias("round_trip_gap_sec"),
        ).alias("evidence"),
    )


# 룰 이름: (함수, 출력 방식)
# update: 값이 바뀔 때마다 바로 출력 / append: 결과가 확정된 건만 출력
RULES = {
    "r01": (detect_r01, "update"),
    "r02": (detect_r02, "update"),
    "r03": (detect_r03, "update"),
    "r04": (detect_r04, "update"),
    "r06": (detect_r06, "append"),
}
