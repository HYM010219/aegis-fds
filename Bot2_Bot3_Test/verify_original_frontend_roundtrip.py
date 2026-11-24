"""Read-only backend verification after restoring the original frontend.

Run inside gateway with --run-id of a completed BOT3_L8 scenario.
"""
import argparse
import json
import os
import time
import urllib.request
from decimal import Decimal

from pymongo import MongoClient
from redis import Redis


def get(path):
    request = urllib.request.Request("http://127.0.0.1:8000/api/v1/admin" + path,
        headers={"X-Admin-Key": os.environ["ADMIN_API_KEY"]})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    database = MongoClient(os.environ["MONGO_URI"], tz_aware=True)[os.getenv("MONGO_DB", "aegis")]
    run = database.demo_runs.find_one({"_id": args.run_id})
    assert run and run["scenario_id"] == "BOT3_L8" and run["status"] == "COMPLETED"
    assert [r["http"] for r in run["results"]] == [200] * 7
    requests = list(database.stream_requests.find({"run_id": args.run_id}))
    assert len(requests) == 7
    assert database.transfers.count_documents({"run_id": args.run_id, "status": "ACCEPTED"}) == 7
    topic_results = {}
    for topic in ("transfer.requests.v1", "transfer.events.v1"):
        rows = list(database.outbox_events.find({"topic": topic, "payload.run_id": args.run_id}))
        assert len(rows) == 7 and all(r["publish_state"] == "PUBLISHED" for r in rows), topic
        topic_results[topic] = {"published": len(rows)}
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        metrics = list(database.stream_metrics.find({"run_id": args.run_id, "window_minutes": 1}))
        if sum(r["tx_count"] for r in metrics) == 7:
            assert sum(Decimal(r["amount_sum"]) for r in metrics) == Decimal("40770")
            break
        time.sleep(3)
    else:
        raise RuntimeError("Request lakehouse did not produce all seven events")
    case = database.fds_cases.find_one({"rule_id": "L8", "evidence_snapshot.source.run_ids": args.run_id})
    assert case
    case_id = str(case["_id"])
    detail = get("/cases/" + case_id)
    snapshot = detail["evidence_snapshot"]
    draft = get("/cases/" + case_id + "/str-draft?snapshot_id=" + snapshot["snapshot_id"])
    graph = get("/cases/" + case_id + "/graph?depth=2")
    assert draft["snapshot"] == snapshot
    assert snapshot["evidence"]["amounts"] == [10000, 9500, 9000]
    assert snapshot["evidence"]["return_ratio"] == .9 and "90.0%" in draft["text"]
    assert sum(e["hit"] for e in graph["edges"]) == 3
    assert Redis(host=os.getenv("REDIS_HOST", "redis"), decode_responses=True).sismember("fds:frozen_wallets", run["wallets"]["A"])
    visible = get("/cases?limit=1000")
    assert any(c["case_id"] == case_id for c in visible)
    assert all(c["rule_id"].startswith("L") for c in visible)
    hidden_ids = {str(c["_id"]) for c in database.fds_cases.find({"case_kind": "STREAM_OBSERVATION"}, {"_id": 1})}
    assert not any(r.get("case_id") in hidden_ids for r in get("/audit?limit=5000"))
    print(json.dumps({"status": "PASS", "run_id": args.run_id, "case_id": case_id,
        "gateway_http": [200] * 7, "predecision_requests": 7, "accepted_transfers": 7,
        "kafka_outbox": topic_results,
        "request_delta_branch": {"one_minute_total_requests": 7, "one_minute_total_amount": "40770.000000",
                                 "query_ids": sorted({r["query_id"] for r in metrics})},
        "l8_case_branch": {"engine": snapshot["source"]["engine"],
            "batch_id": snapshot["source"]["batch_id"], "amounts": [10000, 9500, 9000],
            "return_ratio": .9, "redis_internal_frozen": True,
            "snapshot_id": snapshot["snapshot_id"], "detail_str_match": True,
            "graph_highlighted_edges": 3},
        "original_frontend_api_compatible": True, "observations_hidden_from_original_ui": True}))


if __name__ == "__main__":
    main()
