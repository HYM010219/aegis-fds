"""Opt-in integration test: creates bounded synthetic demo runs in the local DB.

Run INSIDE gateway with --execute. Does not delete/reset existing data.
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.request
from uuid import uuid4


def call(path, body=None, authenticated=True):
    headers = {"Content-Type": "application/json"}
    if authenticated:
        headers["X-Admin-Key"] = os.environ["ADMIN_API_KEY"]
    req = urllib.request.Request("http://127.0.0.1:8000" + path,
        data=json.dumps(body).encode() if body is not None else None, headers=headers)
    try:
        res = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as error:
        res = error
    with res:
        return res.code, json.load(res)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--scenario", choices=["BOT2_HOTWALLET", "BOT3_R01", "BOT3_R04", "BOT3_L8"])
    args = parser.parse_args()
    if not args.execute:
        parser.error("--execute is required: this test writes synthetic requests")
    assert call("/api/v1/admin/demo/scenarios", authenticated=False)[0] == 401
    for scenario, expected in [("BOT2_HOTWALLET", [200]*6), ("BOT3_R01", [200]*5+[429]),
                               ("BOT3_R04", [200]*4+[429]*2), ("BOT3_L8", [200]*7)]:
        if args.scenario and args.scenario != scenario:
            continue
        run_id = "verify-" + str(uuid4())
        body = {"scenario_id": scenario, "run_id": run_id}
        code, result = call("/api/v1/admin/demo/runs", body)
        assert code == 202, result
        print(json.dumps({"scenario": scenario, "run_id": run_id, "state": "started"}), flush=True)
        deadline = time.monotonic()+150
        while time.monotonic() < deadline:
            code, run = call("/api/v1/admin/demo/runs/"+run_id)
            assert code == 200, run
            if run["status"] in ["FAILED", "INTERRUPTED", "STOPPED"]:
                raise AssertionError(run)
            if run["status"] == "COMPLETED" and (scenario != "BOT3_L8" or run["case_id"]):
                break
            time.sleep(2)
        else:
            raise AssertionError("run or L8 detection timed out: " + run_id)
        assert [r["http"] for r in run["results"]] == expected, run["results"]
        if scenario == "BOT3_R01":
            assert run["results"][4]["body"].get("hold_applied") is True
        if scenario == "BOT3_R04":
            assert "R04" in run["results"][4]["body"].get("reason", "")
        # Reposting the ID must only return the saved run.
        assert call("/api/v1/admin/demo/runs", body)[0] == 202
        summary = {"scenario": scenario, "run_id": run_id, "http_results": expected,
                   "status": run["status"], "case_id": run["case_id"]}
        if scenario == "BOT3_L8":
            base = "/api/v1/admin/cases/"+run["case_id"]
            graph_code, graph = call(base+"/graph?depth=2")
            assert graph_code == 200, graph
            assert len(graph["nodes"]) == 7, graph["nodes"]
            assert len(graph["edges"]) == 7, graph["edges"]
            assert sum(e["hit"] for e in graph["edges"]) == 3
            snapshot = graph["case"]["evidence_snapshot"]
            assert snapshot["evidence"]["return_ratio"] == .9
            assert run_id in snapshot["source"]["run_ids"]
            code, draft = call(base+"/str-draft?snapshot_id="+snapshot["snapshot_id"])
            assert code == 200, draft
            assert draft["snapshot"]["snapshot_id"] == snapshot["snapshot_id"]
            assert "90.0%" in draft["text"]
            assert "SYNTHETIC" in draft["snapshot"]["source"]["source_kinds"]
            summary.update(nodes=7, edges=7, highlighted_edges=3, return_ratio=.9,
                           snapshot_id=snapshot["snapshot_id"], spark_batch=snapshot["source"].get("batch_id"))
        print(json.dumps(summary, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
