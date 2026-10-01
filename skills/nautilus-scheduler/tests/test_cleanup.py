"""nrp_cleanup selection logic (no cluster): python3 tests/test_cleanup.py"""
import datetime as dt
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import nrp_cleanup as c  # noqa: E402


def job(name, cond):
    return {"metadata": {"name": name}, "spec": {"backoffLimit": 6},
            "status": {"conditions": [{"type": cond, "status": "True"}]} if cond else {"active": 1}}


def test_finished_jobs_need_tracker_success():
    jobs = [job("me-a", "Complete"), job("me-b", "Complete"), job("me-c", "SuccessCriteriaMet"),
            job("me-d", None), job("other-e", "Complete")]
    tracker = {"me-a": "SUCCEEDED", "me-b": "RUNNING", "me-c": "SUCCEEDED", "me-d": "SUCCEEDED",
               "other-e": "SUCCEEDED"}
    assert c.finished_jobs(jobs, tracker, "me-") == ["me-a", "me-c"]


def test_stuck_pods_force_only_off_ready_nodes():
    now = dt.datetime(2026, 10, 1, 12, tzinfo=dt.timezone.utc)
    old, fresh = "2026-10-01T10:00:00Z", "2026-10-01T11:50:00Z"
    pod = lambda name, node, phase="Running", dl=None: {
        "metadata": {"name": name, **({"deletionTimestamp": dl} if dl else {})},
        "spec": {"nodeName": node}, "status": {"phase": phase}}
    nodes = [{"metadata": {"name": "ok"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}},
             {"metadata": {"name": "sick"}, "status": {"conditions": [{"type": "Ready", "status": "False"}]}}]
    pods = [pod("me-1", "gone", dl=old), pod("me-2", "sick", phase="Unknown"), pod("me-3", "ok", dl=old),
            pod("me-4", "gone", dl=fresh), pod("me-5", "gone"), pod("x-6", "gone", dl=old)]
    force, report = c.stuck_pods(pods, nodes, "me-", 60, now=now)
    assert force == ["me-1", "me-2"] and report == ["me-3"], (force, report)


def test_load_trackers_both_formats():
    with tempfile.TemporaryDirectory() as d:
        json.dump({"me-a": {"status": "SUCCEEDED", "attempts": 1}}, open(os.path.join(d, "w1.tracker.json"), "w"))
        json.dump({"me-b": "SUCCEEDED"}, open(os.path.join(d, "w2.tracker.json"), "w"))
        assert c.load_trackers([d]) == {"me-a": "SUCCEEDED", "me-b": "SUCCEEDED"}


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn(); print("PASS", name)
