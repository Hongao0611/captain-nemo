"""nrp_status helpers (no cluster): python3 tests/test_status.py"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import nrp_status as s  # noqa: E402


def test_failed_at():
    term = {"status": {"phase": "Failed", "containerStatuses": [{"state": {"terminated": {
        "finishedAt": "2026-10-01T04:10:00Z", "reason": "Error"}}}]}}
    lost = {"status": {"phase": "Failed", "containerStatuses": [{"state": {"terminated": {
        "finishedAt": None, "reason": "ContainerStatusUnknown"}}}],
        "conditions": [{"type": "Ready", "status": "False", "reason": "PodFailed",
                        "lastTransitionTime": "2026-09-30T23:23:21Z"}]}}
    assert s.failed_at(term) == 1790827800.0, s.failed_at(term)
    assert s.failed_at(lost) == 1790810601.0, s.failed_at(lost)
    assert s.failed_at({"status": {"phase": "Failed"}}) is None


def test_init_container_failure():
    # Train-then-evaluate pod whose training init container died: the failure lives
    # in initContainerStatuses, and the app container never started.
    pod = {"status": {"phase": "Failed",
                      "initContainerStatuses": [{"name": "training", "state": {"terminated": {
                          "finishedAt": "2026-10-02T05:00:00Z", "reason": "OOMKilled", "exitCode": 137}}}],
                      "containerStatuses": [{"name": "benchmarking", "state": {"waiting": {
                          "reason": "PodInitializing"}}}]}}
    assert s.failed_at(pod) == 1790917200.0, s.failed_at(pod)
    assert s.classify(pod, "")[0] == "oom", s.classify(pod, "")


def test_hub_quota_not_fooled_by_counters():
    # tqdm progress counters contain "429" all the time (eval logs, 2026-10-02).
    pod = {"status": {"phase": "Failed", "containerStatuses": [{"state": {"terminated": {"reason": "Error", "exitCode": 1}}}]}}
    assert s.classify(pod, "Running loglikelihood requests: 10%| 16429/162476 [01:08<29:55]")[0] != "hub-quota"
    assert s.classify(pod, "huggingface_hub.errors.HfHubHTTPError: 429 Client Error: Too Many Requests")[0] == "hub-quota"


def test_exposed_init_running_is_started():
    # A train-then-evaluate pod training in its init container stays in phase Pending:
    # on an excluded node it must not be advised for recycling (2026-10-07: 6.4 h in).
    import contextlib
    import io

    def pod(name, init_state):
        return {"metadata": {"name": name}, "spec": {"nodeName": "bad-1"},
                "status": {"phase": "Pending",
                           "initContainerStatuses": [{"name": "training", "state": init_state}]}}
    pods = [pod("me-training", {"running": {"startedAt": "2026-10-06T20:12:41Z"}}),
            pod("me-unstarted", {"waiting": {"reason": "PodInitializing"}})]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        s.exposed_jobs(pods, ["bad-1"])
    lines = {ln.split(": ", 1)[1].split()[0]: ln for ln in out.getvalue().splitlines() if "ON EXCLUDED" in ln}
    assert "Running ON EXCLUDED" in lines["me-training"] and "recycle the Job" not in lines["me-training"], lines
    assert "Pending ON EXCLUDED" in lines["me-unstarted"] and "not started yet" in lines["me-unstarted"], lines


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn(); print("PASS", name)
