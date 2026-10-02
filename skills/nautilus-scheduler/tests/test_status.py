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


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn(); print("PASS", name)
