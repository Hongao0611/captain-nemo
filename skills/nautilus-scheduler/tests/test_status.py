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


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn(); print("PASS", name)
