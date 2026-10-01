#!/usr/bin/env python3
"""Tidy your part of a shared namespace while waves run. Dry run by default.

    python3 nrp_cleanup.py --prefix me-            # report what it would remove
    python3 nrp_cleanup.py --prefix me- --apply    # remove it

Touches only Jobs / pods whose name starts with --prefix:
  - finished Jobs whose tracker entry (<state-dir>/*.tracker.json) is SUCCEEDED.
    A Job that vanishes before the scheduler has polled it is relaunched as
    "vanished", so the tracker -- not the cluster -- gates the deletion. Jobs no
    tracker knows are left alone.
  - pods stuck Terminating longer than --stuck-min, or in phase Unknown, whose node
    is gone or NotReady: force-deleted. They count against the namespace pod quota
    and can keep a finished Job from ever reaching Complete. Pods on a Ready node
    are only reported (the kubelet may still finish them).
Completed pods do not count against the pod quota: deleting finished Jobs is
housekeeping, not a quota fix. Your project's own artifacts (PVC dirs, experiment
trackers, model repos) are out of scope.
"""
import argparse
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import nrp_common as n  # noqa: E402


def load_trackers(state_dirs):
    """name -> status from every tracker (new {name: {status: ..}} or old flat {name: STATUS})."""
    out = {}
    for d in state_dirs:
        for path in glob.glob(os.path.join(d, "*.tracker.json")):
            for k, v in json.load(open(path)).items():
                out[k] = v.get("status") if isinstance(v, dict) else v
    return out


def finished_jobs(jobs, tracker, prefix):
    return sorted(j["metadata"]["name"] for j in jobs
                  if j["metadata"]["name"].startswith(prefix)
                  and n.job_status(j) == "SUCCEEDED" and tracker.get(j["metadata"]["name"]) == "SUCCEEDED")


def stuck_pods(pods, nodes, prefix, stuck_min, now=None):
    """(force-deletable, report-only) pod names."""
    now = now or n.utcnow()
    ready = {nd["metadata"]["name"]: any(c.get("type") == "Ready" and c.get("status") == "True"
                                        for c in nd.get("status", {}).get("conditions", []))
             for nd in nodes}
    force, report = [], []
    for p in pods:
        name = p["metadata"]["name"]
        if not name.startswith(prefix):
            continue
        dl = p["metadata"].get("deletionTimestamp")
        old = dl and (now - n.dt.datetime.fromisoformat(dl.replace("Z", "+00:00"))).total_seconds() > stuck_min * 60
        if not (old or p.get("status", {}).get("phase") == "Unknown"):
            continue
        (report if ready.get(p["spec"].get("nodeName")) else force).append(name)
    return sorted(force), sorted(report)


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--state-dir", action="append", default=None, help="repeatable; default .nrp")
    ap.add_argument("--namespace", default=None)
    ap.add_argument("--context", default=None)
    ap.add_argument("--stuck-min", type=int, default=60)
    ap.add_argument("--apply", action="store_true")
    return ap.parse_args()


def main():
    a = parse_args()
    if not a.prefix or a.prefix in ("-", "*"):
        print("refusing an empty / wildcard prefix", file=sys.stderr)
        return 2
    kube = n.Kube(a.namespace, a.context)
    tracker = load_trackers(a.state_dir or [".nrp"])
    if not tracker:
        print("no tracker found (looked for <state-dir>/*.tracker.json): nothing is safe to delete")
        return 1
    jobs, pods, nodes = kube.get_json("jobs"), kube.get_json("pods"), kube.get_json("nodes")
    done = finished_jobs(jobs, tracker, a.prefix)
    force, report = stuck_pods(pods, nodes, a.prefix, a.stuck_min)
    print(f"finished Jobs recorded SUCCEEDED: {len(done)}")
    print(f"stuck pods on a gone / NotReady node (force-delete): {len(force)} {force[:5]}")
    if report:
        print(f"stuck pods on a Ready node (report only): {report[:5]}")
    if not a.apply:
        print("dry run: nothing deleted (add --apply)")
        return 0
    for i in range(0, len(done), 40):
        kube.run("delete", "job", *done[i:i + 40], "--wait=false", timeout=300)
    if force:
        kube.run("delete", "pod", *force, "--grace-period=0", "--force", timeout=300)
    print(f"deleted {len(done)} Job(s), force-deleted {len(force)} pod(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
