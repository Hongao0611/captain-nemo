#!/usr/bin/env python3
"""Stand-in for kubectl, backed by the JSON file $FAKE_STATE (used by test_scheduler.py).

Jobs run for annotation fake/duration seconds (default 3) and then end with
fake/outcome ("succeed" | "fail"). State knobs: gate_closed_until (epoch),
auth_down_until (epoch), transient_creates (count of creates to fail)."""
import json
import os
import sys
import time

import yaml

STATE = os.environ["FAKE_STATE"]


def load():
    return json.load(open(STATE))


def save(s):
    tmp = STATE + ".tmp"
    json.dump(s, open(tmp, "w"))
    os.replace(tmp, STATE)


def job_json(j):
    ann = j["manifest"]["metadata"].get("annotations") or {}
    done = time.time() >= j["created"] + float(ann.get("fake/duration", 3))
    cond = []
    if done:
        cond = [{"type": "Failed" if ann.get("fake/outcome") == "fail" else "Complete", "status": "True"}]
    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(j["created"]))
    return {"metadata": {"name": j["manifest"]["metadata"]["name"], "creationTimestamp": created},
            "status": {"conditions": cond, "active": 0 if done else 1}}


def fail(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)


def main(argv):
    while argv and argv[0] in ("--context", "-n", "--namespace"):
        argv = argv[2:]
    s = load()
    if argv[:2] == ["config", "view"]:
        print("testns")
        return
    if time.time() < s.get("auth_down_until", 0):
        fail("error: You must be logged in to the server (Unauthorized)")
    if argv[:2] == ["get", "jobs"]:
        print(json.dumps({"items": [job_json(j) for j in s["jobs"].values()]}))
    elif argv[:1] == ["create"]:
        m = yaml.safe_load(sys.stdin.read())
        name = m["metadata"]["name"]
        if "--dry-run=server" in argv:
            if time.time() < s.get("gate_closed_until", 0):
                fail('Error from server: admission webhook "job.nrp-nautilus.io" denied the request: '
                     "Your pods resources utilization is too low for account")
            print(f"job.batch/{name} created (server dry run)")
            return
        if s.get("transient_creates", 0) > 0:
            s["transient_creates"] -= 1
            save(s)
            fail("error: http2: client connection lost")
        if time.time() < s.get("gate_closed_until", 0):
            fail('Error from server: admission webhook "job.nrp-nautilus.io" denied the request: '
                 "Your pods resources utilization is too low for account")
        if name in s["jobs"]:
            fail(f'Error from server (AlreadyExists): jobs.batch "{name}" already exists')
        s["jobs"][name] = {"manifest": m, "created": time.time()}
        s.setdefault("creates", []).append({"name": name, "t": time.time(), "manifest": m})
        save(s)
        print(f"job.batch/{name} created")
    elif argv[:2] == ["delete", "job"]:
        s["jobs"].pop(argv[2], None)
        s.setdefault("deletes", []).append(argv[2])
        save(s)
        print(f'job.batch "{argv[2]}" deleted')
    elif argv[:1] == ["logs"]:
        print(f"fake log for {argv[1]}")
    else:
        fail(f"fake kubectl: unsupported {argv}")


if __name__ == "__main__":
    main(sys.argv[1:])
