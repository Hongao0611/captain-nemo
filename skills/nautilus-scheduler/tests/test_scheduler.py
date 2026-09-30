#!/usr/bin/env python3
"""Offline tests of nrp_scheduler.py / run_wave.sh against a simulated cluster
(fake_kubectl.py + an in-process fake Prometheus). Touches no real cluster.

    python3 tests/test_scheduler.py            # all scenarios, ~2 min
    python3 tests/test_scheduler.py budget     # scenarios whose name contains 'budget'
"""
import http.server
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(os.path.dirname(HERE), "scripts")
PY = sys.executable


# ------------------------------------------------------------ fake Prometheus
class Prom(http.server.BaseHTTPRequestHandler):
    state_path = None

    def log_message(self, *a):
        pass

    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)["query"][0]
        s = json.load(open(self.state_path))
        now = time.time()
        rows = []
        for name, j in s["jobs"].items():
            ann = j["manifest"]["metadata"].get("annotations") or {}
            age = now - j["created"]
            if age < float(ann.get("fake/pod_delay", 0)):
                continue                      # pod not yet visible to kube-state-metrics
            pod = name + "-abcde"
            done = age >= float(ann.get("fake/duration", 3))
            phase = ("Failed" if ann.get("fake/outcome") == "fail" else "Succeeded") if done else "Running"
            young = age < float(ann.get("fake/young", 0))
            if "kube_pod_status_phase" in q:
                rows.append(({"pod": pod, "phase": phase}, 1))
            elif "kube_pod_info" in q:
                rows.append(({"pod": pod, "node": "node-a"}, 1))
            elif "kube_pod_start_time" in q or "kube_pod_created" in q:
                rows.append(({"pod": pod}, j["created"]))
            elif "kube_pod_deletion_timestamp" in q:
                pass
            elif "kube_pod_container_resource_requests" in q:
                rows += [({"pod": pod, "resource": "cpu"}, 1), ({"pod": pod, "resource": "memory"}, 1e9),
                         ({"pod": pod, "resource": "nvidia_com_gpu"}, 1)]
            elif "DCGM_FI_DEV_GPU_UTIL" in q:
                rows.append(({"pod": pod}, 5 if young else 90))
            elif "container_cpu_usage" in q:
                rows.append(({"pod": pod}, 0.5))
            elif "container_memory_rss" in q:
                rows.append(({"pod": pod}, 5e8))
        body = {"status": "success", "data": {"resultType": "vector",
                "result": [{"metric": m, "value": [now, str(v)]} for m, v in rows]}}
        out = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(out)


# ------------------------------------------------------------------ harness
class Env:
    def __init__(self, jobs, knobs=None, bad_nodes=("bad-node-1",)):
        self.dir = tempfile.mkdtemp(prefix="nrp-test-")
        self.state = os.path.join(self.dir, "cluster.json")
        json.dump({"jobs": {}, **(knobs or {})}, open(self.state, "w"))
        self.manifest = os.path.join(self.dir, "wave.yaml")
        self.write_manifest(jobs)
        self.bad = os.path.join(self.dir, "bad_nodes.txt")
        open(self.bad, "w").write("".join(f"{b}  # test\n" for b in bad_nodes))
        self.statedir = os.path.join(self.dir, "state")
        bindir = os.path.join(self.dir, "bin")
        os.makedirs(bindir)
        shim = os.path.join(bindir, "kubectl")
        open(shim, "w").write(f'#!/bin/sh\nexec {PY} {HERE}/fake_kubectl.py "$@"\n')
        os.chmod(shim, 0o755)
        Prom.state_path = self.state
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Prom)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}", FAKE_STATE=self.state,
                        NRP_PROM_URL=f"http://127.0.0.1:{self.httpd.server_port}",
                        NRP_RETRY_SLEEP="0", NRP_RESPAWN_SLEEP="1", NRP_PYTHON=PY)

    def write_manifest(self, jobs):
        docs = []
        for name, ann in jobs:
            docs.append({"apiVersion": "batch/v1", "kind": "Job",
                         "metadata": {"name": name, "annotations": {k: str(v) for k, v in ann.items()}},
                         "spec": {"backoffLimit": 1, "template": {"spec": {
                             "restartPolicy": "Never",
                             "containers": [{"name": "c", "image": "busybox", "command": ["true"]}]}}}})
        yaml.safe_dump_all(docs, open(self.manifest, "w"))

    def args(self, *extra):
        return [PY, "-u", os.path.join(SCRIPTS, "nrp_scheduler.py"), "--manifest", self.manifest,
                "--prefix", "t-", "--state-dir", self.statedir, "--bad-nodes", self.bad,
                "--poll", "1", "--gate-retry", "2", *extra]

    def run(self, *extra, timeout=120):
        r = subprocess.run(self.args(*extra), env=self.env, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout + r.stderr

    def popen(self, *extra):
        return subprocess.Popen(self.args(*extra), env=self.env, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)

    def cluster(self):
        return json.load(open(self.state))

    def patch(self, **kv):
        s = self.cluster()
        s.update(kv)
        json.dump(s, open(self.state, "w"))

    def tracker(self):
        return json.load(open(os.path.join(self.statedir, "wave.tracker.json")))

    def close(self):
        self.httpd.shutdown()


def max_concurrent(creates, durations):
    ev = sorted([(c["t"], 1) for c in creates] + [(c["t"] + durations[c["name"]], -1) for c in creates])
    cur = best = 0
    for _, d in ev:
        cur += d
        best = max(best, cur)
    return best


# ---------------------------------------------------------------- scenarios
def t_greedy_basic():
    jobs = [(f"t-job{i}", {"fake/duration": 2}) for i in range(5)]
    e = Env(jobs)
    rc, out = e.run("--pace", "greedy", "--max-concurrent", "2")
    c = e.cluster()
    assert rc == 0, out
    assert all(v["status"] == "SUCCEEDED" for v in e.tracker().values()), e.tracker()
    assert len(c["creates"]) == 5
    assert max_concurrent(c["creates"], {n: 2 for n, _ in jobs}) <= 2
    aff = c["creates"][0]["manifest"]["spec"]["template"]["spec"]["affinity"]
    expr = aff["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchExpressions"][0]
    assert expr == {"key": "kubernetes.io/hostname", "operator": "NotIn", "values": ["bad-node-1"]}, expr
    e.close()


def t_budget_pacing():
    # Budget 2, every new pod violates for its first 3 s, max 6: never more than
    # 2 young pods at once even though 6 slots are free.
    jobs = [(f"t-job{i}", {"fake/duration": 5, "fake/young": 3}) for i in range(6)]
    e = Env(jobs)
    rc, out = e.run("--pace", "budget", "--max-concurrent", "6", "--violation-budget", "2")
    c = e.cluster()
    assert rc == 0, out
    times = sorted(x["t"] for x in c["creates"])
    worst = max(sum(1 for u in times if t - 3 < u <= t) for t in times)
    assert worst <= 2, (worst, [round(t - times[0], 1) for t in times])
    assert "[budget]" in out, out
    e.close()


def t_budget_counts_invisible_new_jobs():
    # Pods show up in "Prometheus" only 3 s after the Job is created: the
    # scheduler must still count the fresh Jobs (presumed violators).
    jobs = [(f"t-job{i}", {"fake/duration": 6, "fake/young": 4, "fake/pod_delay": 3}) for i in range(4)]
    e = Env(jobs)
    rc, out = e.run("--pace", "budget", "--max-concurrent", "4", "--violation-budget", "1")
    times = sorted(x["t"] for x in e.cluster()["creates"])
    assert rc == 0, out
    assert all(b - a >= 3 for a, b in zip(times, times[1:])), [round(t - times[0], 1) for t in times]
    e.close()


def t_gate_denial():
    jobs = [(f"t-job{i}", {"fake/duration": 1}) for i in range(3)]
    e = Env(jobs, knobs={"gate_closed_until": time.time() + 3})
    rc, out = e.run("--pace", "greedy", "--max-concurrent", "3")
    assert rc == 0, out
    assert "[gate]" in out, out
    assert min(x["t"] for x in e.cluster()["creates"]) >= e.cluster()["gate_closed_until"]
    e.close()


def t_failed_retries_then_exit3():
    jobs = [("t-ok", {"fake/duration": 1}), ("t-bad", {"fake/duration": 1, "fake/outcome": "fail"})]
    e = Env(jobs)
    rc, out = e.run("--pace", "greedy", "--retries", "1")
    tr = e.tracker()
    assert rc == 3, out
    assert tr["t-bad"]["status"] == "FAILED" and tr["t-bad"]["attempts"] == 2, tr
    assert tr["t-ok"]["status"] == "SUCCEEDED"
    assert sum(1 for x in e.cluster()["creates"] if x["name"] == "t-bad") == 2
    assert os.path.exists(os.path.join(e.statedir, "failed_logs", "t-bad.attempt1.log"))
    e.close()


def t_transient_create_errors():
    e = Env([("t-a", {"fake/duration": 1})], knobs={"transient_creates": 2})
    rc, out = e.run("--pace", "greedy")
    assert rc == 0, out
    assert out.count("[retry]") == 2, out
    e.close()


def t_auth_expiry():
    e = Env([("t-a", {"fake/duration": 1})], knobs={"auth_down_until": time.time() + 3})
    rc, out = e.run("--pace", "greedy")
    assert rc == 0, out
    assert out.count("[AUTH]") == 1, out      # warned once, not every poll
    e.close()


def t_vanished_running_job_is_requeued():
    e = Env([("t-a", {"fake/duration": 4})])
    p = e.popen("--pace", "greedy")
    time.sleep(2)
    e.patch(jobs={})                           # someone deletes the running Job
    out = p.communicate(timeout=60)[0]
    assert p.returncode == 0, out
    assert "[requeue] t-a vanished" in out, out
    assert len(e.cluster()["creates"]) == 2
    e.close()


def t_requeue_file_and_reload():
    jobs = [("t-a", {"fake/duration": 1})]
    e = Env(jobs)
    rc, out = e.run("--pace", "greedy")
    assert rc == 0, out
    # Deliberate rerun of a SUCCEEDED job + a job appended to the manifest while running.
    open(os.path.join(e.statedir, "wave.requeue"), "w").write("t-a\n")
    e.write_manifest([("t-a", {"fake/duration": 4})])   # rerun long enough to see the reload
    p = e.popen("--pace", "greedy")
    time.sleep(1.5)
    e.write_manifest([("t-a", {"fake/duration": 4}), ("t-b", {"fake/duration": 1})])
    out = p.communicate(timeout=60)[0]
    assert p.returncode == 0, out
    names = [x["name"] for x in e.cluster()["creates"]]
    assert names.count("t-a") == 2 and names.count("t-b") == 1, (names, out)
    assert not os.path.exists(os.path.join(e.statedir, "wave.requeue"))
    e.close()


def t_pause_and_live_max():
    jobs = [(f"t-job{i}", {"fake/duration": 1}) for i in range(4)]
    e = Env(jobs)
    os.makedirs(e.statedir, exist_ok=True)
    open(os.path.join(e.statedir, "wave.pause"), "w").close()
    open(os.path.join(e.statedir, "wave.max"), "w").write("1\n")
    p = e.popen("--pace", "greedy", "--max-concurrent", "4")
    time.sleep(3)
    assert not e.cluster().get("creates"), "launched while paused"
    os.remove(os.path.join(e.statedir, "wave.pause"))
    out = p.communicate(timeout=60)[0]
    assert p.returncode == 0, out
    assert max_concurrent(e.cluster()["creates"], {n: 1 for n, _ in jobs}) <= 1
    e.close()


def t_prefix_and_duplicate_refused():
    e = Env([("other-job", {})])
    rc, out = e.run()
    assert rc == 2 and "refusing" in out, out
    e.write_manifest([("t-a", {}), ("t-a", {})])
    rc, out = e.run()
    assert rc == 2 and "duplicate" in out, out
    assert not e.cluster().get("creates")
    e.close()


def t_adopts_existing_and_dry_run():
    e = Env([("t-a", {"fake/duration": 3}), ("t-b", {"fake/duration": 3})])
    m = next(yaml.safe_load_all(open(e.manifest)))
    e.patch(jobs={"t-a": {"manifest": m, "created": time.time()}})   # already in the cluster
    rc, out = e.run("--pace", "greedy", "--dry-run", "--once")
    assert rc == 1, out
    assert "[adopt] t-a" in out and "[would launch] t-b" in out, out
    assert not e.cluster().get("creates"), "dry run created a job"
    assert not os.path.exists(os.path.join(e.statedir, "wave.tracker.json")), "dry run wrote the real tracker"
    e.close()


def t_wrapper_respawn_and_stop():
    e = Env([("t-a", {"fake/duration": 30})])
    log = os.path.join(e.dir, "wave.log")
    wrapper = subprocess.Popen(["bash", os.path.join(SCRIPTS, "run_wave.sh"), log] + e.args("--pace", "greedy")[3:],
                               env=e.env, start_new_session=True)
    pidf = os.path.join(e.statedir, "wave.wave.pid")
    spidf = os.path.join(e.statedir, "wave.scheduler.pid")
    for _ in range(50):
        if os.path.exists(spidf):
            break
        time.sleep(0.2)
    first = int(open(spidf).read())
    os.kill(first, signal.SIGTERM)                       # scheduler dies -> respawn
    for _ in range(50):
        time.sleep(0.2)
        if os.path.exists(spidf) and int(open(spidf).read()) != first:
            break
    assert int(open(spidf).read()) != first, open(log).read()
    # a second wrapper for the same manifest is refused
    r = subprocess.run(["bash", os.path.join(SCRIPTS, "run_wave.sh"), log] + e.args()[3:], env=e.env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 2 and "already running" in r.stderr, r.stderr
    os.kill(int(open(pidf).read()), signal.SIGTERM)      # stop the whole wave
    assert wrapper.wait(timeout=30) == 143
    assert not os.path.exists(pidf)
    second = int(open(spidf).read())
    time.sleep(0.5)
    assert subprocess.run(["kill", "-0", str(second)], capture_output=True).returncode != 0, "orphaned scheduler"
    assert len(e.cluster()["creates"]) == 1, "respawned scheduler relaunched a running job"
    e.close()


if __name__ == "__main__":
    pick = sys.argv[1] if len(sys.argv) > 1 else ""
    tests = [(k, v) for k, v in globals().items() if k.startswith("t_") and pick in k]
    failed = 0
    for name, fn in tests:
        t0 = time.time()
        try:
            fn()
            print(f"PASS {name} ({time.time() - t0:.1f}s)")
        except Exception as ex:
            failed += 1
            print(f"FAIL {name}: {type(ex).__name__}: {str(ex)[:1500]}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
