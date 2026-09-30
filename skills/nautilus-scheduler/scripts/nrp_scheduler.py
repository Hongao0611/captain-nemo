#!/usr/bin/env python3
"""Schedule the Jobs in a multi-document YAML manifest on Nautilus (NRP).

- Keeps up to --max-concurrent Jobs running (live-adjustable: <state>/<stem>.max).
- --pace budget (default): launches only while fewer than 4 of your pods violate
  NRP's usage bands, counted the way the portal counts them (Prometheus), so the
  admission gate never closes and nothing lingers on the Violations page.
  --pace greedy: fill every free slot; gate denials are retried later.
- Excludes the nodes in the bad-nodes file at apply time (re-read every launch).
- Tracker <state>/<stem>.tracker.json is the completion record: finished Jobs
  expire from the cluster after 24 h. Never delete it mid-wave.
- Reloads the manifest when the file changes; <state>/<stem>.pause stops launches.
- Exit 0 = all SUCCEEDED; 3 = finished with FAILED/INVALID/UNKNOWN jobs;
  2 = usage error. Run it under run_wave.sh, which respawns on anything else.
"""
import argparse
import copy
import json
import os
import re
import shlex
import subprocess
import sys
import time

import yaml

import nrp_common as n

TERMINAL = ("SUCCEEDED", "FAILED", "INVALID", "UNKNOWN")
RETRY_SLEEP = int(os.environ.get("NRP_RETRY_SLEEP", "30"))   # tests shorten it


def die(msg):
    """Configuration errors exit 2, which run_wave.sh does not respawn."""
    print(msg, file=sys.stderr)
    sys.exit(2)


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", required=True, help="multi-document YAML of batch/v1 Jobs")
    ap.add_argument("--prefix", required=True,
                    help="job-name prefix that marks YOUR jobs (e.g. 'me-'); the namespace is "
                         "shared, so jobs without it are refused and never touched")
    ap.add_argument("--namespace", default=None)
    ap.add_argument("--context", default=None)
    ap.add_argument("--max-concurrent", type=int, default=10)
    ap.add_argument("--pace", choices=("budget", "greedy"), default="budget")
    ap.add_argument("--violation-budget", type=int, default=n.GATE_MAX_VIOLATORS,
                    help="launch only while violators + presumed < this (default 4: the gate "
                         "refuses new pods when MORE than 4 violate)")
    ap.add_argument("--retries", type=int, default=2,
                    help="relaunches of a Job that ended Failed (each Job also retries pods "
                         "itself up to its backoffLimit)")
    ap.add_argument("--state-dir", default=".nrp")
    ap.add_argument("--bad-nodes", default=n.BAD_NODES_FILE)
    ap.add_argument("--poll", type=int, default=300, help="seconds between cluster polls")
    ap.add_argument("--gate-retry", type=int, default=900,
                    help="seconds to wait after an admission-gate denial")
    ap.add_argument("--only", default=None, help="regex: schedule only matching job names")
    ap.add_argument("--on-complete", default=None,
                    help="shell command run once when every job SUCCEEDED")
    ap.add_argument("--dry-run", action="store_true",
                    help="read the cluster and decide, but create/delete nothing and use a "
                         "throwaway tracker (<stem>.dryrun.json)")
    ap.add_argument("--once", action="store_true", help="one sync+launch pass, then exit 1")
    return ap.parse_args()


class Wave:
    def __init__(self, a):
        self.a = a
        self.kube = n.Kube(a.namespace, a.context, dry_run=a.dry_run)
        self.stem = os.path.splitext(os.path.basename(a.manifest))[0]
        os.makedirs(a.state_dir, exist_ok=True)
        base = os.path.join(a.state_dir, self.stem)
        self.tracker_path = base + (".dryrun.json" if a.dry_run else ".tracker.json")
        self.real_tracker = base + ".tracker.json"
        self.max_path, self.pause_path = base + ".max", base + ".pause"
        self.requeue_path = base + ".requeue"
        self.logs_dir = os.path.join(a.state_dir, "failed_logs")
        self.jobs, self.order, self.manifest_mtime = {}, [], None
        self.state = self.load_tracker()
        self.gate_until = 0.0
        self.auth_warned = False

    # ------------------------------------------------------------- manifest
    def load_manifest(self):
        mtime = os.path.getmtime(self.a.manifest)
        if mtime == self.manifest_mtime:
            return
        docs = [d for d in yaml.safe_load_all(open(self.a.manifest)) if d]
        jobs, order = {}, []
        for d in docs:
            name = (d.get("metadata") or {}).get("name")
            if d.get("kind") != "Job" or not name:
                n.log(f"[skip] non-Job document or unnamed job in {self.a.manifest}")
                continue
            if not name.startswith(self.a.prefix):
                die(f"refusing {name!r}: does not start with --prefix {self.a.prefix!r}")
            if self.a.only and not re.search(self.a.only, name):
                continue
            if name in jobs:
                die(f"duplicate job name {name!r} in {self.a.manifest}")
            jobs[name] = d
            order.append(name)
        if self.manifest_mtime is not None:
            added = set(order) - set(self.order)
            n.log(f"[manifest] reloaded: {len(order)} jobs ({len(added)} new)")
        self.jobs, self.order, self.manifest_mtime = jobs, order, mtime
        for name in order:
            self.state.setdefault(name, {"status": "PENDING", "attempts": 0})

    # -------------------------------------------------------------- tracker
    def load_tracker(self):
        # A dry run starts from the real tracker (read-only) so it decides as the
        # live scheduler would, but writes only its own throwaway copy.
        path = self.tracker_path if os.path.exists(self.tracker_path) or not self.a.dry_run \
            else self.real_tracker
        if not os.path.exists(path):
            return {}
        raw = json.load(open(path))
        state = {}
        for k, v in raw.items():
            if not isinstance(v, dict):
                # Older flat {name: "STATUS"} trackers: APPLY_FAILED meant "parked by
                # the gate", FAILED meant "retry on the next restart" -- both PENDING here.
                v = {"status": "PENDING" if v in ("APPLY_FAILED", "FAILED") else v, "attempts": 0}
            if v.get("status") not in ("PENDING", "RUNNING") + TERMINAL:
                n.log(f"[tracker] {k}: unknown status {v.get('status')!r} -> PENDING")
                v["status"] = "PENDING"
            state[k] = v
        return state

    def save_tracker(self):
        tmp = self.tracker_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f, indent=1, sort_keys=True)
        os.replace(tmp, self.tracker_path)   # atomic: a crash never leaves half a tracker

    def set(self, name, status, **extra):
        s = self.state[name]
        if s.get("status") != status:
            s["since"] = n.ts()
        s["status"] = status
        s.update(extra)

    # ----------------------------------------------------------------- sync
    def consume_requeue(self, cluster):
        """<stem>.requeue: job names (one per line) to run again -- after an
        UNKNOWN / FAILED / INVALID verdict, or to deliberately rerun a job."""
        if not os.path.exists(self.requeue_path):
            return
        names = [l.strip() for l in open(self.requeue_path) if l.strip() and not l.startswith("#")]
        for name in names:
            if name not in self.state or name not in self.jobs:
                n.log(f"[requeue] {name}: not in the manifest; ignored")
            elif name in cluster and n.job_status(cluster[name]) == "RUNNING":
                n.log(f"[requeue] {name}: still running in the cluster; ignored")
            else:
                if name in cluster:
                    # A finished Job lingers until its TTL; delete it, or the
                    # next sync would re-adopt its old verdict.
                    try:
                        self.kube.delete_job(name)
                    except n.KubectlError as e:
                        n.log(f"[requeue] {name}: could not delete the finished Job ({str(e)[:120]}); retry later")
                        continue
                    cluster.pop(name)
                n.log(f"[requeue] {name}: {self.state[name]['status']} -> PENDING")
                self.set(name, "PENDING", attempts=0)
        if not self.a.dry_run:
            left = [x for x in names if x in self.state and self.state[x]["status"] != "PENDING"
                    and not (x in cluster and n.job_status(cluster[x]) == "RUNNING")]
            if left:
                open(self.requeue_path, "w").write("".join(f"{x}\n" for x in left))
            else:
                os.remove(self.requeue_path)

    def sync(self):
        """Merge cluster reality into the tracker; relaunch failed Jobs."""
        try:
            cluster = {j["metadata"]["name"]: j for j in self.kube.get_json("jobs")}
        except n.KubectlError as e:
            self.report_kube_error("get jobs", e)
            return None
        self.auth_warned = False
        self.consume_requeue(cluster)
        now = time.time()
        for name in self.order:
            s = self.state[name]
            j = cluster.get(name)
            if j is not None:
                st = n.job_status(j)
                if s["status"] in ("PENDING", "UNKNOWN") and st == "RUNNING":
                    n.log(f"[adopt] {name} is already in the cluster")
                if st == "FAILED" and s["status"] != "FAILED":
                    self.handle_failed(name)
                elif st != "FAILED":
                    self.set(name, st, seen=now)
                continue
            if s["status"] == "RUNNING":
                # Gone while we believed it running. Seen recently -> deleted on
                # purpose (hung job, node cleanup): relaunch. Not seen for longer
                # than the 24 h finished-Job TTL -> it may have finished while no
                # scheduler watched; do NOT rerun blindly.
                if now - s.get("seen", 0) < 3 * self.a.poll + 600:
                    n.log(f"[requeue] {name} vanished from the cluster; relaunching")
                    self.set(name, "PENDING")
                else:
                    n.log(f"[unknown] {name} vanished while unobserved (> TTL?); not relaunched")
                    self.set(name, "UNKNOWN")
        self.save_tracker()
        return cluster

    def handle_failed(self, name):
        s = self.state[name]
        self.save_failed_logs(name, s.get("attempts", 0))
        if s.get("attempts", 0) <= self.a.retries:
            n.log(f"[failed] {name} (attempt {s.get('attempts', 0)}); "
                  f"{'would delete' if self.a.dry_run else 'deleting'} it for a relaunch")
            try:
                self.kube.delete_job(name)
                self.set(name, "PENDING")
            except n.KubectlError as e:
                n.log(f"[warn] could not delete failed {name}: {str(e)[:160]}")
        else:
            n.log(f"[failed] {name}: out of retries ({self.a.retries}); leaving it FAILED")
            self.set(name, "FAILED")

    def save_failed_logs(self, name, attempt):
        os.makedirs(self.logs_dir, exist_ok=True)
        path = os.path.join(self.logs_dir, f"{name}.attempt{attempt}.log")
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write(self.kube.logs(f"job/{name}", tail=400))

    def report_kube_error(self, what, e):
        if e.kind == "auth":
            if not self.auth_warned:
                n.log(f"[AUTH] kubectl credentials expired during {what}: a human must log in "
                      "(run any kubectl command in a terminal). Waiting; running Jobs are unaffected.")
                self.auth_warned = True
        else:
            n.log(f"[warn] {what}: {str(e)[:200]}")

    # --------------------------------------------------------------- launch
    def capacity(self):
        try:
            return max(0, int(open(self.max_path).read().strip()))
        except (OSError, ValueError):
            return self.a.max_concurrent

    def budget_left(self, cluster):
        """Launches allowed now by the violation budget (greedy -> unlimited)."""
        if self.a.pace == "greedy":
            return 10**9
        try:
            real, presumed, _ = n.count_violators(self.kube.namespace, self.a.prefix,
                                                  list(cluster.values()))
        except Exception as e:
            n.log(f"[warn] Prometheus unavailable ({str(e)[:120]}); not launching this pass")
            return 0
        left = self.a.violation_budget - real - presumed
        if left <= 0:
            n.log(f"[budget] {real} violating + {presumed} presumed (new/pending) pods >= "
                  f"{self.a.violation_budget}; holding launches")
        return max(0, left)

    def render(self, name):
        job = copy.deepcopy(self.jobs[name])
        n.exclude_nodes(job, n.load_bad_nodes(self.a.bad_nodes))
        return yaml.safe_dump(job, sort_keys=False)

    def launch(self, cluster):
        if os.path.exists(self.pause_path):
            n.log(f"[pause] {self.pause_path} exists; no launches")
            return
        if time.time() < self.gate_until:
            return
        running = sum(self.state[x]["status"] == "RUNNING" for x in self.order)
        pending = [x for x in self.order if self.state[x]["status"] == "PENDING"]
        free = self.capacity() - running
        if free <= 0 or not pending:
            return
        with n.LaunchLock():
            allowed = min(free, self.budget_left(cluster))
            for name in pending[:allowed]:
                if not self.create(name):
                    break
            self.save_tracker()

    def create(self, name):
        """Create one Job. Returns False when launching should stop for now."""
        body = self.render(name)
        for attempt in range(1, 4):
            try:
                self.kube.create(body)
                s = self.state[name]
                self.set(name, "RUNNING", attempts=s.get("attempts", 0) + 1, seen=time.time())
                n.log(f"[{'would launch' if self.a.dry_run else 'launch'}] {name} (attempt {s['attempts']})")
                return True
            except n.KubectlError as e:
                msg = str(e)
                if e.kind in ("gate", "quota"):
                    self.gate_until = time.time() + self.a.gate_retry
                    why = (f"NRP refused new pods (> {n.GATE_MAX_VIOLATORS} violators)" if e.kind == "gate"
                           else f"namespace quota exceeded: {msg[:160]}")
                    n.log(f"[{e.kind}] {why}; retrying in {self.a.gate_retry}s")
                    return False
                if e.kind == "auth":
                    self.report_kube_error(f"create {name}", e)
                    return False
                if e.kind == "exists":
                    n.log(f"[adopt] {name} already exists; tracking it")
                    self.set(name, "RUNNING", seen=time.time())
                    return True
                if e.kind == "permanent":
                    n.log(f"[invalid] {name}: {msg[:300]}")
                    self.set(name, "INVALID", error=msg[:500])
                    return True
                n.log(f"[retry] {name} attempt {attempt}: {msg[:160]}")
                time.sleep(RETRY_SLEEP * attempt)
        return False    # transient errors persisted: try again next pass

    # ----------------------------------------------------------------- loop
    def summary(self):
        c = {}
        for x in self.order:
            c[self.state[x]["status"]] = c.get(self.state[x]["status"], 0) + 1
        return c

    def run(self):
        n.log(f"=== {self.stem}: pace={self.a.pace} max={self.capacity()} tracker={self.tracker_path}"
              + (" DRY-RUN" if self.a.dry_run else ""))
        while True:
            self.load_manifest()
            cluster = self.sync()
            if cluster is not None:
                self.launch(cluster)
            c = self.summary()
            n.log(f"[status] {c}")
            if all(self.state[x]["status"] in TERMINAL for x in self.order):
                break
            if self.a.once:
                return 1
            time.sleep(self.a.poll)
        bad = {k: v for k, v in self.summary().items() if k != "SUCCEEDED"}
        if bad:
            n.log(f"=== finished with problems: {bad} (see {self.tracker_path})")
            return 3
        n.log("=== all jobs SUCCEEDED")
        if self.a.on_complete and not self.a.dry_run:
            rc = subprocess.run(self.a.on_complete, shell=True).returncode
            n.log(f"[on-complete] {shlex.quote(self.a.on_complete)} exited {rc}")
        return 0


def main():
    a = parse_args()
    if not os.path.exists(a.manifest):
        print(f"no such manifest: {a.manifest}", file=sys.stderr)
        return 2
    return Wave(a).run()


if __name__ == "__main__":
    sys.exit(main())
