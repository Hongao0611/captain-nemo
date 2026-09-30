#!/usr/bin/env python3
"""One-shot health report for your jobs on Nautilus -- the hourly check.

Read-only: it creates, deletes and edits nothing (the optional --gate-probe is
a server-side dry run, which creates nothing either).

Sections: waves (trackers + scheduler liveness), NRP violations and the launch
budget, failures since --since grouped by node and cause (with exclusion
suggestions), Jobs still able to land on bad nodes, Pending reasons, idle /
possibly hung pods, and optionally the admission gate.

Exit codes: 0 ok, 4 kubectl credentials expired (a human must log in).
"""
import argparse
import collections
import datetime as dt
import glob
import json
import os
import re
import sys
import time

import yaml

import nrp_common as n

# Failure causes, checked in order against "<pod reason> <container reason> <log tail>".
# node=True: the cause lives on the node, so repeats there justify excluding it.
SIGNATURES = [
    ("gpu-unusable", True, r"CUDA error: unspecified launch failure|CUDA unknown error|No CUDA GPUs are available"
                           r"|requires Ampere|cudaGetDeviceCount|Unable to determine the device handle"
                           r"|CUDA-capable device\S* (is|are) (busy|unavailable)|NVML|Xid"),
    ("gpu-admission", True, r"UnexpectedAdmissionError"),
    ("node-lost", True, r"ContainerStatusUnknown|NodeLost|node .* not ready|Evicted"),
    ("storage-stall", True, r"dataset copy stalled|HF login stalled|Input/output error"
                            r"|Transport endpoint is not connected|Stale file handle"),
    ("network-stall", True, r"(git clone|apt-get|pip install|download) stalled|unexpected disconnect while reading sideband"
                            r"|early EOF|RPC failed|Could not resolve host|Temporary failure in name resolution"
                            r"|Network is unreachable|Connection timed out"),
    ("setup-stall", True, r"stalled on node"),
    ("oom", False, r"OOMKilled|Killed process|out of memory"),
    ("hub-quota", False, r"429|Too Many Requests|rate limit"),
    ("app-error", False, r"Traceback|Error|error|exit"),
]


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prefix", required=True, help="your job-name prefix, e.g. 'me-'")
    ap.add_argument("--namespace", default=None)
    ap.add_argument("--context", default=None)
    ap.add_argument("--since", default=None, help="UTC start of the failure window (ISO); default --hours ago")
    ap.add_argument("--hours", type=float, default=2.0)
    ap.add_argument("--state-dir", action="append", default=None,
                    help="scheduler state dir(s) to summarize (repeatable; default .nrp if present)")
    ap.add_argument("--bad-nodes", default=n.BAD_NODES_FILE)
    ap.add_argument("--idle-min", type=int, default=60,
                    help="flag Running pods older than this whose GPU and CPU sat idle this long")
    ap.add_argument("--no-logs", action="store_true", help="skip log tails of failed pods (faster)")
    ap.add_argument("--gate-probe", default=None, metavar="MANIFEST",
                    help="server-side dry-run of this manifest's first Job (renamed) to test the gate")
    return ap.parse_args()


def age_min(iso):
    if not iso:
        return None
    t = dt.datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return (n.utcnow() - t).total_seconds() / 60


def section(title):
    print(f"\n== {title}")


def alive(pidfile, needle):
    """PID from pidfile if that process is still ours (PIDs are reused after a reboot)."""
    try:
        pid = int(open(pidfile).read().strip())
        cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode()
        return pid if needle in cmd else None
    except (OSError, ValueError):
        return None


def waves(state_dirs):
    section("waves")
    trackers = [t for d in state_dirs for t in sorted(glob.glob(os.path.join(d, "*.tracker.json")))]
    if not trackers:
        print("  no trackers found (pass --state-dir)")
    for t in trackers:
        stem = os.path.basename(t)[: -len(".tracker.json")]
        base = os.path.join(os.path.dirname(t), stem)
        st = json.load(open(t))
        c = collections.Counter((v["status"] if isinstance(v, dict) else v) for v in st.values())
        wave, sched = alive(base + ".wave.pid", "run_wave.sh"), alive(base + ".scheduler.pid", "nrp_scheduler.py")
        flags = []
        if os.path.exists(base + ".pause"):
            flags.append("PAUSED")
        if os.path.exists(base + ".max"):
            flags.append(f"max={open(base + '.max').read().strip()}")
        run = f"wrapper {wave}, scheduler {sched}" if wave else "NOT RUNNING"
        print(f"  {stem}: {dict(c)} | {run} {' '.join(flags)}")


def violations(ns, prefix, jobs):
    section("NRP violations (portal rules: GPU lifetime avg >= 40%, CPU 20-200%, MEM(RSS) 20-150% of requests)")
    try:
        real, presumed, live = n.count_violators(ns, prefix, jobs)
    except Exception as e:
        print(f"  Prometheus unavailable: {str(e)[:150]}")
        return
    now = time.time()
    capacity_wait, no_metrics = [], []
    for p, u in sorted(live.items()):
        flags, pre = n.judge(u, now)
        age = (now - (u["start"] or u["created"] or now)) / 60
        if flags:
            print(f"  VIOLATING {', '.join(flags):22s} age {age:5.0f} min  {u['node']}  {p}")
        elif pre:
            print(f"  presumed  (new, no samples yet)  age {age:5.0f} min  {u['phase']}  {p}")
        elif u["req_gpu"] > 0 and u["gpu_pct"] is None:
            (capacity_wait if u["phase"] == "Pending" else no_metrics).append((age, u, p))
        elif u["req_gpu"] > 0 and u["gpu_pct"] < n.GPU_MIN_PCT + 10:
            print(f"  at risk   GPU {u['gpu_pct']:.0f}% lifetime          age {age:5.0f} min  {u['node']}  {p}")
    for age, u, p in no_metrics:
        print(f"  no GPU metrics after {age:.0f} min on {u['node']} (node lacks a DCGM exporter, or stopped "
              f"reporting): {p}")
    if capacity_wait:
        oldest = max(a for a, _, _ in capacity_wait)
        print(f"  {len(capacity_wait)} GPU pods Pending > {n.PENDING_GRACE_S // 60} min (capacity or affinity; "
              f"oldest {oldest:.0f} min) -- not scored, not counted")
    left = n.GATE_MAX_VIOLATORS - real - presumed
    print(f"  => {real} violating + {presumed} presumed of {len(live)} live pods; gate refuses new pods "
          f"above {n.GATE_MAX_VIOLATORS} -> budget allows {max(0, left)} launch(es) now")


def classify(pod, log_tail):
    st = pod.get("status", {})
    reasons = [st.get("reason") or ""]
    for cs in st.get("containerStatuses") or []:
        term = (cs.get("state") or {}).get("terminated") or (cs.get("lastState") or {}).get("terminated") or {}
        reasons += [term.get("reason") or "", str(term.get("exitCode", ""))]
    text = " ".join(reasons) + " " + (log_tail or "")
    for name, node_fault, rx in SIGNATURES:
        if re.search(rx, text):
            return name, node_fault
    return "unknown", False


def failed_history(ns, prefix, since):
    """Failed pods in [since, now] from Prometheus -- survives Job/pod deletion
    (a recycled Job takes its failed pods with it). -> {pod: (node, reason)}"""
    sel = f'namespace="{ns}",pod=~"{n.prom_prefix_re(prefix)}"'
    t0 = dt.datetime.fromisoformat(since.replace("Z", "+00:00"))
    w = f"{max(60, int((n.utcnow() - t0).total_seconds()))}s"
    failed = {m["pod"] for m, v in n.prom(f'max_over_time(kube_pod_status_phase{{{sel},phase="Failed"}}[{w}]) == 1')}
    node = {}
    for m, v in n.prom(f"max_over_time(kube_pod_info{{{sel}}}[{w}])"):
        if m.get("node"):
            node[m["pod"]] = m["node"]
    reason = {}
    for metric in ("kube_pod_container_status_last_terminated_reason",
                   "kube_pod_container_status_terminated_reason", "kube_pod_status_reason"):
        for m, v in n.prom(f"max_over_time({metric}{{{sel}}}[{w}]) == 1"):
            if m.get("reason") not in (None, "", "Completed"):
                reason[m["pod"]] = m["reason"]      # later metrics (pod-level) win
    created = {m["pod"]: v for m, v in n.prom(f"max_over_time(kube_pod_created{{{sel}}}[{w}])")}
    return {p: (node.get(p), reason.get(p, ""), created.get(p)) for p in failed}


def failures(kube, ns, prefix, pods, since, bad, want_logs):
    section(f"failures since {since}")
    live = {p["metadata"]["name"]: p for p in pods}
    try:
        hist = failed_history(ns, prefix, since)
    except Exception as e:
        print(f"  Prometheus unavailable ({str(e)[:120]}); using live pods only")
        hist = {}
    for name, p in live.items():            # failed pods Prometheus has not scraped yet
        if p.get("status", {}).get("phase") == "Failed" and name not in hist:
            hist[name] = (p["spec"].get("nodeName"), p["status"].get("reason") or "", time.time())
    if not hist:
        print("  none")
        return
    rows, fetched = [], 0
    for name, (node, reason, when) in sorted(hist.items()):
        tail = ""
        if want_logs and name in live and fetched < 40:
            tail, fetched = kube.logs(name, tail=40), fetched + 1
        cls, node_fault = classify(live.get(name, {}), f"{reason} {tail}")
        rows.append((node or "(unknown node)", cls, node_fault, name,
                     (tail.strip().splitlines() or [""])[-1][:140], when or 0))
    by_node = collections.defaultdict(list)
    for r in rows:
        by_node[r[0]].append(r)
    for node, rs in sorted(by_node.items(), key=lambda kv: -len(kv[1])):
        c = collections.Counter(r[1] for r in rs)
        tag = " [already excluded]" if node in bad else ""
        print(f"  {node}{tag}: {len(rs)} failed -- {dict(c)}")
        for r in rs[:3]:
            print(f"      {r[3]}" + (f"  | {r[4]}" if r[4] else ""))
        if len(rs) > 3:
            print(f"      ... {len(rs) - 3} more")
    healthy_since = collections.defaultdict(list)     # node -> start times of pods running > 30 min
    for p in pods:
        st = p.get("status", {})
        age = age_min(st.get("startTime"))
        if st.get("phase") == "Running" and age and age > 30 and p["spec"].get("nodeName"):
            healthy_since[p["spec"]["nodeName"]].append(time.time() - age * 60)
    for node, rs in by_node.items():
        if node == "(unknown node)" or node in bad:
            continue          # excluded nodes: live exposure is checked in the next section
        nf = [r for r in rs if r[2]]
        gpu = [r for r in nf if r[1].startswith("gpu")]
        # A node that fails pods within seconds attracts every retry (it looks
        # free), so many failures of ANY cause on one node is the black-hole sign.
        suspect = len(gpu) >= 2 or len(nf) >= 3 or len(rs) >= 5
        if not suspect:
            if nf:
                print(f"  watch {node}: {len(nf)} node-type failure(s) ({nf[0][1]}); exclude if it repeats")
            continue
        last_fail = max(r[5] for r in rs)
        recovered = [t for t in healthy_since.get(node, []) if t > last_fail]
        causes = dict(collections.Counter(r[1] for r in rs))
        if recovered:
            print(f"  recovered? {node}: {causes}, but {len(recovered)} pod(s) started after the last failure "
                  f"have run > 30 min -- likely transient; watch")
        else:
            print(f"  EXCLUDE {node}: {causes}  ->  add it to {n.BAD_NODES_FILE}")


def excluded_by(pod):
    """Hostnames a pod's required node affinity keeps it off (per term; a node is
    only safely excluded if EVERY ORed term excludes it)."""
    terms = (((pod["spec"].get("affinity") or {}).get("nodeAffinity") or {})
             .get("requiredDuringSchedulingIgnoredDuringExecution") or {}).get("nodeSelectorTerms") or []
    if not terms:
        return set()
    sets = []
    for t in terms:
        ex = set()
        for e in t.get("matchExpressions") or []:
            if e.get("key") == "kubernetes.io/hostname" and e.get("operator") == "NotIn":
                ex |= set(e.get("values") or [])
        sets.append(ex)
    return set.intersection(*sets)


def exposed_jobs(pods, bad):
    """Live pods on a bad node, or whose Job template predates an exclusion (a
    Job's pod template is immutable: its retries can still land there)."""
    section("jobs exposed to bad nodes")
    on_bad, stale = [], collections.Counter()
    for p in pods:
        ph = p.get("status", {}).get("phase")
        if ph not in ("Running", "Pending"):
            continue
        node = p["spec"].get("nodeName")
        missing = set(bad) - excluded_by(p)
        if node in bad:
            on_bad.append(f"  {ph} ON EXCLUDED {node}: {p['metadata']['name']}")
        elif missing:
            stale[frozenset(missing)] += 1
    for line in on_bad:
        print(line)
    for missing, k in stale.items():
        print(f"  {k} live pod(s) whose Job does not exclude {len(missing)} bad node(s) "
              f"(e.g. {sorted(missing)[0]}): retries may land there; recycle them only if they fail")
    if on_bad:
        print("  -> delete those Jobs (the scheduler relaunches a Job it saw vanish, with the current exclusions)")
    if not on_bad and not stale:
        print("  none")


def summarize_unschedulable(msg):
    """'0/532 nodes are available: 3 Insufficient nvidia.com/gpu, 1 node(s) had untolerated
    taint {...}, ...' -> the three biggest causes, taints lumped together."""
    body = msg.split(". preemption:", 1)[0].split(": ", 1)[-1]
    counts = collections.Counter()
    for part in re.split(r",\s*(?=\d+ )", body):
        m = re.match(r"(\d+) (.+)", part.strip().rstrip("."))
        if m:
            why = m.group(2)
            if why.startswith("Preemption"):
                continue
            if "untolerated taint" in why:
                why = "tainted (reserved/under repair)"
            elif "affinity" in why or "selector" in why:
                why = "excluded by node affinity/selector"
            counts[why] += int(m.group(1))
    return ", ".join(f"{v} {k}" for k, v in counts.most_common(3)) or re.sub(r"\s+", " ", msg)[:110]


def pending(pods):
    section("pending pods")
    rows = []
    for p in pods:
        if p.get("status", {}).get("phase") != "Pending":
            continue
        msg = next((c.get("message", "") for c in p["status"].get("conditions") or []
                    if c.get("type") == "PodScheduled" and c.get("status") == "False"), "")
        waiting = next(((cs.get("state") or {}).get("waiting", {}).get("reason", "")
                        for cs in p["status"].get("containerStatuses") or []), "")
        why = ("unschedulable: " + summarize_unschedulable(msg)) if msg else (waiting or "starting")
        rows.append((age_min(p["metadata"].get("creationTimestamp")) or 0, p["metadata"]["name"], why))
    if not rows:
        print("  none")
    for a, name, why in sorted(rows, reverse=True)[:15]:
        print(f"  {a:5.0f} min  {name}  [{why}]")
    if len(rows) > 15:
        print(f"  ... {len(rows) - 15} more")


def idle(ns, prefix, idle_min):
    section(f"idle Running pods (GPU < 5% and CPU < 0.1 core for {idle_min} min: hung setup, dead dataloader, lost node)")
    sel = f'namespace="{ns}",pod=~"{n.prom_prefix_re(prefix)}"'
    w = f"{idle_min}m"
    try:
        old = {m["pod"] for m, v in n.prom(f"(time() - kube_pod_start_time{{{sel}}}) > {idle_min * 60}")}
        gpu = {m["pod"]: v for m, v in n.prom(f"avg by (pod) (avg_over_time(DCGM_FI_DEV_GPU_UTIL{{{sel}}}[{w}]))")}
        cpu = {m["pod"]: v for m, v in n.prom(
            f'sum by (pod) (rate(container_cpu_usage_seconds_total{{{sel},container!="",container!="POD"}}[{w}]))')}
        running = {m["pod"] for m, v in n.prom(f'kube_pod_status_phase{{{sel},phase="Running"}} == 1')}
    except Exception as e:
        print(f"  Prometheus unavailable: {str(e)[:150]}")
        return
    hits = [p for p in sorted(running & old) if gpu.get(p, 100) < 5 and cpu.get(p, 1) < 0.1]
    silent = [p for p in sorted(running & old) if p not in cpu]
    for p in hits:
        print(f"  IDLE {p}: GPU {gpu[p]:.0f}%, CPU {cpu.get(p, 0):.2f} cores")
    for p in silent:
        print(f"  NO SAMPLES {p}: no CPU metrics for {idle_min} min (node unreachable? check kubectl get pod)")
    if not hits and not silent:
        print("  none")


def gate_probe(kube, manifest, prefix):
    section("admission gate (server-side dry run; creates nothing)")
    doc = next(d for d in yaml.safe_load_all(open(manifest)) if d)
    doc["metadata"] = {"name": f"{prefix}gateprobe-{int(time.time()) % 100000}"}
    try:
        kube.create(yaml.safe_dump(doc), server_dry_run=True)
        print("  OPEN: a new Job would be admitted now")
    except n.KubectlError as e:
        state = "CLOSED (too many violating pods)" if e.kind == "gate" else f"refused ({e.kind})"
        print(f"  {state}: {str(e)[:200]}")


def main():
    a = parse_args()
    kube = n.Kube(a.namespace, a.context)
    since = a.since or (n.utcnow() - dt.timedelta(hours=a.hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    if re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d", since):
        since += ":00Z"
    state_dirs = a.state_dir or ([".nrp"] if os.path.isdir(".nrp") else [])
    print(f"NRP status {n.ts()}  namespace={kube.namespace}  prefix={a.prefix}")
    try:
        jobs = [j for j in kube.get_json("jobs") if j["metadata"]["name"].startswith(a.prefix)]
        pods = [p for p in kube.get_json("pods") if p["metadata"]["name"].startswith(a.prefix)]
    except n.KubectlError as e:
        if e.kind == "auth":
            print("AUTH EXPIRED: kubectl needs a fresh login (run any kubectl command in a terminal). "
                  "Running Jobs are unaffected; schedulers wait.")
            return 4
        print(f"kubectl failed: {str(e)[:300]}")
        return 1
    bad = n.load_bad_nodes(a.bad_nodes)
    waves(state_dirs)
    section("jobs in cluster")
    print(f"  {dict(collections.Counter(n.job_status(j) for j in jobs))}  "
          f"(finished Jobs expire after the cluster TTL, ~24 h; trackers keep the record)")
    violations(kube.namespace, a.prefix, jobs)
    failures(kube, kube.namespace, a.prefix, pods, since, bad, not a.no_logs)
    exposed_jobs(pods, bad)
    pending(pods)
    idle(kube.namespace, a.prefix, a.idle_min)
    if a.gate_probe:
        gate_probe(kube, a.gate_probe, a.prefix)
    return 0


if __name__ == "__main__":
    sys.exit(main())
