#!/usr/bin/env python3
"""Lint (and optionally harden / right-size) a Job manifest before scheduling it on Nautilus.

  nrp_preflight.py jobs.yaml --prefix me-                   # lint only
  nrp_preflight.py jobs.yaml --prefix me- --harden out.yaml # + write a hardened copy
  nrp_preflight.py jobs.yaml --prefix me- --usage-from me-train-   # + requests from measured usage
  nrp_preflight.py jobs.yaml --prefix me- --server-dry-run  # + API validation and gate check

Read-only except for --harden OUT (a new file). --server-dry-run creates nothing.
"""
import argparse
import collections
import copy
import math
import re
import statistics
import sys
import time

import yaml

import nrp_common as n

# Setup steps that can hang on a bad node or mirror, holding a GPU at 0%:
# (what, regex, timeout seconds for --harden)
SLOW_STEPS = [
    ("apt-get", r"^\s*(sudo\s+)?apt(-get)?\s+(update|install)", 900),
    ("pip install", r"^\s*(python3?\s+-m\s+)?pip3?\s+install", 1800),
    ("conda install", r"^\s*(conda|mamba|micromamba)\s+(install|create|env)", 1800),
    ("git", r"^\s*git\s+(clone|fetch|pull)", 600),
    ("hf login", r"^\s*(hf\s+auth\s+login|huggingface-cli\s+login)", 300),
    ("hf download", r"^\s*(hf|huggingface-cli)\s+download", 1800),
    ("download", r"^\s*(wget|curl)\s", 900),
    ("PVC copy", r"^\s*(cp|rsync|tar)\s.*(/mnt/|/pvc)", 1800),
]
GPU_WORK = r"^\s*(python3?|torchrun|accelerate|deepspeed|bash\s+\S+\.sh)\b"


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("manifest")
    ap.add_argument("--prefix", required=True, help="your job-name prefix, e.g. 'me-'")
    ap.add_argument("--bad-nodes", default=n.BAD_NODES_FILE)
    ap.add_argument("--harden", metavar="OUT", help="write a hardened copy of the manifest here")
    ap.add_argument("--usage-from", metavar="POD_PREFIX",
                    help="measure past pods with this name prefix (Prometheus) and recommend requests")
    ap.add_argument("--days", type=float, default=7.0,
                    help="history window for --usage-from; must cover whole pod lifetimes, or peaks are missed")
    ap.add_argument("--namespace", default=None)
    ap.add_argument("--context", default=None)
    ap.add_argument("--server-dry-run", action="store_true",
                    help="send the first Job (renamed) through the API server with --dry-run=server")
    return ap.parse_args()


def script_of(container):
    """The shell script of a `bash -c` / `sh -c` container, else None."""
    cmd = list(container.get("command") or []) + list(container.get("args") or [])
    for i, tok in enumerate(cmd[:-1]):
        if tok in ("-c", "-lc", "-ec") and re.search(r"(^|/)(ba)?sh$", str(cmd[max(0, i - 1)])):
            return str(cmd[i + 1])
    return None


def slow_lines(script):
    """Unbounded slow setup lines: [(line_no, what, line, timeout)]."""
    out = []
    for i, line in enumerate(script.splitlines()):
        if re.match(r"^\s*timeout\s", line) or line.rstrip().endswith("\\"):
            continue
        for what, rx, t in SLOW_STEPS:
            if re.search(rx, line):
                out.append((i, what, line, t))
                break
    return out


def wrap_timeouts(script, lines):
    """Bound each flagged line with `timeout` (skips lines with single quotes,
    which the sh -c wrapper would break). Returns (script, n_wrapped, skipped)."""
    rows = script.split("\n")
    done, skipped = 0, []
    for i, what, line, t in lines:
        if "'" in line:
            skipped.append(line.strip())
            continue
        indent = re.match(r"^\s*", line).group(0)
        rows[i] = (f"{indent}timeout {t} sh -c '{line.strip()}' || "
                   f"{{ echo \"{what} stalled on node $NODE_NAME\"; exit 1; }}")
        done += 1
    return "\n".join(rows), done, skipped


class Findings:
    def __init__(self):
        self.items = collections.OrderedDict()

    def add(self, level, msg, job):
        self.items.setdefault((level, msg), []).append(job)

    def show(self, total):
        order = {"ERROR": 0, "WARN": 1, "INFO": 2}
        for (level, msg), jobs in sorted(self.items.items(), key=lambda kv: order[kv[0][0]]):
            scope = "all jobs" if len(jobs) == total else f"{len(jobs)} job(s), e.g. {jobs[0]}"
            print(f"  {level:5s} {msg}  [{scope}]")
        return sum(1 for (lvl, _) in self.items if lvl == "ERROR")


def lint(docs, prefix, bad):
    f = Findings()
    names = collections.Counter((d.get("metadata") or {}).get("name") for d in docs)
    for d in docs:
        name = (d.get("metadata") or {}).get("name") or "(unnamed)"
        if d.get("kind") != "Job" or d.get("apiVersion") != "batch/v1":
            f.add("ERROR", f"kind/apiVersion is {d.get('kind')}/{d.get('apiVersion')}, expected Job/batch/v1", name)
            continue
        if not name.startswith(prefix):
            f.add("ERROR", f"name does not start with {prefix!r} (shared namespace)", name)
        if len(name) > 63:
            f.add("ERROR", "name longer than 63 characters (the job-name pod label must fit)", name)
        if names[name] > 1:
            f.add("ERROR", "duplicate job name", name)
        spec = d.get("spec") or {}
        pod = (spec.get("template") or {}).get("spec") or {}
        bl = spec.get("backoffLimit")
        if bl is not None and bl < 3:
            f.add("WARN", f"backoffLimit {bl}: a node that fails pods in seconds burns these retries "
                          "in minutes; use 4-6", name)
        if pod.get("restartPolicy") == "OnFailure":
            f.add("WARN", "restartPolicy OnFailure restarts in place on the same (possibly bad) node; "
                          "Never lets the Job retry elsewhere", name)
        if "ttlSecondsAfterFinished" not in spec:
            f.add("INFO", "no ttlSecondsAfterFinished: the cluster default (24 h) applies -- the "
                          "scheduler's tracker is the lasting record", name)
        missing = set(bad) - excluded_hosts(pod)
        if missing:
            f.add("INFO", f"{len(missing)} bad node(s) not excluded in the manifest (nrp_scheduler.py "
                          "injects them at apply time; --harden bakes them in)", name)
        for c in pod.get("containers") or []:
            lint_container(f, name, c)
    return f


def excluded_hosts(pod):
    terms = (((pod.get("affinity") or {}).get("nodeAffinity") or {})
             .get("requiredDuringSchedulingIgnoredDuringExecution") or {}).get("nodeSelectorTerms") or []
    sets = [{v for e in t.get("matchExpressions") or [] if e.get("key") == "kubernetes.io/hostname"
             and e.get("operator") == "NotIn" for v in e.get("values") or []} for t in terms]
    return set.intersection(*sets) if sets else set()


def lint_container(f, name, c):
    res = c.get("resources") or {}
    req, lim = res.get("requests") or {}, res.get("limits") or {}
    gpu = n.parse_quantity(lim.get("nvidia.com/gpu") or req.get("nvidia.com/gpu") or 0)
    for r in ("cpu", "memory"):
        if r not in req:
            f.add("WARN", f"no {r} request: NRP judges usage against requests", name)
    cpu_r, mem_r = n.parse_quantity(req.get("cpu", 0)), n.parse_quantity(req.get("memory", 0))
    if req.get("cpu") and cpu_r <= n.CPU_EXEMPT_CORES:
        f.add("INFO", f"CPU request {req['cpu']} <= 1 core: CPU usage is not judged", name)
    if req.get("memory") and mem_r <= n.MEM_EXEMPT_BYTES:
        f.add("INFO", f"memory request {req['memory']} <= 2 GB: memory usage is not judged", name)
    for r in ("cpu", "memory"):
        if r in req and r in lim and n.parse_quantity(lim[r]) > 1.2 * n.parse_quantity(req[r]):
            f.add("INFO", f"{r} limit/request > 1.2: NRP's container-must-meet-memory-and-cpu-ratio "
                          "policy warns (not enforced as of 2026-09)", name)
    script = script_of(c)
    if script is None:
        return
    if not re.search(r"^\s*set\s+-[a-z]*e", script, re.M):
        f.add("WARN", "script has no `set -e`: a failed setup step is ignored and the job fails later, "
                      "or succeeds with wrong output", name)
    for _, what, line, _ in slow_lines(script):
        f.add("WARN", f"unbounded {what} step (can hang for hours on a bad node/mirror): "
                      f"`{line.strip()[:70]}`", name)
    if gpu > 0:
        head = script.split("\n")
        first_work = next((i for i, l in enumerate(head) if re.match(GPU_WORK, l)), len(head))
        setup = [w for i, w, _, _ in [(i, w, l, t) for i, w, l, t in slow_lines(script)] if i < first_work]
        setup += [w for i, l in enumerate(head[:first_work]) if re.match(r"^\s*timeout\s", l)
                  for w, rx, _ in SLOW_STEPS if re.search(rx, re.sub(r"^\s*timeout\s+\d+\s+(sh -c ')?", "", l))]
        if setup:
            f.add("INFO", f"{len(setup)} setup step(s) ({', '.join(sorted(set(setup)))}) run while the GPU "
                          "idles: the pod scores 0% GPU until they finish and stays a violator until its "
                          "lifetime average passes 40% -- keep setup short (prebuilt image, cached env)", name)


def harden(docs, bad):
    out, wrapped, skipped = [], 0, []
    for d in docs:
        d = copy.deepcopy(d)
        if d.get("kind") == "Job":
            n.exclude_nodes(d, bad)
            n.add_node_name_env(d)
            for c in d["spec"]["template"]["spec"].get("containers") or []:
                script = script_of(c)
                if script is None:
                    continue
                new, k, sk = wrap_timeouts(script, slow_lines(script))
                wrapped, skipped = wrapped + k, skipped + sk
                if k:
                    replace_script(c, script, new)
        out.append(d)
    return out, wrapped, skipped


def replace_script(c, old, new):
    for key in ("args", "command"):
        seq = c.get(key) or []
        for i, tok in enumerate(seq):
            if tok == old:
                seq[i] = new
                return


# ------------------------------------------------------------- right-sizing
def usage_report(ns, prefix, days):
    sel = f'namespace="{ns}",pod=~"{n.prom_prefix_re(prefix)}"'
    csel = sel + ',container!="",container!="POD"'
    w = f"{int(days * 86400)}s"
    q = lambda e: {m["pod"]: v for m, v in n.prom(e)}
    gpu_life = q(f"avg by (pod) (avg_over_time(DCGM_FI_DEV_GPU_UTIL{{{sel}}}[{w}]))")
    cpu_lo = q(f"quantile_over_time(0.1, sum by (pod) (rate(container_cpu_usage_seconds_total{{{csel}}}[15m]))[{w}:5m])")
    cpu_hi = q(f"quantile_over_time(0.9, sum by (pod) (rate(container_cpu_usage_seconds_total{{{csel}}}[15m]))[{w}:5m])")
    cpu_max = q(f"max_over_time(sum by (pod) (rate(container_cpu_usage_seconds_total{{{csel}}}[15m]))[{w}:5m])")
    rss_avg = q(f"avg_over_time(sum by (pod) (container_memory_rss{{{csel}}})[{w}:5m])")
    rss_max = q(f"max_over_time(sum by (pod) (container_memory_rss{{{csel}}})[{w}:5m])")
    pods = sorted(set(cpu_hi) | set(gpu_life))
    if not pods:
        print(f"  no pods matching {prefix!r} in the last {days} days")
        return
    med = lambda d: statistics.median(d.values()) if d else None
    print(f"  {len(pods)} pods measured over {days} days (per-pod values, median across pods):")
    if gpu_life:
        below = sum(v < n.GPU_MIN_PCT for v in gpu_life.values())
        print(f"    GPU lifetime avg: median {med(gpu_life):.0f}%, {below}/{len(gpu_life)} pods below 40%")
        mat = maturity(sel, days)
        if mat:
            m = statistics.median(mat)
            print(f"    minutes from first GPU sample until the running average passed 40%: median "
                  f"{m:.0f} (n={len(mat)}) -> each new pod counts as a violator that long; with a budget of "
                  f"{n.GATE_MAX_VIOLATORS} the scheduler can start ~{n.GATE_MAX_VIOLATORS} pods per {m:.0f} min "
                  "when all older pods comply")
    ages = [v / 3600 for m, v in n.prom(f"time() - kube_pod_start_time{{{sel}}}")]
    if ages and statistics.median(ages) > days * 24 * 0.8:
        print(f"    ! live pods are {statistics.median(ages):.0f} h old (median) vs a {days * 24:.0f} h window: "
              "phases before the window (and their peaks) are missing -- raise --days")
    print(f"    CPU cores (15-min rate): p10 {med(cpu_lo):.2f}, p90 {med(cpu_hi):.2f}, max {max(cpu_max.values()):.2f}")
    print(f"    RSS: avg {n.fmt_bytes(med(rss_avg))}, max {n.fmt_bytes(max(rss_max.values()))}")
    recommend(med(cpu_lo), med(cpu_hi), max(cpu_max.values()), med(rss_avg), max(rss_max.values()),
              med(gpu_life) if gpu_life else None)


def maturity(sel, days, max_pods=20):
    end = time.time()
    start = end - days * 86400
    step = max(60, int(days * 86400 / 3000))
    out = []
    for m, pts in n.prom_range(f"avg by (pod) (DCGM_FI_DEV_GPU_UTIL{{{sel}}})", start, end, step)[-max_pods:]:
        if pts[0][0] <= start + step:
            continue                      # started before the window: its start is unseen
        total = 0.0
        for k, (t, v) in enumerate(pts, 1):
            total += v
            if total / k >= n.GPU_MIN_PCT:
                out.append((t - pts[0][0]) / 60)
                break
    return out


def recommend(cpu_lo, cpu_hi, cpu_max, rss_avg, rss_max, gpu_med):
    print("  recommended resources:")
    # CPU: judged only above 1 core. If steady use fits, request 1 core and let
    # bursts use the limit; otherwise size so the 15-min rate stays inside 20-200%.
    if cpu_hi <= 1.2:
        cpu_req, why = "1", "exempt from CPU judging (<= 1 core); bursts use the limit"
    else:
        lo, hi = cpu_hi / 1.8, cpu_lo / 0.25
        r = math.sqrt(lo * hi) if lo <= hi else lo
        cpu_req = f"{max(1.5, round(r * 2) / 2):g}"
        why = "keeps p10..p90 inside 25-180%" if lo <= hi else \
            "p10 is too low for any request that also covers p90 -- the quiet phase will violate"
    cpu_lim = f"{max(2, math.ceil(cpu_max * 1.1))}"
    print(f"    cpu: request {cpu_req}, limit {cpu_lim}   ({why})")
    # Memory: judged only when > 2 GB is requested, whatever the usage. Exempting
    # a pod whose RSS runs well above its request invites eviction under node
    # memory pressure, so exempt only small workloads. The limit must clear the
    # peak with room to spare: an OOM kill costs far more than a ratio warning.
    if rss_avg <= 1.5e9:
        mem_req, why = "2G", "exempt from memory judging (<= 2 GB)"
    else:
        r = max(rss_avg / 0.6, rss_max / 1.4)
        mem_req, why = f"{math.ceil(r / 2**30)}Gi", "average RSS ~60% of request, peak < 140%"
    mem_lim = f"{max(math.ceil(rss_max * 2 / 2**30), math.ceil(n.parse_quantity(mem_req) / 2**30))}Gi"
    print(f"    memory: request {mem_req}, limit {mem_lim}   ({why}; limit = 2x the peak RSS seen -- "
          "never lower an existing limit below that)")
    if gpu_med is not None and gpu_med < n.GPU_MIN_PCT:
        print(f"    GPU: median lifetime {gpu_med:.0f}% < 40% -> every concurrent pod of this kind is a "
              "PERMANENT violator. Raise utilization (cache preprocessing, larger batches, move CPU-only "
              "phases off the GPU pod) or keep its concurrency <= 4 minus your other violators.")


def server_dry_run(docs, prefix, ns, context):
    kube = n.Kube(ns, context)
    d = copy.deepcopy(next(x for x in docs if x.get("kind") == "Job"))
    d["metadata"]["name"] = f"{prefix}preflight-{int(time.time()) % 100000}"
    try:
        kube.create(yaml.safe_dump(d), server_dry_run=True)
        print("  OK: the API server accepts the Job and the admission gate is open")
    except n.KubectlError as e:
        what = {"gate": "gate CLOSED (too many violating pods) -- manifest itself may be fine",
                "auth": "kubectl login expired"}.get(e.kind, f"rejected ({e.kind})")
        print(f"  {what}: {str(e)[:300]}")


def main():
    a = parse_args()
    docs = [d for d in yaml.safe_load_all(open(a.manifest)) if d]
    bad = n.load_bad_nodes(a.bad_nodes)
    print(f"preflight {a.manifest}: {len(docs)} document(s), {len(bad)} bad nodes known")
    errors = lint(docs, a.prefix, bad).show(len(docs))
    if a.harden:
        out, k, skipped = harden(docs, bad)
        yaml.safe_dump_all(out, open(a.harden, "w"), sort_keys=False)
        print(f"\nhardened copy -> {a.harden}: bad-node exclusion + NODE_NAME env on every Job, "
              f"{k} slow step(s) wrapped in timeout")
        for line in sorted(set(skipped)):
            print(f"  not wrapped (contains a single quote -- bound it by hand): {line[:100]}")
    if a.usage_from:
        print(f"\nusage of past pods {a.usage_from!r}:")
        usage_report(a.namespace or n.Kube(None, a.context).namespace, a.usage_from, a.days)
    if a.server_dry_run:
        print("\nserver dry run:")
        server_dry_run(docs, a.prefix, a.namespace, a.context)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
