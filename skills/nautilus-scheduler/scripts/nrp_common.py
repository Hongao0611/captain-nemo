"""Shared helpers for the Nautilus (NRP) job tools: kubectl, Prometheus, the NRP
usage policy, bad-node bookkeeping and manifest patching.

Policy numbers and metric choices are calibrated against the NRP portal's
"Utilization violations" page (see reference.md, "How NRP scores pods").
"""
import datetime as dt
import fcntl
import json
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request

PROM_URL = os.environ.get("NRP_PROM_URL", "https://prometheus.nrp-nautilus.io")
BAD_NODES_FILE = os.path.expanduser(os.environ.get("NRP_BAD_NODES", "~/.nrp/bad_nodes.txt"))
LAUNCH_LOCK = os.path.expanduser("~/.nrp/launch.lock")

# NRP fair-use policy (nrp.ai/documentation/userdocs/start/policies, portal
# Violations page). A pod violates when any judged resource is outside its band,
# measured against REQUESTS. New pods are refused while MORE than 4 pods violate.
GPU_MIN_PCT = 40.0
CPU_BAND = (20.0, 200.0)
MEM_BAND = (20.0, 150.0)
CPU_EXEMPT_CORES = 1.0      # CPU is not judged when <= 1 core is requested
MEM_EXEMPT_BYTES = 2e9      # memory is not judged when <= 2 GB is requested
GATE_MAX_VIOLATORS = 4
GATE_DENIED_MARKER = "utilization is too low"

# How the portal measures (calibrated 2026-09-29 on 7 pods, mean abs error):
# GPU = the pod's LIFETIME average (0.5 pt) -- so a pod whose setup idles the GPU
# stays a violator until the running average climbs past 40%; CPU = recent
# ~15-min rate (1.8 pt); memory = RSS, not the page-cache-inflated working set
# (~5 pt; working set was off by > 100 pt).
GPU_WINDOW = "7d"           # longer than any pod: avg_over_time == lifetime average
CPU_WINDOW = "15m"
MEM_WINDOW = "30m"
# A GPU pod without GPU samples counts as a "presumed" violator only while it is
# probably about to start scoring 0%: Pending < 10 min, or Running < 15 min.
# Longer Pending = waiting for capacity (not scored); longer Running without
# samples = node without a DCGM exporter, or a node that stopped reporting.
PENDING_GRACE_S = 600
STARTUP_GRACE_S = 900


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def ts():
    return utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"{ts()} {msg}", flush=True)


# ----------------------------------------------------------------- quantities
_SUFFIX = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50,
           "k": 1e3, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "m": 1e-3}


def parse_quantity(q):
    """Kubernetes quantity -> float ("500m" -> 0.5, "6Gi" -> 6442450944)."""
    if q is None:
        return 0.0
    s = str(q).strip()
    m = re.fullmatch(r"([0-9.eE+-]+)([A-Za-z]*)", s)
    if not m:
        raise ValueError(f"bad quantity {q!r}")
    num, suf = m.groups()
    return float(num) * (_SUFFIX[suf] if suf else 1.0)


def fmt_bytes(b):
    return f"{b / 2**30:.1f}Gi" if b >= 2**30 else f"{b / 2**20:.0f}Mi"


# -------------------------------------------------------------------- kubectl
class KubectlError(Exception):
    def __init__(self, msg, kind):
        super().__init__(msg)
        self.kind = kind


# The login (OIDC) server itself down: credentials are fine, a fresh login would not
# help -- wait. Seen 2026-09-30: "get-token: authentication error: oidc error: oidc
# discovery error: 503 Service Unavailable ... No server is available" for ~1 h.
_LOGIN_DOWN = ("oidc discovery error", "No server is available to handle this request")
_AUTH = ("Unauthorized", "You must be logged in", "provide credentials", "oidc",
         "refresh token", "token has expired", "id-token")
_TRANSIENT = ("http2: client connection lost", "connection reset", "i/o timeout",
              "TLS handshake timeout", "EOF", "ServiceUnavailable", "InternalError",
              "Internal error", "etcdserver", "context deadline exceeded",
              "Too Many Requests", "timed out", "connection refused", "no route to host",
              "Service Unavailable", "the server is currently unable")
_EXISTS = ("AlreadyExists", "already exists", "field is immutable")
_PERMANENT = ("is invalid", "Invalid value", "error validating", "unknown field",
              "Forbidden", "admission webhook", "denied the request")


def classify_error(msg):
    """Kubectl stderr -> gate | quota | login_down | auth | exists | transient | permanent."""
    if GATE_DENIED_MARKER in msg:
        return "gate"          # before 'permanent': the gate speaks via an admission webhook
    if "exceeded quota" in msg:
        return "quota"         # a Forbidden that clears when our own pods finish
    if any(m in msg for m in _LOGIN_DOWN) or (
            "get-token" in msg and re.search(r"\b50[0-4]\b", msg)):
        return "login_down"    # before 'auth': these messages also mention oidc
    if any(m in msg for m in _AUTH):
        return "auth"
    if any(m in msg for m in _EXISTS):
        return "exists"
    if any(m in msg for m in _TRANSIENT):
        return "transient"
    if any(m in msg for m in _PERMANENT):
        return "permanent"
    return "transient"         # unknown: retrying is cheaper than parking a good job


class Kube:
    def __init__(self, namespace=None, context=None, dry_run=False):
        self.base = ["kubectl"] + (["--context", context] if context else []) \
            + (["-n", namespace] if namespace else [])
        self.namespace = namespace or self._current_namespace(context)
        self.dry_run = dry_run

    @staticmethod
    def _current_namespace(context):
        cmd = ["kubectl", "config", "view", "--minify", "-o", "jsonpath={..namespace}"]
        if context:
            cmd[1:1] = ["--context", context]
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
        except (OSError, subprocess.TimeoutExpired):
            out = ""
        return out or "default"

    def run(self, *args, stdin=None, timeout=120):
        try:
            r = subprocess.run(self.base + list(args), input=stdin, capture_output=True,
                               text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise KubectlError(f"kubectl {' '.join(args[:2])} timed out after {timeout}s", "transient")
        if r.returncode != 0:
            msg = (r.stderr or r.stdout).strip()
            raise KubectlError(msg, classify_error(msg))
        return r.stdout

    def get_json(self, kind, timeout=180):
        return json.loads(self.run("get", kind, "-o", "json", timeout=timeout)).get("items", [])

    def create(self, manifest_yaml, server_dry_run=False):
        args = ["create", "-f", "-"] + (["--dry-run=server"] if server_dry_run else [])
        if self.dry_run and not server_dry_run:
            return "(dry-run) not created"
        return self.run(*args, stdin=manifest_yaml)

    def delete_job(self, name):
        if self.dry_run:
            return "(dry-run) not deleted"
        return self.run("delete", "job", name, "--wait=true", "--timeout=120s", timeout=180)

    def logs(self, target, tail=None, timeout=60):
        args = ["logs", target] + ([f"--tail={tail}"] if tail else [])
        try:
            return self.run(*args, timeout=timeout)
        except KubectlError:
            return ""


def job_status(job):
    """Job json -> RUNNING | SUCCEEDED | FAILED.

    Kubernetes >= 1.31 sets SuccessCriteriaMet / FailureTarget as soon as the outcome
    is decided, and Complete / Failed only once every pod has terminated. A pod
    stranded Terminating on a vanished node blocks the latter forever (seen
    2026-09-30: a finished Job held its slot 23 h), so trust the earlier conditions."""
    for c in job.get("status", {}).get("conditions") or []:
        if c.get("status") == "True":
            if c.get("type") in ("Complete", "SuccessCriteriaMet"):
                return "SUCCEEDED"
            if c.get("type") in ("Failed", "FailureTarget"):
                return "FAILED"
    return "RUNNING"


# ----------------------------------------------------------------- Prometheus
def prom(expr, at=None, timeout=90, retries=3):
    """Instant query -> list of (labels, value)."""
    params = {"query": expr}
    if at is not None:
        params["time"] = str(at)
    url = f"{PROM_URL}/api/v1/query?{urllib.parse.urlencode(params)}"
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                data = json.load(r)
            if data.get("status") != "success":
                raise RuntimeError(data.get("error", "query failed"))
            return [(x["metric"], float(x["value"][1])) for x in data["data"]["result"]]
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(5 * (attempt + 1))


def prom_range(expr, start, end, step, timeout=180):
    """Range query -> list of (labels, [(t, value), ...])."""
    params = {"query": expr, "start": str(start), "end": str(end), "step": str(step)}
    url = f"{PROM_URL}/api/v1/query_range?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=timeout) as r:
        data = json.load(r)
    if data.get("status") != "success":
        raise RuntimeError(data.get("error", "query failed"))
    return [(x["metric"], [(float(t), float(v)) for t, v in x["values"]]) for x in data["data"]["result"]]


def _by_pod(rows, key="pod"):
    return {m.get(key): v for m, v in rows if m.get(key)}


def prom_prefix_re(prefix):
    """Prefix -> RE2 pattern safe inside a PromQL double-quoted string (Python's
    re.escape emits '\\-', an invalid escape there -> HTTP 400)."""
    return re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\\\1", prefix) + ".*"


def pod_usage(namespace, prefix, at=None):
    """NRP-style usage per pod (Prometheus only, so it also works for a past `at`).

    Returns {pod: dict(phase, node, start, req_cpu, req_mem, req_gpu, gpu_pct,
    cpu_cores, rss)}; usage fields are None when no samples exist yet."""
    sel = f'namespace="{namespace}",pod=~"{prom_prefix_re(prefix)}"'
    csel = sel + ',container!="",container!="POD"'
    phase = {m["pod"]: m["phase"] for m, v in prom(f"kube_pod_status_phase{{{sel}}} == 1", at)}
    node = {m["pod"]: m.get("node") for m, v in prom(f"kube_pod_info{{{sel}}}", at)}
    start = _by_pod(prom(f"kube_pod_start_time{{{sel}}}", at))
    created = _by_pod(prom(f"kube_pod_created{{{sel}}}", at))
    deleting = set(_by_pod(prom(f"kube_pod_deletion_timestamp{{{sel}}}", at)))
    req = {}
    for m, v in prom(f"sum by (pod, resource) (kube_pod_container_resource_requests{{{sel}}})", at):
        req.setdefault(m["pod"], {})[m["resource"]] = v
    # "and <current series>": only pods still reporting -- a pod on a dead node
    # keeps its old lifetime average forever otherwise.
    dcgm = f"DCGM_FI_DEV_GPU_UTIL{{{sel}}}"
    gpu = _by_pod(prom(f"avg by (pod) (avg_over_time({dcgm}[{GPU_WINDOW}]) and {dcgm})", at))
    cpu = _by_pod(prom(f"sum by (pod) (rate(container_cpu_usage_seconds_total{{{csel}}}[{CPU_WINDOW}]))", at))
    rss = _by_pod(prom(f"sum by (pod) (avg_over_time(container_memory_rss{{{csel}}}[{MEM_WINDOW}]))", at))
    out = {}
    for p, ph in phase.items():
        r = req.get(p, {})
        out[p] = dict(phase=ph, node=node.get(p), start=start.get(p), created=created.get(p),
                      deleting=p in deleting,
                      req_cpu=r.get("cpu", 0.0), req_mem=r.get("memory", 0.0),
                      req_gpu=r.get("nvidia_com_gpu", 0.0),
                      gpu_pct=gpu.get(p), cpu_cores=cpu.get(p), rss=rss.get(p))
    return out


def judge(u, now=None):
    """NRP verdict for one pod's usage dict -> (flags, presumed).

    flags: list of human-readable band violations. presumed: True when the pod
    requests a GPU, has no GPU samples yet and is about to start scoring (see
    PENDING_GRACE_S / STARTUP_GRACE_S) -- count it as a violator in advance."""
    now = time.time() if now is None else now
    flags = []
    presumed = False
    if u["req_gpu"] > 0:
        if u["gpu_pct"] is None:
            if u["phase"] == "Pending":
                presumed = u["created"] is not None and now - u["created"] < PENDING_GRACE_S
            else:
                ref = u["start"] or u["created"]
                presumed = ref is not None and now - ref < STARTUP_GRACE_S
        elif u["gpu_pct"] < GPU_MIN_PCT:
            flags.append(f"GPU {int(u['gpu_pct'])}%")   # truncate: 39.9 must not read as 40
    if u["req_cpu"] > CPU_EXEMPT_CORES and u["cpu_cores"] is not None:
        pct = 100 * u["cpu_cores"] / u["req_cpu"]
        if not CPU_BAND[0] <= pct <= CPU_BAND[1]:
            flags.append(f"CPU {pct:.0f}%")
    if u["req_mem"] > MEM_EXEMPT_BYTES and u["rss"] is not None:
        pct = 100 * u["rss"] / u["req_mem"]
        if not MEM_BAND[0] <= pct <= MEM_BAND[1]:
            flags.append(f"MEM {pct:.0f}%")
    return flags, presumed


def count_violators(namespace, prefix, jobs=None):
    """Violators the gate would count now + pods about to become one.

    `jobs`: optional kubectl job list; active Jobs of ours created in the last
    5 min with no pod in Prometheus yet are presumed violators too (covers the
    seconds-to-minutes before a new pod shows up in kube-state-metrics)."""
    usage = pod_usage(namespace, prefix)
    live = {p: u for p, u in usage.items() if u["phase"] in ("Running", "Pending") and not u["deleting"]}
    real = presumed = 0
    for u in live.values():
        flags, pre = judge(u)
        real += bool(flags)
        presumed += pre and not flags
    if jobs:
        seen_jobs = {p.rsplit("-", 1)[0] for p in usage}
        now = utcnow()
        for j in jobs:
            name = j["metadata"]["name"]
            if not name.startswith(prefix) or job_status(j) != "RUNNING" or name in seen_jobs:
                continue
            created = dt.datetime.fromisoformat(j["metadata"]["creationTimestamp"].replace("Z", "+00:00"))
            if (now - created).total_seconds() < 300:
                presumed += 1
    return real, presumed, live


# ------------------------------------------------------------------ bad nodes
def load_bad_nodes(path=BAD_NODES_FILE):
    """{hostname: note}. File format: one hostname per line, '# comment' allowed."""
    nodes = {}
    if not os.path.exists(path):
        return nodes
    for line in open(path):
        body, _, note = line.partition("#")
        host = body.strip()
        if host:
            nodes[host] = note.strip()
    return nodes


def add_bad_node(host, reason, path=BAD_NODES_FILE):
    nodes = load_bad_nodes(path)
    if host in nodes:
        return False
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        f.write(f"{host}  # {utcnow():%Y-%m-%d} {reason}\n")
    return True


# ---------------------------------------------------------- manifest patching
def exclude_nodes(job, nodes):
    """Add `hostname NotIn nodes` to every nodeSelectorTerm (terms are ORed, so
    one term without the exclusion would let the pod land on a bad node)."""
    if not nodes:
        return job
    pod = job["spec"]["template"]["spec"]
    req = (pod.setdefault("affinity", {}).setdefault("nodeAffinity", {})
           .setdefault("requiredDuringSchedulingIgnoredDuringExecution", {}))
    terms = req.setdefault("nodeSelectorTerms", [])
    if not terms:
        terms.append({"matchExpressions": []})
    for term in terms:
        exprs = term.setdefault("matchExpressions", [])
        ex = next((e for e in exprs if e.get("key") == "kubernetes.io/hostname"
                   and e.get("operator") == "NotIn"), None)
        if ex is None:
            exprs.append({"key": "kubernetes.io/hostname", "operator": "NotIn",
                          "values": sorted(nodes)})
        else:
            ex["values"] = sorted(set(ex.get("values") or []) | set(nodes))
    return job


def add_node_name_env(job):
    """Expose the node name as $NODE_NAME so failure messages can name the node."""
    for c in job["spec"]["template"]["spec"].get("containers", []):
        env = c.setdefault("env", [])
        if not any(e.get("name") == "NODE_NAME" for e in env):
            env.append({"name": "NODE_NAME",
                        "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}})
    return job


class LaunchLock:
    """Host-wide lock around 'count violators -> launch', so two schedulers on
    this machine never spend the same violation budget twice."""

    def __enter__(self):
        os.makedirs(os.path.dirname(LAUNCH_LOCK), exist_ok=True)
        self.f = open(LAUNCH_LOCK, "w")
        fcntl.flock(self.f, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        fcntl.flock(self.f, fcntl.LOCK_UN)
        self.f.close()
