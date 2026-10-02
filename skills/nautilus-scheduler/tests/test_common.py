#!/usr/bin/env python3
"""Unit tests for nrp_common's pure functions: python3 tests/test_common.py"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
import nrp_common as n  # noqa: E402

NOW = 1_000_000.0


def pod(**kw):
    u = dict(phase="Running", node="x", start=NOW - 7200, created=NOW - 7300, deleting=False,
             req_cpu=2.0, req_mem=6 * 2**30, req_gpu=1.0, gpu_pct=90.0, cpu_cores=1.0, rss=2.3e9)
    u.update(kw)
    return u


def test_judge():
    assert n.judge(pod(), NOW) == ([], False)
    assert n.judge(pod(gpu_pct=39.9), NOW)[0] == ["GPU 39%"]
    assert n.judge(pod(gpu_pct=40.0), NOW)[0] == []
    assert n.judge(pod(cpu_cores=0.3), NOW)[0] == ["CPU 15%"]            # 0.3 / 2 cores
    assert n.judge(pod(cpu_cores=4.2), NOW)[0] == ["CPU 210%"]
    assert n.judge(pod(req_cpu=1.0, cpu_cores=0.05), NOW)[0] == []       # <= 1 core: CPU not judged
    assert n.judge(pod(rss=0.5e9), NOW)[0] == ["MEM 8%"]
    assert n.judge(pod(req_mem=2e9, rss=0.1e9), NOW)[0] == []            # <= 2 GB: memory not judged
    assert n.judge(pod(req_gpu=0, gpu_pct=None), NOW) == ([], False)     # CPU-only pod
    # presumed: no GPU samples yet
    assert n.judge(pod(phase="Pending", gpu_pct=None, start=None, created=NOW - 60), NOW) == ([], True)
    assert n.judge(pod(phase="Pending", gpu_pct=None, start=None, created=NOW - 3600), NOW) == ([], False)
    assert n.judge(pod(gpu_pct=None, start=NOW - 300), NOW) == ([], True)
    assert n.judge(pod(gpu_pct=None, start=NOW - 7200), NOW) == ([], False)   # no DCGM on the node


def test_classify_error():
    c = n.classify_error
    assert c('admission webhook "job.nrp-nautilus.io" denied the request: Your pods resources '
             'utilization is too low for account') == "gate"
    assert c('jobs.batch "x" is forbidden: exceeded quota: gpu-quota') == "quota"
    assert c("error: You must be logged in to the server (Unauthorized)") == "auth"
    assert c('Error from server (AlreadyExists): jobs.batch "x" already exists') == "exists"
    assert c('The Job "x" is invalid: spec.template: field is immutable') == "exists"
    assert c("error: http2: client connection lost") == "transient"
    assert c('The Job "x" is invalid: metadata.name: Invalid value') == "permanent"
    assert c("something never seen before") == "transient"
    # login server down (2026-09-30): not an expired login
    assert c("error: get-token: authentication error: oidc error: oidc discovery error: 503 Service "
             "Unavailable: <html><body><h1>503 Service Unavailable</h1> No server is available to "
             "handle this request.") == "login_down"
    assert c("error: get-token: authentication error: oidc error: 500 Internal Server Error") == "login_down"
    assert c("error: get-token: authentication error: oidc error: refresh token is expired") == "auth"


def test_job_status():
    js = lambda *conds: n.job_status({"status": {"conditions": [{"type": t, "status": "True"} for t in conds]}})
    assert js() == "RUNNING"
    assert js("Complete") == "SUCCEEDED" and js("Failed") == "FAILED"
    # k8s >= 1.31: decided outcome while a pod is still stranded Terminating
    assert js("SuccessCriteriaMet") == "SUCCEEDED"
    assert js("FailureTarget") == "FAILED"
    assert n.job_status({"status": {"conditions": [{"type": "Complete", "status": "False"}]}}) == "RUNNING"


def test_exclude_nodes():
    job = {"spec": {"template": {"spec": {}}}}
    n.exclude_nodes(job, {"b", "a"})
    terms = job["spec"]["template"]["spec"]["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
    assert terms == [{"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "NotIn", "values": ["a", "b"]}]}]
    # merge into an existing NotIn; add to every ORed term
    job = {"spec": {"template": {"spec": {"affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
        "nodeSelectorTerms": [
            {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "NotIn", "values": ["old"]},
                                  {"key": "nvidia.com/gpu.product", "operator": "In", "values": ["A100"]}]},
            {"matchExpressions": [{"key": "nvidia.com/gpu.product", "operator": "In", "values": ["L40"]}]}]}}}}}}}
    n.exclude_nodes(job, {"new"})
    t0, t1 = job["spec"]["template"]["spec"]["affinity"]["nodeAffinity"][
        "requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
    assert t0["matchExpressions"][0]["values"] == ["new", "old"]
    assert {"key": "kubernetes.io/hostname", "operator": "NotIn", "values": ["new"]} in t1["matchExpressions"]


def test_quantities_and_prefix():
    assert n.parse_quantity("500m") == 0.5
    assert n.parse_quantity("6Gi") == 6 * 2**30
    assert n.parse_quantity("2G") == 2e9
    assert n.parse_quantity(4) == 4.0
    assert n.prom_prefix_re("me-") == "me-.*"
    assert n.prom_prefix_re("a.b") == "a\\\\.b.*"


def test_bad_nodes_file(tmp="/tmp/nrp-test-badnodes.txt"):
    if os.path.exists(tmp):
        os.remove(tmp)
    assert n.add_bad_node("node-1", "GPU fault", tmp)
    assert not n.add_bad_node("node-1", "again", tmp)
    open(tmp, "a").write("# a comment line\n\nnode-2   # manual\n")
    assert set(n.load_bad_nodes(tmp)) == {"node-1", "node-2"}
    os.remove(tmp)



def test_init_containers_get_node_name():
    job = {"spec": {"template": {"spec": {"initContainers": [{"name": "train"}], "containers": [{"name": "eval"}]}}}}
    n.add_node_name_env(job)
    spec = job["spec"]["template"]["spec"]
    assert [c["name"] for c in n.all_containers(spec)] == ["train", "eval"]
    assert all(any(e["name"] == "NODE_NAME" for e in c["env"]) for c in n.all_containers(spec))


if __name__ == "__main__":
    fails = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            try:
                f()
                print("PASS", k)
            except AssertionError as e:
                fails += 1
                print("FAIL", k, repr(e))
    sys.exit(1 if fails else 0)
