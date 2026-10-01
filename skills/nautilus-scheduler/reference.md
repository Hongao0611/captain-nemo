# Nautilus (NRP) scheduling reference

Evidence behind the rules in SKILL.md. Learned running ~450 GPU training Jobs and
~240 GPU eval Jobs in one lab namespace, 2026-09-23 .. 2026-10-01.

## 1. How NRP scores pods (the fair-use gate)

Policy (nrp.ai/documentation/userdocs/start/policies + the portal's "Utilization
violations" page): a pod violates when a judged resource is outside its band,
**measured against its requests**:

| resource | allowed | not judged when |
|---|---|---|
| GPU | > 40% | (always judged if requested) |
| CPU | 20% - 200% | request <= 1 core |
| memory | 20% - 150% | request <= 2 GB |

**While more than 4 of your pods violate, the admission webhook refuses every
new Job**: `admission webhook "job.nrp-nautilus.io" denied the request: Your pods
resources utilization is too low for account`. Running pods are not touched; you
just cannot replace them. A refusal creates nothing and costs nothing.

How the portal measures (calibrated 2026-09-29 against the Violations page, 7
listed pods out of 53 running; `nrp_common.pod_usage` reproduces the list exactly):

| resource | metric | window | error |
|---|---|---|---|
| GPU | `DCGM_FI_DEV_GPU_UTIL` | **pod lifetime** average | 0.5 pt |
| CPU | `rate(container_cpu_usage_seconds_total)` | ~15 min | 1.8 pt |
| memory | `container_memory_rss` | ~30 min | ~5 pt |

Consequences:
- **Every new GPU pod is a violator at first.** Setup (image pull aside: apt, pip,
  git clone, dataset copy) runs at 0% GPU, and the lifetime average only climbs
  past 40% later. Measured: training pods ~40-45 min, cached eval pods ~38 min
  (`nrp_preflight.py --usage-from <pod-prefix>` measures it for any workload).
- A burst of N launches is admitted (the gate looks at current violators), then
  the account sits at N violators for ~40 min and the gate closes. With healthy
  pods it reopens by itself; the damage is the replacements you cannot launch.
- **Structural violators** -- pods whose lifetime GPU average never reaches 40%
  (e.g. an evaluator that idles the GPU while building requests: uncached lm-eval
  pods averaged 29-38%) -- hold a violation slot for their whole life. More than 4
  of them close the gate indefinitely.
- Memory is scored on RSS. `kubectl top` shows the working set, inflated by page
  cache (training pods: ~18.5 GiB shown vs ~2.3 GB RSS), which once led to 24 Gi
  requests that sat at 8-10% -> every training pod a memory violator -> gate
  closed ~12 h. Size memory from RSS.
- The stats lag: the page and the gate react to what the pods did over the
  windows above, so a fix shows up ~15-60 min later.
- `kubectl create --dry-run=server` goes through admission: it is a free gate probe
  (`nrp_status.py --gate-probe`, `nrp_preflight.py --server-dry-run`).
- Nodes without a DCGM exporter (seen: ry-gpu-16.sdsc.optiputer.net) produce no
  GPU samples; pods there cannot be GPU-scored from Prometheus.

## 2. Concurrency

The ceiling is not a number of pods but the violation budget: at most 4
violators at any moment. Compliant, mature pods cost nothing, so a wave can run
as wide as your `--max-concurrent` as long as

    (violating pods) + (pods still maturing) <= 4

`--pace budget` (default) enforces exactly that before every launch, counting like
the portal: real violators + GPU pods with no samples yet (Pending < 10 min,
Running < 15 min) + Jobs created < 5 min ago with no pod visible yet. Pods Pending
longer (capacity) or Running without samples (DCGM-less node, lost node) are not
counted -- they are not scored, and counting them stalled launches for hours.

Throughput under budget pacing is about 4 new pods per maturation time (~4 per
40 min for training) once older pods comply. That keeps up with a wave that
finishes a few Jobs an hour. After a mass failure it refills slower than greedy,
but never closes the gate on your other waves.

`--pace greedy` fills every free slot at once and relies on the gate-retry loop.
Use it when nothing else of yours is running and a closed gate costs nothing.

Several schedulers on one machine share one budget: they count the same pods,
and a host-wide lock (`~/.nrp/launch.lock`) serializes count-then-launch.

A workload that is a structural violator must be fixed (next section) or capped:
its concurrency plus your other violators must stay <= 4 (we ran uncached evals at 3).

## 3. Requests (right-sizing)

- CPU: if steady use is <= ~1.2 cores, **request 1 core** (not judged) with a
  higher limit for bursts; CPU is compressible, so going above the request only
  throttles at the limit. Otherwise request ~p90/1.8 .. p10/0.25 of the 15-min rate.
- Memory: request **2G** (not judged) only for small workloads (avg RSS <= ~1.5 GB):
  a pod far above its request is first in line for eviction under node memory
  pressure. Otherwise request ~avg RSS / 0.6 and keep the peak under 140%.
- Limits: memory limit >= 2x the peak RSS you have seen over whole pod
  lifetimes (an OOM kill wastes the run; measure with `--days` covering full runs).
- NRP's `container-must-meet-memory-and-cpu-ratio` policy warns when a limit
  exceeds 1.2x the request. As of 2026-09 it only warns (kubectl prints
  `Warning: [container-must-meet-memory-and-cpu-ratio]`); re-check if applies start failing.
- In-place pod resize is forbidden for regular accounts: new requests need new pods.

Measured (2026-09-29): GPT-2 training 1.0-1.07 cores (bursts 4), RSS avg 2.1 GiB,
max 3.0 GiB; lm-eval harness 0.4-0.9 cores, RSS avg 1.0 GiB, max 2.7 GiB.

## 4. Keeping the GPU busy

The GPU is allocated from pod start, so every CPU-only minute is 0% on the
lifetime average:
- Bake dependencies into the image (apt/pip at startup cost ~5-25 min).
- Copy datasets to node-local disk with a timeout; some CephFS mounts hang on
  large reads.
- Do tokenization / preprocessing once (separate CPU Job, cache on the PVC).
- For lm-evaluation-harness, `--cache_requests true` built requests once per pod
  instead of per checkpoint: bit-identical results, 5.7 -> 3.6 min per checkpoint,
  GPU-busy share ~50% -> ~75%.

## 5. Bad nodes

A node that fails pods within seconds looks free, so the scheduler keeps sending
retries there: one bad node can eat a Job's whole backoffLimit in minutes and
dozens of pods an hour ("black hole"). Signatures seen:

| signature | meaning | action |
|---|---|---|
| pod reason `UnexpectedAdmissionError` (e.g. `Allocate failed due to device plugin GetPreferredAllocation rpc failed`) | kubelet could not hand out the GPU | exclude if repeated and nothing runs there since |
| `CUDA error: unspecified launch failure`, `CUBLAS_STATUS_EXECUTION_FAILED`, `CUDA unknown error`, `No CUDA GPUs are available`, `--tf32 requires Ampere` on an Ampere-labelled node | GPU unusable in the container | exclude |
| node's `nvidia.com/gpu.*` labels vanish after a burst of admission errors | GPU stack down on that node | exclude (pods stop landing there anyway, but its label may come back broken) |
| `ContainerStatusUnknown`, pod stuck `Terminating`, no metrics for an hour, `kubectl logs` times out | node lost / unreachable | exclude if it does not come back; the Job retries elsewhere |
| a setup step hangs (dataset `cp`, `hf auth login` to a PVC HF_HOME, apt mirror, `git clone` "unexpected disconnect while reading sideband packet") | CephFS / network trouble on that node | bound every such step with `timeout`, exclude on repeats |
| a training step stalls > 1 h at 100% GPU | GPU/driver fault | exclude |
| `untolerated taint {nautilus.io/issue: N}` in Pending reasons | node reserved or under repair | nothing (not yours to fix) |

Rules:
- Exclude on evidence: >= 2 GPU-type failures, >= 3 node-type failures, or >= 5
  failures of any kind on one node in the window -- **unless pods started after
  the last failure have run healthily for 30+ min** (it recovered; a cluster-wide
  GPU-admission blip hit several nodes at once 2026-09-29 19:40-20:20).
- **Persistent group fault vs site blip.** Hosts of one group (same name prefix /
  site) failing *one after another over hours* is a group problem: ry-gpu-01..16
  lost 6 hosts in ~45 h (15, 04, 01, 09, 10, 02; admission errors, stalled clones),
  each exclusion cost 5-20 burned retries -> after the 3rd faulty host, exclude the
  whole group. Many nodes of one site failing *at the same moment*, 1-3 pods each,
  with `NodeNotReady` events, is a transient site blip (gpu-*.nrp.mghpcc.org,
  2026-09-30 21:30: 11 admission errors over 6 nodes, all Ready minutes later):
  exclude nothing, watch the worst node.
- Evidence that survives pod garbage collection (admission-failed pods never
  start a container and vanish within minutes, so the pod list shows nothing):
  the Job's own `.status.failed` counter (`nrp_status.py` "retry budget burned"),
  `kubectl get events --field-selector type=Warning` (the event's `source.host` is
  the node; kept ~1 h), and Prometheus (`kube_pod_status_phase{phase="Failed"}`,
  `kube_pod_status_reason`, `kube_pod_container_status_*terminated_reason`,
  `kube_pod_info` for the node). A Job at N-1 of N failed attempts on a black hole
  is one bad placement from exhausting.
- A Job's pod template is immutable: an exclusion only reaches Jobs created
  afterwards. Jobs already created can still land on the node; recycle (delete ->
  relaunch) those that fail there and those still Pending (nothing lost).
- Faults hit at admission / GPU init: a pod that is already training on a node
  excluded later usually finishes fine (ry-gpu-02 / -03 / -16 pods ran for hours
  after the group exclusion). Leave it; recycle only if it goes IDLE or fails.
- `nrp_scheduler.py` injects the bad-nodes file at every create, so a new
  exclusion needs no manifest regeneration and no restart.
- Nodes get repaired: retry old entries after a few weeks, or when capacity is
  short. Probe before re-including: a tiny Job pinned to the node
  (`nodeSelector: kubernetes.io/hostname: <node>`, `backoffLimit: 0`,
  `activeDeadlineSeconds`) that does the failing step for real under a timeout --
  e.g. read the actual dataset from the PVC (14 hcc-nrp-shor hosts excluded for a
  CephFS hang read 5.3 GB in ~225 s and were re-included, +111 GPUs). A storage
  probe says nothing about the GPU: one re-included host then failed 5 of 5 GPU
  pods (CUBLAS errors). Request a GPU in the probe and run a matmul when the GPU
  matters, and keep re-included nodes on watch.

## 6. Operations

- **Finished Jobs expire** (cluster default ttlSecondsAfterFinished = 24 h). The
  tracker is the only completion record: never delete or reset it mid-wave. If a
  RUNNING Job vanishes while nobody watched (scheduler down > TTL), it may have
  finished -- `nrp_scheduler.py` marks it UNKNOWN instead of rerunning it.
- Job names must be <= 63 characters and unique; a wave that reuses another
  wave's names needs its own tracker (the tracker is keyed by name).
- The API server drops connections under load (`http2: client connection lost`):
  retry creates; do not park the job.
- kubectl uses OIDC via a browser. Two different failures:
  - **login expired** (`Unauthorized`, `refresh token`): only a human can log in
    (WSL: forward localhost:8000; open the URL by hand if the configured browser
    path is wrong).
  - **login server down** (`get-token: ... oidc discovery error: 503 Service
    Unavailable ... No server is available`, or 500s; ~1 h on 2026-09-30): the
    credentials are fine and a new login would not help -- wait.
  `classify_error` tells them apart (`auth` vs `login_down`). Either way running
  Jobs are unaffected and schedulers wait.
- **Aftermath of a control-plane / login outage.** While it lasted, kubelets lost
  track of containers: 41 pods went `ContainerStatusUnknown` (each one a burned
  retry -- 25 Jobs ended with 2-6 failed attempts) and the CephFS CSI driver logged
  `FailedMount` (`an operation with the given Volume ID ... already exists`,
  `DeadlineExceeded`) on 8 nodes. When it is back: list Jobs close to their
  backoffLimit, and verify the checkpoints of runs that were writing one then
  (one optimizer state was corrupted).
- **A finished Job can stay "running" forever.** Kubernetes >= 1.31 sets
  `SuccessCriteriaMet` when the outcome is decided and `Complete` only after every
  pod has terminated; a pod stranded `Terminating` on a node that vanished blocked
  `Complete` and held a scheduler slot for 23 h. `nrp_common.job_status` accepts
  `SuccessCriteriaMet` / `FailureTarget`. Force-delete such pods
  (`--grace-period=0 --force`) once the node is confirmed gone and the run's
  outputs are confirmed complete.
- The namespace pod quota counts pods that have not terminated -- including ones
  stuck `Terminating` / `Unknown` -- not `Completed` ones. Delete finished Jobs only
  after the tracker recorded them SUCCEEDED (one that vanishes before the
  scheduler polled it is relaunched as "vanished").
- Hugging Face: many pods downloading at once exhaust the account quota (HTTP
  429, `1000 api req / 5 min`); stagger downloads, pre-cache datasets on the PVC,
  run offline (`HF_DATASETS_OFFLINE=1`).
- The namespace and PVC root are shared with the lab: touch only your prefix
  and your own PVC paths.
- Kill processes by PID (`<state>/<stem>.wave.pid`); never `pkill -f` (it also
  matches the shell running it). Never edit a bash script while it runs (bash
  reads it incrementally).
- `kubectl logs --timestamps` prints the local timezone.
- Retries land on other nodes, so Jobs must be resumable and idempotent: resume
  only from a checkpoint with the complete state (weights, optimizer, scheduler,
  RNG), verify it, and delete old resume state only after the new checkpoint is
  verified. Upload only what later stages need. Corruptions seen, each passing the
  check before it: missing weight files; zero-filled files of the right size (node
  lost mid-save: JSON and safetensors headers must parse); and a `torch.save` zip
  with one corrupted member -- `zipfile.is_zipfile` only reads the end record, so
  `torch.load` crash-looped through all 6 retries. Verify with
  `zipfile.ZipFile(p).testzip()` (CRC of every member; ~9 s per GB on CephFS).
  Keeping the previous full state until the new one verified let that run resume
  from the step-8192 checkpoint instead of starting over.
- Bound every setup step in **every** template, not just the main one: an
  evaluation template without timeouts sat 73 min in `apt-get update` on a node
  whose mirror access hung, while the training template (already bounded) failed
  fast and retried elsewhere.
- Long Pending: before blaming capacity, count the nodes your affinity admits
  (GPU compute-capability / CUDA-runtime labels, ephemeral storage, exclusions vs
  taints). 2026-09-30: 75 fitting nodes, all full -> real capacity; widening the
  pool (re-probing old exclusions) helped, changing the spec would not have.

## 7. Prometheus

Public, no auth: `https://prometheus.nrp-nautilus.io/api/v1/query` (or
`thanos.nrp-nautilus.io`). Series carry `namespace` and `pod`; kube-state-metrics
names the GPU request `resource="nvidia_com_gpu"`. PromQL strings need `\\` for a
literal backslash, so do not feed Python's `re.escape` output into a selector
(`nrp_common.prom_prefix_re`).
