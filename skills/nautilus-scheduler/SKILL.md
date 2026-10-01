---
name: nautilus-scheduler
description: Schedule, monitor and repair waves of Kubernetes Jobs on the Nautilus / NRP cluster (nrp-nautilus.io) at the highest concurrency NRP's fair-use gate allows. Use when launching a multi-document Job YAML on Nautilus, running the periodic health check of such a wave, choosing concurrency or CPU/memory requests, handling NRP "utilization violations" or the admission denial "pods resources utilization is too low", or when pods fail on bad nodes (UnexpectedAdmissionError, CUDA errors, hung dataset copy / git clone / pip install, pods stuck Pending or Terminating).
---

# Nautilus job scheduler

Tools in `scripts/` (Python 3 + PyYAML, `kubectl` logged in to the cluster):

| script | does | touches the cluster? |
|---|---|---|
| `nrp_preflight.py` | lint a manifest; `--harden OUT` writes a copy with bad-node exclusion, `$NODE_NAME` and timeouts on slow setup steps; `--usage-from PREFIX` recommends requests from measured usage; `--server-dry-run` validates + probes the gate | read-only |
| `run_wave.sh` + `nrp_scheduler.py` | keep a wave running at max concurrency within NRP's violation budget, exclude bad nodes, retry failures, track completion | creates / deletes **your** Jobs |
| `nrp_status.py` | the periodic check: waves, violations + launch budget, failures by node with exclusion advice, exposed Jobs, Pending reasons, idle pods, gate | read-only |

`SKILL_DIR` below means the directory this SKILL.md is in (e.g. `~/.claude/skills/nautilus-scheduler` for a manual install, or the plugin install location).
Read `reference.md` before changing concurrency, requests or exclusion rules: it
holds how NRP scores pods (calibrated), the failure signatures and the sizing math.

## The rules that matter

1. **NRP refuses new pods while more than 4 of yours violate** (GPU < 40% lifetime
   average; CPU outside 20-200% of request unless <= 1 core requested; RSS outside
   20-150% unless <= 2 GB requested). Every new GPU pod violates until setup ends
   and its average passes 40% (~40 min for training). So the true limit on
   concurrency is the violation budget, not a pod count. `--pace budget` handles it.
2. **The tracker (`<state>/<stem>.tracker.json`) is the only completion record.**
   Finished Jobs expire from the cluster after ~24 h. Never delete or reset it.
3. **Touch only Jobs with your prefix** (shared namespace, shared PVC root).
4. **A Job's pod template is immutable.** New node exclusions reach only Jobs
   created later. Recycle (delete -> the scheduler relaunches) older Jobs that fail
   on a bad node or are still Pending (free: nothing ran). Leave a pod that already
   runs healthily on a node excluded after it started -- faults hit at admission /
   GPU init, and recycling it throws its progress away.
5. Stop processes by PID from the `.pid` files, never `pkill -f`. Deleting Jobs
   that hold real work, PVC data or Hub repos needs the user's OK.

## Workflow

### 1. Preflight (before the first launch, and after changing the generator)

```bash
python3 $SKILL_DIR/scripts/nrp_preflight.py jobs.yaml --prefix me- [--usage-from me-train-] [--server-dry-run]
```
Fix every ERROR and WARN **in the script that generates the manifest**, not in
the YAML: unbounded setup steps (wrap in `timeout N ... || { echo "<step> stalled on
node $NODE_NAME"; exit 1; }`), `backoffLimit` < 4, `restartPolicy: OnFailure`,
missing `set -e`, requests. `--harden out.yaml` shows the intended result. With
`--usage-from`, pass `--days` longer than one run, or peaks are missed. If the
workload's median lifetime GPU average is < 40% it is a structural violator: fix
utilization first (reference.md section 4) or cap its concurrency.

### 2. Launch

```bash
setsid nohup bash $SKILL_DIR/scripts/run_wave.sh .nrp/<stem>.log \
  --manifest jobs.yaml --prefix me- --max-concurrent 50 \
  > /dev/null 2>&1 < /dev/null &
```
Defaults: `--pace budget`, `--retries 2` (relaunches of a Failed Job, on top of its
own backoffLimit), `--state-dir .nrp`, `--bad-nodes ~/.nrp/bad_nodes.txt`, poll 5 min.
Add `--on-complete "<cmd>"` for a follow-up step (e.g. a gather Job). Run
`--dry-run --once` first to see what it would do. One wrapper per manifest; the
wrapper refuses to start twice. Waves die with the machine: after a reboot,
relaunch with the same command (trackers survive; running Jobs are adopted).

### 3. Periodic check (hourly while a wave runs)

```bash
python3 $SKILL_DIR/scripts/nrp_status.py --prefix me- --since <last check, UTC ISO> [--gate-probe jobs.yaml]
```
Act on what it prints:

| output | action |
|---|---|
| `AUTH EXPIRED` (exit 4) | ask the user to log in (browser OIDC); nothing else can fix it |
| `LOGIN SERVER DOWN` (exit 5) | NRP's OIDC server returns 5xx; credentials are fine -- wait and re-check hourly, do not ask the user to log in. Afterwards re-check retry budgets and checkpoints (reference.md section 6) |
| wave `NOT RUNNING` with jobs left | relaunch it (step 2, same arguments) |
| `EXCLUDE <node>` | append `<node>  # <date> <evidence>` to `~/.nrp/bad_nodes.txt` (live at the next launch) |
| `recovered? <node>` / `watch <node>` | nothing yet; exclude if it repeats |
| several hosts of one group / site failing one after another | exclude the whole group (reference.md section 5) |
| `Pending ON EXCLUDED` | delete that Job (it never started); the scheduler relaunches it with current exclusions |
| `Running ON EXCLUDED` | leave it if its log advances and its GPU is busy; delete the Job if it is IDLE or failing |
| `N/M attempts failed` (retry budget burned) | a black-hole node, even if no failed pod is left to see: find it in `kubectl get events --field-selector type=Warning` (kept ~1 h), exclude, recycle the Jobs still Pending |
| stuck `Terminating` / `Unknown` pods | node gone or lost: confirm, then `kubectl delete pod --grace-period=0 --force` (they hold pod quota and can keep a finished Job open) |
| `VIOLATING` on pods older than ~1 h | structural: fix the workload, or lower that wave's `<stem>.max` |
| `IDLE` / `NO SAMPLES` | read the pod's log; delete the Job if it is hung or its node is gone |
| many `Pending` for capacity | nothing (or widen the GPU types the manifest accepts) |
| `budget allows 0` for hours with a long queue | find the violators above; do not raise the budget |

### 4. Adjust a running wave (no restart needed)

| goal | how |
|---|---|
| change concurrency | `echo 30 > .nrp/<stem>.max` |
| stop launching (running Jobs continue) | `touch .nrp/<stem>.pause` (remove to resume) |
| rerun jobs / retry FAILED, INVALID, UNKNOWN | job names, one per line, in `.nrp/<stem>.requeue` |
| add or change jobs | regenerate the manifest file; it is reloaded when it changes |
| exclude a node | edit `~/.nrp/bad_nodes.txt` |
| stop the wave | `kill "$(cat .nrp/<stem>.wave.pid)"` (Jobs keep running in the cluster) |
| pick up new scheduler code | `kill "$(cat .nrp/<stem>.scheduler.pid)"` (respawns in 60 s) |
| change flags | stop the wave, relaunch with new flags |

### 5. Finish

`run_wave.sh` stops when the scheduler exits 0 (all SUCCEEDED) or 3 (done, some
FAILED / INVALID / UNKNOWN). For 3: read `.nrp/failed_logs/`, fix the cause,
requeue. Then remove leftovers only after confirming with the user.

Cleaning up while a wave runs: delete a finished Job only after the tracker has
recorded it SUCCEEDED -- a Job that vanishes before the scheduler polled it is
relaunched as "vanished". Completed pods do not count against the namespace pod
quota; pods stuck Terminating / Unknown do.

## Scheduler behaviour worth knowing

- Adopts Jobs already in the cluster; re-creates a Job that vanished while it was
  watching (deleted as hung / on a bad node); marks UNKNOWN (no rerun) a Job that
  vanished while no scheduler watched, since it may have finished and expired.
- Gate or quota refusal: waits `--gate-retry` (15 min), then tries again.
  Transient API errors: retried. Invalid manifests: INVALID. Expired login: waits,
  logs `[AUTH]` once.
- Prometheus unreachable in budget mode: launches nothing that pass (safe side).
- Old flat trackers (`{name: "STATUS"}`) load fine: APPLY_FAILED / FAILED become PENDING.

## Tests

`python3 $SKILL_DIR/tests/test_scheduler.py` (13 scenarios against a simulated
cluster, ~2 min) and `python3 $SKILL_DIR/tests/test_common.py`. Run both after
changing the scripts; neither touches the real cluster.
