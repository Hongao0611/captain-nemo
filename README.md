# captain-nemo

Run large batches of Kubernetes Jobs on the [Nautilus / NRP](https://nrp.ai)
cluster at the highest concurrency its fair-use rules allow, and keep them
healthy while you are away. It is a set of plain Python / bash tools plus
written instructions (`SKILL.md`) that any code agent -- Claude Code, Codex,
Cursor, Gemini CLI, Copilot, Aider, ... -- can follow. You can also run the
tools yourself, without an agent.

## Why you need it

- **NRP refuses new pods while more than 4 of your pods "violate"** its usage
  bands: GPU under 40%, or CPU / memory far from what you requested. Every new
  GPU pod counts as a violator until its setup is done, so launching a big batch
  at once locks you out for an hour or more. captain-nemo counts violators the
  way NRP's Violations page does and launches only while there is room.
- **Bad nodes eat retries.** A node with a broken GPU kills pods within seconds,
  looks free, and swallows retry after retry. captain-nemo keeps your Jobs off
  nodes listed in one file and tells you which nodes to add.
- **Hourly health report**: violators, failures grouped by node (with "exclude /
  watch / recovered" advice), Jobs burning their retries, hung or stuck pods,
  Pending reasons, and whether NRP accepts new pods right now.
- **Manifest preflight**: flags setup steps that can hang (apt, pip, git,
  dataset copies) and too few retries, writes a hardened copy, and recommends
  CPU / memory requests from your pods' measured usage.

`skills/nautilus-scheduler/reference.md` holds the lessons behind these rules:
how NRP scores pods, what bad nodes look like in logs, how to size requests.

## What you need

| need | check it with |
|---|---|
| Linux or WSL (macOS untested: no `setsid`; use `nohup ... &`) | |
| Python 3.8+ with PyYAML (the only dependency) | `python3 -c "import yaml"` (if it fails: `pip install pyyaml`) |
| `kubectl` set up for Nautilus (config from the NRP portal) and logged in | `kubectl get pods` lists your namespace without asking you to log in |
| Your Jobs in one multi-document YAML file of `batch/v1` Jobs | |
| A job-name prefix only you use, e.g. `alice-` | every `metadata.name` in the YAML starts with it |
| HTTPS access to `prometheus.nrp-nautilus.io` (public, no login) | `curl -s https://prometheus.nrp-nautilus.io/-/healthy` |

The tools use your kubectl context's namespace (override with `--namespace`).
They refuse to touch any Job without your prefix, because a lab namespace is
shared.

## Install

```bash
git clone https://github.com/Hongao0611/captain-nemo.git ~/captain-nemo
```

Then connect it to your agent:

- **Claude Code** -- install it as a plugin (inside Claude Code):
  ```
  /plugin marketplace add Hongao0611/captain-nemo
  /plugin install captain-nemo@captain-nemo
  ```
  or copy the skill: `cp -r ~/captain-nemo/skills/nautilus-scheduler ~/.claude/skills/`.
  Claude loads it by itself when you talk about Nautilus jobs.
- **Any other agent** -- paste this into the instructions file your agent reads
  in your project (`AGENTS.md` for Codex and others, `GEMINI.md` for Gemini CLI,
  `.cursor/rules/` for Cursor, `.github/copilot-instructions.md` for Copilot,
  `CONVENTIONS.md` loaded with `--read` for Aider):
  ```markdown
  ## Nautilus (NRP) jobs
  For anything about launching, monitoring or repairing Kubernetes Jobs on
  Nautilus / NRP, first read ~/captain-nemo/skills/nautilus-scheduler/SKILL.md
  and follow it. SKILL_DIR in it means ~/captain-nemo/skills/nautilus-scheduler.
  Read reference.md in that directory before changing concurrency, requests or
  node exclusions. My job-name prefix is `alice-`.
  ```
  If your agent has no instructions file, start the conversation with that text.
- **No agent** -- use the commands in the quick start below; `SKILL.md` explains
  what to do with each line of the health report.

To update later: `git -C ~/captain-nemo pull` (Claude Code plugin: update it from the `/plugin` menu).

## Quick start

Run these from your project directory: the scheduler keeps its state in `./.nrp/`.
Replace `alice-` with your prefix and `jobs.yaml` with your manifest.

```bash
S=~/captain-nemo/skills/nautilus-scheduler/scripts

# 1. Check the manifest (read-only). Fix every WARN / ERROR in the script that
#    GENERATES the YAML. --harden out.yaml shows the fixed form.
python3 $S/nrp_preflight.py jobs.yaml --prefix alice-

# 2. See what the scheduler would do. Creates nothing; exits 1 by design (--once).
python3 $S/nrp_scheduler.py --manifest jobs.yaml --prefix alice- --max-concurrent 50 --dry-run --once

# 3. Launch the batch in the background. It survives closing the terminal or
#    the agent session, but not a reboot: after a reboot, run the same command
#    again (finished Jobs are remembered; running ones are picked up).
mkdir -p .nrp
setsid nohup bash $S/run_wave.sh .nrp/jobs.log \
  --manifest jobs.yaml --prefix alice- --max-concurrent 50 \
  > /dev/null 2>&1 < /dev/null &

# 4. Health report: run it every hour while the batch runs (read-only).
python3 $S/nrp_status.py --prefix alice- --hours 1
```

Each line of the report has an action in `SKILL.md`, section "Periodic check".
Exit codes: 0 ok; 4 = your kubectl login expired (log in again in a terminal);
5 = NRP's login server is down (wait; a new login will not help).

Steer a running batch without restarting it (`<stem>` is the manifest's file
name without `.yaml`):

| to | do |
|---|---|
| change concurrency | `echo 30 > .nrp/<stem>.max` |
| pause / resume launching (running Jobs continue) | `touch .nrp/<stem>.pause` / `rm .nrp/<stem>.pause` |
| hold back some jobs until you release them | job-name regexes, one per line, in `.nrp/<stem>.hold`; `rm` it to release |
| rerun or retry jobs | write their names, one per line, to `.nrp/<stem>.requeue` |
| keep Jobs off a node | add `<hostname>  # <date> <reason>` to `~/.nrp/bad_nodes.txt` |
| add or change jobs | regenerate `jobs.yaml`; it is reloaded when it changes |
| stop the batch (Jobs already in the cluster keep running) | `kill "$(cat .nrp/<stem>.wave.pid)"` |

`.nrp/<stem>.tracker.json` is the only record of which Jobs finished (the
cluster deletes finished Jobs after 24 h). Never delete it while a batch runs.

### The hourly check without Claude Code

Most agents cannot wake themselves up. Run the report from cron and hand the
file to your agent when you return (or read it yourself):

```cron
17 * * * * cd /path/to/project && python3 ~/captain-nemo/skills/nautilus-scheduler/scripts/nrp_status.py --prefix alice- --hours 1 >> .nrp/status.log 2>&1
```

In Claude Code, `/loop 1h run the Nautilus health check for prefix alice-`
does the same with the agent acting on the result.

## Things to ask your agent

- "Preflight `jobs.yaml` for Nautilus with prefix `alice-` and fix the generator."
- "Launch `jobs.yaml` on Nautilus with prefix `alice-`, up to 50 at a time."
- "Run the Nautilus health check for `alice-` and act on it."
- "Why is NRP refusing my pods?" / "Which nodes should I exclude?"
- "How much CPU and memory should my pods request?" (uses measured usage)

## Sharing a namespace with other users

- **Finished Jobs do not use quota.** NRP's namespace quota counts pods that
  have not terminated: Running and Pending pods, plus pods stuck `Terminating` /
  `Unknown` on a lost node. Completed and Failed pods do not count, and finished
  Jobs disappear by themselves after 24 h. The scheduler does not delete
  finished Jobs early: it would gain no quota, and `kubectl logs` of a finished
  Job helps debugging.
- **Stuck pods do use quota.** The health report lists them ("stuck pods");
  force-delete them once their node is confirmed gone
  (`kubectl delete pod <name> --grace-period=0 --force`).
- **Pending pods use quota too.** With several people in one namespace, the sum
  of everyone's `--max-concurrent` should fit the namespace's pod quota (see
  `kubectl get resourcequota`). When the quota is full, the scheduler waits and
  retries; it does not fail your Jobs.
- **Use a different prefix per person** (and per batch, if two batches reuse job
  names): the prefix is what keeps the tools away from other people's Jobs.

## Safety

| tool | changes the cluster? |
|---|---|
| `nrp_preflight.py` | no (`--harden` writes a new local file; `--server-dry-run` creates nothing) |
| `nrp_status.py` | no |
| `nrp_scheduler.py` / `run_wave.sh` | creates and deletes **your prefixed** Jobs only; `--dry-run` changes nothing |

None of the tools delete PVC data, W&B runs or Hugging Face repos. Stop
processes by PID (the `.pid` files above), never with `pkill -f`.

## Files

```
skills/nautilus-scheduler/
  SKILL.md        instructions for the agent: workflow, what to do with each report line
  reference.md    how NRP scores pods, bad-node signatures, request sizing, operations
  scripts/        nrp_preflight.py  nrp_scheduler.py + run_wave.sh  nrp_status.py  nrp_common.py
  tests/          offline tests against a simulated cluster
```

## Status

Version 0.2.0.

- Offline tests pass: `python3 skills/nautilus-scheduler/tests/test_scheduler.py`
  (13 scenarios against a simulated cluster) and `.../tests/test_common.py`.
- The preflight, the health report and the scheduler's dry run have been run on
  the real cluster. The scheduler has not yet driven a real batch end to end;
  the rules it follows come from ~700 GPU Jobs run with an earlier scheduler.
- NRP's scoring was matched to its Violations page on 2026-09-29. If NRP changes
  its policy, re-check the thresholds in `scripts/nrp_common.py`.

## License

MIT -- see [LICENSE](LICENSE). Use it, change it, share it; keep the copyright notice.
