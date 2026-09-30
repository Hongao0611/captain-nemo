# captain-nemo

A Claude Code skill for running large batches of Kubernetes Jobs on the
[Nautilus / NRP](https://nrp.ai) cluster without tripping its fair-use rules.

NRP refuses new pods while more than 4 of your pods "violate" its usage bands
(GPU under 40%, CPU or memory far from what you requested). Every new GPU pod
counts as a violator until its setup is done, so launching a big batch at once
locks you out for an hour or more. Bad nodes make it worse: a node that kills
pods in seconds looks free, so it swallows retry after retry.

captain-nemo keeps a batch running as wide as it can while staying under that
limit:

- **Paces launches by the violation budget.** It counts violating pods the same
  way NRP's Violations page does (checked against the page: same pods flagged)
  and launches only while fewer than 4 of yours violate.
- **Keeps jobs off bad nodes.** Exclusions live in one file
  (`~/.nrp/bad_nodes.txt`) and apply to every new Job, with no manifest
  regeneration and no restart.
- **Hourly health report.** Violators, failures grouped by node with
  "exclude / watch / recovered" advice, hung pods, Pending reasons, and whether
  NRP is accepting new pods right now.
- **Preflight for manifests.** Flags setup steps that can hang (apt, pip, git,
  dataset copies) and too few retries, writes a hardened copy, and recommends
  CPU/memory requests from your pods' measured usage.

It also carries the lessons behind these rules (`reference.md`): how NRP scores
pods, what bad nodes look like in logs, and how to size requests.

## Install

You need Claude Code, Python 3 with PyYAML, and `kubectl` logged in to Nautilus.

As a plugin (in Claude Code; you need read access to this repo):

```
/plugin marketplace add Hongao0611/captain-nemo
/plugin install captain-nemo@captain-nemo
```

Or copy the skill by hand:

```bash
git clone git@github.com:Hongao0611/captain-nemo.git
cp -r captain-nemo/skills/nautilus-scheduler ~/.claude/skills/
```

## Use

Ask Claude Code things like "schedule jobs.yaml on Nautilus with prefix
`me-`", "run the hourly check on my wave", or "why is NRP refusing my pods?".
The skill's `SKILL.md` tells Claude the workflow. The scripts also work on their
own:

```bash
S=skills/nautilus-scheduler/scripts
python3 $S/nrp_preflight.py jobs.yaml --prefix me-          # check a manifest
setsid nohup bash $S/run_wave.sh .nrp/wave.log \
  --manifest jobs.yaml --prefix me- --max-concurrent 50 \
  > /dev/null 2>&1 < /dev/null &                             # run a batch
python3 $S/nrp_status.py --prefix me- --hours 1              # health report
```

Use your own job-name prefix: the namespace is shared, and the tools refuse to
touch Jobs without it.

## Status

Version 0.1.0.

- The offline tests pass: 13 scheduler scenarios against a simulated cluster,
  plus unit tests (`python3 skills/nautilus-scheduler/tests/test_scheduler.py`,
  `.../test_common.py`).
- The report, preflight and scheduler dry-run have been run read-only on the
  real cluster.
- It has not yet driven a real batch end to end.
- NRP's scoring was matched to its Violations page on 2026-09-29. If NRP changes
  its policy, re-check the thresholds in `scripts/nrp_common.py`.

Private for now; no license yet.
