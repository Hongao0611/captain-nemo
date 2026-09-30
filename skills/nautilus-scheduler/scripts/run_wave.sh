#!/bin/bash
# Keeps nrp_scheduler.py alive for one manifest until the wave is done.
#   exit 0 (all SUCCEEDED) / 3 (finished with failures) / 2 (config error): stop
#   anything else (crash, kill of the scheduler alone, network blip): respawn in 60 s
#
# Launch detached (survives the terminal / Claude session, not a machine restart):
#   setsid nohup bash run_wave.sh <log> --manifest jobs.yaml --prefix me- [...] > /dev/null 2>&1 < /dev/null &
# Stop the whole wave:     kill "$(cat <state-dir>/<stem>.wave.pid)"        (never pkill -f)
# Restart the scheduler:   kill "$(cat <state-dir>/<stem>.scheduler.pid)"   (respawns in 60 s,
#   picks up code changes). Flags live in this wrapper's argv: to change them,
#   stop the wave and relaunch. Manifest edits, <stem>.max, <stem>.pause and the
#   bad-nodes file are picked up live -- no restart needed.
LOG=$1; shift
DIR=$(cd "$(dirname "$0")" && pwd)
PY=${NRP_PYTHON:-$(command -v python3)}

# Mirror the scheduler's defaults to find the state dir and manifest stem.
STATE=.nrp; MANIFEST=
ARGS=("$@")
for ((i = 0; i < ${#ARGS[@]}; i++)); do
  case "${ARGS[i]}" in
    --state-dir) STATE=${ARGS[i+1]} ;;
    --state-dir=*) STATE=${ARGS[i]#*=} ;;
    --manifest) MANIFEST=${ARGS[i+1]} ;;
    --manifest=*) MANIFEST=${ARGS[i]#*=} ;;
  esac
done
[ -n "$MANIFEST" ] || { echo "run_wave.sh: --manifest is required" >&2; exit 2; }
STEM=$(basename "$MANIFEST"); STEM=${STEM%.*}
mkdir -p "$STATE"
PIDFILE="$STATE/$STEM.wave.pid"
# After a reboot a stale PID file may name an unrelated process: match the command line.
if [ -f "$PIDFILE" ] && tr '\0' ' ' < "/proc/$(cat "$PIDFILE")/cmdline" 2>/dev/null | grep -q run_wave.sh; then
  echo "run_wave.sh: a wave for $STEM is already running (PID $(cat "$PIDFILE"))" >&2; exit 2
fi
echo $$ > "$PIDFILE"

CHILD=
trap '[ -n "$CHILD" ] && kill "$CHILD" 2>/dev/null; wait "$CHILD" 2>/dev/null; rm -f "$PIDFILE"; echo "$(date -u +%FT%TZ) wave stopped by signal" >> "$LOG"; exit 143' TERM INT HUP

while true; do
  "$PY" -u "$DIR/nrp_scheduler.py" "$@" >> "$LOG" 2>&1 &
  CHILD=$!
  echo "$CHILD" > "$STATE/$STEM.scheduler.pid"
  wait "$CHILD"; rc=$?
  CHILD=
  case $rc in
    0) echo "$(date -u +%FT%TZ) wave complete" >> "$LOG"; rm -f "$PIDFILE"; exit 0 ;;
    3) echo "$(date -u +%FT%TZ) wave finished WITH FAILURES; see the tracker" >> "$LOG"; rm -f "$PIDFILE"; exit 3 ;;
    2) echo "$(date -u +%FT%TZ) configuration error; not respawning" >> "$LOG"; rm -f "$PIDFILE"; exit 2 ;;
  esac
  echo "$(date -u +%FT%TZ) scheduler exited $rc; respawning in 60 s" >> "$LOG"
  sleep "${NRP_RESPAWN_SLEEP:-60}" &
  CHILD=$!; wait "$CHILD"; CHILD=
done
