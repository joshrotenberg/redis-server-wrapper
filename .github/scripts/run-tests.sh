#!/usr/bin/env bash
# Run `cargo test` for CI with the arguments given, isolated and instrumented
# for #170.
#
# The Linux test jobs die in "Run tests" with no log, and the step timeout
# never fires: the job runs on until the job timeout and GitHub reports it
# cancelled. That is what it looks like when the runner's own worker process
# is killed. The step shell shares a process group with the worker, and the
# crate escalates to `kill -9 -<pid>` (a whole process group) when a server
# does not stop, so a signal aimed at the wrong group could take the runner
# down.
#
# Three things here:
#
# - `setsid` puts the suite in a session and process group of its own, so no
#   group signal it sends can reach the runner.
# - A `kill` shim first on PATH records every signal the crate sends and what
#   each target was at the time, printed at the end.
# - `timeout` ends the suite before the step timeout, so a genuine hang still
#   leaves a log and a process listing instead of a cancelled job.
set -uo pipefail

budget="${TEST_BUDGET:-10m}"
log_dir="${RUNNER_TEMP:-/tmp}/rsw-ci"
mkdir -p "$log_dir/shim"
export KILL_LOG="$log_dir/kill.log"
: > "$KILL_LOG"

real_kill="$(command -v kill)"
if [ "$real_kill" = "kill" ]; then
  # `command -v` reports the shell builtin; the crate execs the binary.
  real_kill="$(type -P kill)"
fi

cat > "$log_dir/shim/kill" <<EOF
#!/usr/bin/env bash
{
  printf '%s kill %s (from pid %s: %s)\n' "\$(date +%T.%N | cut -c1-12)" "\$*" "\$PPID" "\$(ps -o args= -p "\$PPID" | cut -c1-100)"
  for a in "\$@"; do
    case "\$a" in
      -0|-9|-[A-Z]*) ;;
      -[0-9]*) printf '    group %s: %s\n' "\${a#-}" "\$(ps -eo pid=,pgid=,args= | awk -v g="\${a#-}" '\$2==g' | cut -c1-120 | paste -sd';' -)" ;;
      [0-9]*) printf '    pid %s: %s\n' "\$a" "\$(ps -o pid=,pgid=,args= -p "\$a" | cut -c1-120)" ;;
    esac
  done
} >> "\$KILL_LOG" 2>&1
exec "$real_kill" "\$@"
EOF
chmod +x "$log_dir/shim/kill"
export PATH="$log_dir/shim:$PATH"

echo "--- step shell and its ancestors (pid pgid session args) ---"
pid=$$
while [ "$pid" -gt 1 ]; do
  ps -o pid=,pgid=,sess=,args= -p "$pid" | cut -c1-120
  pid="$(ps -o ppid= -p "$pid" | tr -d ' ')"
done

runner=(cargo test "$@")
if command -v setsid >/dev/null 2>&1; then
  runner=(setsid --wait "${runner[@]}")
fi

status=0
if command -v timeout >/dev/null 2>&1; then
  timeout --kill-after=30s "$budget" "${runner[@]}" || status=$?
else
  # macOS images do not ship timeout(1); the step timeout still applies.
  "${runner[@]}" || status=$?
fi

if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
  echo "::error::cargo test exceeded ${budget} and was stopped"
  echo "--- processes at timeout ---"
  ps -eo pid,ppid,pgid,etime,stat,wchan:20,args | grep -vE ' (ps|grep) ' | cut -c1-200
fi

# Liveness probes (`kill -0`) and their target lines are dropped: they send
# nothing.
signals="$(awk '/ kill /{probe = ($0 ~ / kill -0 /)} !probe' "$KILL_LOG")"
echo "--- signals sent by the suite, kill -0 probes omitted ---"
echo "${signals:-none}"
echo "--- live targets that were not Redis ---"
printf '%s\n' "$signals" | grep -E '^    (pid|group) ' \
  | grep -vE 'redis-(server|sentinel)' | grep -vE ': *$' || echo "none"

exit "$status"
