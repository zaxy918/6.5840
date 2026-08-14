#!/usr/bin/env bash

# Run MIT 6.5840 Lab 4 tests repeatedly with bounded concurrency.
# Raw go test output is stored per job, while runner output is written to one
# invocation summary log.

set -uo pipefail

usage() {
  cat <<'EOF'
Usage: ./test_lab4.sh <suite> <run-times> [max-parallel] [test-filter]

Suites:
  4A   Run kvraft1/rsm tests (default filter: 4A)
  4B   Run kvraft1 tests     (default filter: 4B)
  4C   Run kvraft1 tests     (default filter: 4C)
  all  Run 4A, 4B, and 4C as separate jobs

Examples:
  ./test_lab4.sh 4A 20 4
  ./test_lab4.sh 4B 10 3
  ./test_lab4.sh 4C 10 2 TestSnapshotRPC4C
  ./test_lab4.sh all 5 3

Environment:
  TEST_JOBS         Default maximum parallelism when the argument is omitted.
  TEST_KEEP_PASSED  Set to 1 to retain raw logs for successful jobs.
  TEST_LOG_DIR      Override the raw debug-log root directory.
  RAFT              Extra arguments passed to go test, matching the Makefile.
EOF
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 2
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

unique_path() {
  local requested=$1
  local stem=${requested%.*}
  local extension=""
  local candidate=$requested
  local suffix=2

  if [[ "$requested" == *.* ]]; then
    extension=.${requested##*.}
  else
    stem=$requested
  fi

  while [[ -e "$candidate" ]]; do
    candidate="${stem}_${suffix}${extension}"
    ((suffix++))
  done
  printf '%s\n' "$candidate"
}

sanitize_filter() {
  local value=$1
  value=${value//[^A-Za-z0-9._-]/_}
  value=${value:0:80}
  [[ -n "$value" ]] || value=filter
  printf '%s\n' "$value"
}

if (( $# < 2 || $# > 4 )); then
  usage >&2
  exit 2
fi

SUITE_INPUT=${1^^}
RUN_TIMES=$2

case "$SUITE_INPUT" in
  4A|4B|4C)
    SUITE_NAME=$SUITE_INPUT
    ;;
  ALL)
    SUITE_NAME=all
    ;;
  *)
    die "unsupported suite '$1'; expected 4A, 4B, 4C, or all"
    ;;
esac

is_positive_integer "$RUN_TIMES" || die "run-times must be a positive integer"

if (( $# >= 3 )); then
  MAX_PARALLEL=$3
elif [[ -n "${TEST_JOBS:-}" ]]; then
  MAX_PARALLEL=$TEST_JOBS
else
  MAX_PARALLEL=$(nproc 2>/dev/null || printf '1\n')
fi
is_positive_integer "$MAX_PARALLEL" || die "max-parallel must be a positive integer"

CUSTOM_FILTER=""
if (( $# == 4 )); then
  CUSTOM_FILTER=$4
  [[ "$SUITE_NAME" != all ]] || die "the all suite does not accept a custom test filter"
  [[ -n "$CUSTOM_FILTER" ]] || die "test-filter must not be empty"
fi

if [[ "$SUITE_NAME" == all ]]; then
  TOTAL_JOBS=$((RUN_TIMES * 3))
else
  TOTAL_JOBS=$RUN_TIMES
fi
if (( MAX_PARALLEL > TOTAL_JOBS )); then
  MAX_PARALLEL=$TOTAL_JOBS
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
INVOCATION_DATE=$(date '+%Y%m%d')
INVOCATION_TIMESTAMP=$(date '+%Y%m%d%H%M%S')

SUMMARY_DIR="$SCRIPT_DIR/test_log/lab4/$INVOCATION_DATE"
mkdir -p -- "$SUMMARY_DIR" || die "cannot create summary directory '$SUMMARY_DIR'"

if [[ -n "$CUSTOM_FILTER" ]]; then
  FILTER_LABEL=$(sanitize_filter "$CUSTOM_FILTER")
else
  FILTER_LABEL=default
fi

SUMMARY_BASENAME="${SUITE_NAME}_${RUN_TIMES}_${MAX_PARALLEL}_${FILTER_LABEL}_${INVOCATION_TIMESTAMP}.log"
SUMMARY_LOG=$(unique_path "$SUMMARY_DIR/$SUMMARY_BASENAME")
: > "$SUMMARY_LOG" || die "cannot create summary log '$SUMMARY_LOG'"
SUMMARY_LOG=$(cd -- "$(dirname -- "$SUMMARY_LOG")" && printf '%s/%s\n' "$PWD" "$(basename -- "$SUMMARY_LOG")")

# From this point on, runner output goes both to the terminal and summary log.
exec > >(tee -a "$SUMMARY_LOG") 2>&1

printf 'Summary log: %s\n' "$SUMMARY_LOG"
printf 'Invocation timestamp: %s\n' "$INVOCATION_TIMESTAMP"

if [[ -n "${TEST_LOG_DIR:-}" ]]; then
  RAW_LOG_ROOT=$TEST_LOG_DIR
else
  RAW_LOG_ROOT="$SCRIPT_DIR/debug_log/lab4"
fi
mkdir -p -- "$RAW_LOG_ROOT" || die "cannot create raw log root '$RAW_LOG_ROOT'"
RAW_LOG_ROOT=$(cd -- "$RAW_LOG_ROOT" && pwd -P)
printf 'Debug log root: %s\n' "$RAW_LOG_ROOT"

if command -v go >/dev/null 2>&1; then
  GO_BIN=$(command -v go)
elif [[ -x /usr/local/go/bin/go ]]; then
  GO_BIN=/usr/local/go/bin/go
else
  die "Go executable was not found"
fi
printf 'Go executable: %s\n' "$GO_BIN"

# Central suite registry. Add a suite by extending these four mappings.
declare -A SUITE_PACKAGE=(
  [4A]="$SCRIPT_DIR/kvraft1/rsm"
  [4B]="$SCRIPT_DIR/kvraft1"
  [4C]="$SCRIPT_DIR/kvraft1"
)
declare -A SUITE_DAEMON_SOURCE=(
  [4A]="$SCRIPT_DIR/main/rsm1d.go"
  [4B]="$SCRIPT_DIR/main/kvraft1d.go"
  [4C]="$SCRIPT_DIR/main/kvraft1d.go"
)
declare -A SUITE_DAEMON_BINARY=(
  [4A]="$SCRIPT_DIR/main/rsm1d"
  [4B]="$SCRIPT_DIR/main/kvraft1d"
  [4C]="$SCRIPT_DIR/main/kvraft1d"
)
declare -A SUITE_DEFAULT_FILTER=(
  [4A]=4A
  [4B]=4B
  [4C]=4C
)

if [[ "$SUITE_NAME" == all ]]; then
  SELECTED_SUITES=(4A 4B 4C)
else
  SELECTED_SUITES=("$SUITE_NAME")
fi

for suite in "${SELECTED_SUITES[@]}"; do
  [[ -d "${SUITE_PACKAGE[$suite]}" ]] || die "package directory does not exist: ${SUITE_PACKAGE[$suite]}"
  [[ -f "${SUITE_DAEMON_SOURCE[$suite]}" ]] || die "daemon source does not exist: ${SUITE_DAEMON_SOURCE[$suite]}"
done

# Build each distinct daemon once, even when several suites share it.
declare -A BUILT_DAEMONS=()
printf '\nBuilding required daemons...\n'
for suite in "${SELECTED_SUITES[@]}"; do
  daemon_binary=${SUITE_DAEMON_BINARY[$suite]}
  if [[ -n "${BUILT_DAEMONS[$daemon_binary]:-}" ]]; then
    continue
  fi
  daemon_source=${SUITE_DAEMON_SOURCE[$suite]}
  printf '  Building %s from %s\n' "$daemon_binary" "$daemon_source"
  if ! (cd -- "$SCRIPT_DIR" && "$GO_BIN" build -race -o "$daemon_binary" "${daemon_source#$SCRIPT_DIR/}"); then
    printf 'Error: daemon build failed: %s\n' "$daemon_source" >&2
    exit 1
  fi
  BUILT_DAEMONS[$daemon_binary]=1
done
printf 'Daemon build completed.\n\n'

RAFT_ARGS=()
if [[ -n "${RAFT:-}" ]]; then
  read -r -a RAFT_ARGS <<< "$RAFT"
  printf 'Additional go test arguments from RAFT: %s\n\n' "$RAFT"
fi

# Build an explicit queue so that max-parallel applies globally to all suites.
JOB_QUEUE_SUITE=()
JOB_QUEUE_ROUND=()
for suite in "${SELECTED_SUITES[@]}"; do
  for ((round = 1; round <= RUN_TIMES; round++)); do
    JOB_QUEUE_SUITE+=("$suite")
    JOB_QUEUE_ROUND+=("$round")
  done
done

declare -A JOB_SUITE=()
declare -A JOB_ROUND=()
declare -A JOB_LOG=()
ACTIVE_PIDS=()
ACTIVE_COUNT=0
NEXT_JOB=0
PASSED_JOBS=0
FAILED_JOBS=0
INTERRUPTED=0
FAILURE_LOGS=()

declare -A SUITE_PASSED_JOBS=([4A]=0 [4B]=0 [4C]=0)
declare -A SUITE_FAILED_JOBS=([4A]=0 [4B]=0 [4C]=0)

# Aggregates are keyed by "suite|test-name". Times are stored as microseconds
# to keep all shell arithmetic integral.
declare -A TEST_RUNS=()
declare -A TEST_PASS=()
declare -A TEST_FAIL=()
declare -A TEST_TIME_US=()
declare -A TEST_PEERS=()
declare -A TEST_RPCS=()
declare -A TEST_OPS=()
declare -A TEST_METRIC_RUNS=()
TEST_KEYS=()

remove_active_pid() {
  local target=$1
  local kept=()
  local pid
  for pid in "${ACTIVE_PIDS[@]}"; do
    [[ "$pid" == "$target" ]] || kept+=("$pid")
  done
  ACTIVE_PIDS=("${kept[@]}")
  ACTIVE_COUNT=${#ACTIVE_PIDS[@]}
}

parse_job_log() {
  local suite=$1
  local log_path=$2
  local parsed_suite parsed_test parsed_status parsed_time parsed_peers parsed_rpcs parsed_ops parsed_has_metrics
  local key

  while IFS=$'\t' read -r parsed_suite parsed_test parsed_status parsed_time parsed_peers parsed_rpcs parsed_ops parsed_has_metrics; do
    [[ -n "$parsed_test" ]] || continue
    key="${parsed_suite}|${parsed_test}"
    if [[ -z "${TEST_RUNS[$key]:-}" ]]; then
      TEST_KEYS+=("$key")
      TEST_RUNS[$key]=0
      TEST_PASS[$key]=0
      TEST_FAIL[$key]=0
      TEST_TIME_US[$key]=0
      TEST_PEERS[$key]=0
      TEST_RPCS[$key]=0
      TEST_OPS[$key]=0
      TEST_METRIC_RUNS[$key]=0
    fi

    TEST_RUNS[$key]=$((TEST_RUNS[$key] + 1))
    TEST_TIME_US[$key]=$((TEST_TIME_US[$key] + parsed_time))
    if [[ "$parsed_status" == PASS ]]; then
      TEST_PASS[$key]=$((TEST_PASS[$key] + 1))
    else
      TEST_FAIL[$key]=$((TEST_FAIL[$key] + 1))
    fi
    if (( parsed_has_metrics == 1 )); then
      TEST_PEERS[$key]=$parsed_peers
      TEST_RPCS[$key]=$((TEST_RPCS[$key] + parsed_rpcs))
      TEST_OPS[$key]=$((TEST_OPS[$key] + parsed_ops))
      TEST_METRIC_RUNS[$key]=$((TEST_METRIC_RUNS[$key] + 1))
    fi
  done < <(
    awk -v suite="$suite" '
      function reset_metrics() {
        peers = 0
        rpc_sum = 0
        ops_sum = 0
        metric_lines = 0
      }

      /^=== RUN[[:space:]]+/ {
        name = $3
        if (name !~ /\//) {
          current = name
          reset_metrics()
        }
        next
      }

      /Passed --/ && current != "" {
        phase = substr($0, index($0, "Passed --") + length("Passed --"))
        count = split(phase, fields, /[[:space:]]+/)
        phase_peers = -1
        phase_rpcs = -1
        phase_ops = -1
        number_count = 0
        for (i = 1; i <= count; i++) {
          if (fields[i] == "#peers" && i < count) {
            phase_peers = fields[i + 1] + 0
          } else if (fields[i] == "#RPCs" && i < count) {
            phase_rpcs = fields[i + 1] + 0
          } else if (fields[i] == "#Ops" && i < count) {
            phase_ops = fields[i + 1] + 0
          }

          numeric = fields[i]
          sub(/s$/, "", numeric)
          if (numeric ~ /^[0-9]+([.][0-9]+)?$/) {
            numbers[++number_count] = numeric
          }
        }

        if (phase_peers >= 0 && phase_rpcs >= 0 && phase_ops >= 0) {
          if (metric_lines == 0) {
            peers = phase_peers
          }
          rpc_sum += phase_rpcs
          ops_sum += phase_ops
          metric_lines++
        } else if (number_count >= 4) {
          # Compatibility with older tester output without metric labels.
          if (metric_lines == 0) {
            peers = numbers[2] + 0
          }
          rpc_sum += numbers[3] + 0
          ops_sum += numbers[4] + 0
          metric_lines++
        }
        delete numbers
        next
      }

      /^--- (PASS|FAIL):/ {
        status = $2
        sub(/:$/, "", status)
        test_name = $3
        if (current != "" && test_name == current) {
          duration = $NF
          gsub(/[()s]/, "", duration)
          duration_us = int((duration + 0) * 1000000 + 0.5)
          printf "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\n", \
            suite, current, status, duration_us, peers, rpc_sum, ops_sum, (metric_lines > 0)
          current = ""
          reset_metrics()
        }
      }
    ' "$log_path"
  )
}

start_job() {
  local suite=$1
  local round=$2
  local filter
  local job_date job_timestamp raw_dir raw_path
  local package_dir=${SUITE_PACKAGE[$suite]}

  if [[ -n "$CUSTOM_FILTER" ]]; then
    filter=$CUSTOM_FILTER
  else
    filter=${SUITE_DEFAULT_FILTER[$suite]}
  fi

  job_date=$(date '+%Y%m%d')
  job_timestamp=$(date '+%Y%m%d%H%M%S')
  raw_dir="$RAW_LOG_ROOT/$job_date"
  mkdir -p -- "$raw_dir" || die "cannot create raw log directory '$raw_dir'"
  raw_path=$(unique_path "$raw_dir/${suite}_round${round}_${job_timestamp}.log")

  printf '[START] suite=%s round=%d filter=%s log=%s\n' "$suite" "$round" "$filter" "$raw_path"
  (
    cd -- "$package_dir" || exit 125
    exec "$GO_BIN" test -v -race -run "$filter" "${RAFT_ARGS[@]}"
  ) > "$raw_path" 2>&1 &
  local pid=$!

  JOB_SUITE[$pid]=$suite
  JOB_ROUND[$pid]=$round
  JOB_LOG[$pid]=$raw_path
  ACTIVE_PIDS+=("$pid")
  ACTIVE_COUNT=${#ACTIVE_PIDS[@]}
}

finish_job() {
  local pid=$1
  local status
  local suite=${JOB_SUITE[$pid]}
  local round=${JOB_ROUND[$pid]}
  local raw_path=${JOB_LOG[$pid]}

  if wait "$pid"; then
    status=0
  else
    status=$?
  fi

  parse_job_log "$suite" "$raw_path"

  if (( status == 0 )); then
    ((PASSED_JOBS++))
    SUITE_PASSED_JOBS[$suite]=$((SUITE_PASSED_JOBS[$suite] + 1))
    printf '[PASS] suite=%s round=%d\n' "$suite" "$round"
    if [[ "${TEST_KEEP_PASSED:-0}" == 1 ]]; then
      printf '       retained log: %s\n' "$raw_path"
    else
      rm -f -- "$raw_path"
    fi
  else
    ((FAILED_JOBS++))
    SUITE_FAILED_JOBS[$suite]=$((SUITE_FAILED_JOBS[$suite] + 1))
    FAILURE_LOGS+=("$raw_path")
    printf '[FAIL] suite=%s round=%d exit=%d log=%s\n' "$suite" "$round" "$status" "$raw_path"
  fi

  unset 'JOB_SUITE[$pid]' 'JOB_ROUND[$pid]' 'JOB_LOG[$pid]'
  remove_active_pid "$pid"
}

reap_one_job() {
  local running padded_running pid
  while :; do
    running=$(jobs -pr)
    padded_running=" $(tr '\n' ' ' <<< "$running") "
    for pid in "${ACTIVE_PIDS[@]}"; do
      if [[ "$padded_running" != *" $pid "* ]]; then
        finish_job "$pid"
        return
      fi
    done
    sleep 0.05
  done
}

handle_interrupt() {
  local signal=$1
  local pid
  trap - INT TERM
  INTERRUPTED=1
  printf '\nReceived %s; terminating active test jobs...\n' "$signal"
  for pid in "${ACTIVE_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${ACTIVE_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
    printf 'Retained interrupted job log: %s\n' "${JOB_LOG[$pid]}"
  done
  printf 'Summary log: %s\n' "$SUMMARY_LOG"
  exit 130
}

trap 'handle_interrupt INT' INT
trap 'handle_interrupt TERM' TERM

printf 'Scheduling %d jobs with max-parallel=%d...\n' "$TOTAL_JOBS" "$MAX_PARALLEL"
while (( NEXT_JOB < TOTAL_JOBS || ACTIVE_COUNT > 0 )); do
  while (( NEXT_JOB < TOTAL_JOBS && ACTIVE_COUNT < MAX_PARALLEL )); do
    start_job "${JOB_QUEUE_SUITE[$NEXT_JOB]}" "${JOB_QUEUE_ROUND[$NEXT_JOB]}"
    ((NEXT_JOB++))
  done
  if (( ACTIVE_COUNT > 0 )); then
    reap_one_job
  fi
done

printf '\nTest case statistics\n'
printf '%-6s %-42s %10s %7s %10s %10s %6s %6s %6s\n' \
  Suite 'Test case' 'Avg time' Peers 'Avg RPCs' 'Avg Ops' Pass Fail Runs
printf '%-6s %-42s %10s %7s %10s %10s %6s %6s %6s\n' \
  '------' '------------------------------------------' '----------' '-------' '----------' '----------' '------' '------' '------'

declare -A SUITE_AVG_TIME_US_SUM=([4A]=0 [4B]=0 [4C]=0)
for key in "${TEST_KEYS[@]}"; do
  suite=${key%%|*}
  test_name=${key#*|}
  runs=${TEST_RUNS[$key]}
  metric_runs=${TEST_METRIC_RUNS[$key]}
  avg_time_us=$((TEST_TIME_US[$key] / runs))
  SUITE_AVG_TIME_US_SUM[$suite]=$((SUITE_AVG_TIME_US_SUM[$suite] + avg_time_us))
  avg_time=$(awk -v value="$avg_time_us" 'BEGIN { printf "%.3fs", value / 1000000 }')
  if (( metric_runs > 0 )); then
    avg_rpcs=$(awk -v total="${TEST_RPCS[$key]}" -v count="$metric_runs" 'BEGIN { printf "%.1f", total / count }')
    avg_ops=$(awk -v total="${TEST_OPS[$key]}" -v count="$metric_runs" 'BEGIN { printf "%.1f", total / count }')
    peers=${TEST_PEERS[$key]}
  else
    avg_rpcs='-'
    avg_ops='-'
    peers='-'
  fi
  printf '%-6s %-42s %10s %7s %10s %10s %6d %6d %6d\n' \
    "$suite" "$test_name" "$avg_time" "$peers" "$avg_rpcs" "$avg_ops" \
    "${TEST_PASS[$key]}" "${TEST_FAIL[$key]}" "$runs"
done

printf '\nInvocation summary\n'
printf '  Invocation timestamp: %s\n' "$INVOCATION_TIMESTAMP"
printf '  Summary log path: %s\n' "$SUMMARY_LOG"
printf '  Debug log root/date: %s/<job-start-date>\n' "$RAW_LOG_ROOT"
printf '  Total jobs: %d\n' "$TOTAL_JOBS"
printf '  Passed jobs: %d\n' "$PASSED_JOBS"
printf '  Failed jobs: %d\n' "$FAILED_JOBS"

for suite in "${SELECTED_SUITES[@]}"; do
  avg_sum=$(awk -v value="${SUITE_AVG_TIME_US_SUM[$suite]}" 'BEGIN { printf "%.3fs", value / 1000000 }')
  printf '  %s rounds: passed=%d failed=%d; average time sum=%s\n' \
    "$suite" "${SUITE_PASSED_JOBS[$suite]}" "${SUITE_FAILED_JOBS[$suite]}" "$avg_sum"
done

if (( ${#FAILURE_LOGS[@]} > 0 )); then
  printf '  Retained failure logs:\n'
  for raw_path in "${FAILURE_LOGS[@]}"; do
    printf '    %s\n' "$raw_path"
  done
else
  printf '  Retained failure logs: none\n'
fi

if (( FAILED_JOBS > 0 )); then
  exit 1
fi
exit 0
