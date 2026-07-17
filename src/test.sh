#!/usr/bin/env bash

# Run repeated lab tests in parallel.
# Usage: ./test.sh <test-filter> <test-target> <run-times> [max-parallel]
# Example: ./test.sh 3A raft1 20 4

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
    echo "Usage: $0 <test-filter> <test-target> <run-times> [max-parallel]"
    echo "Example: $0 3A raft1 20 4"
    exit 1
fi

TEST_FILTER="$1"
TEST_TARGET="$2"
RUN_TIMES="$3"

if ! [[ "${RUN_TIMES}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: run-times must be a positive integer"
    exit 1
fi

if [ "$#" -eq 4 ]; then
    MAX_PARALLEL="$4"
elif [ -n "${TEST_JOBS:-}" ]; then
    MAX_PARALLEL="${TEST_JOBS}"
else
    MAX_PARALLEL="$(nproc 2>/dev/null || echo 4)"
fi

if ! [[ "${MAX_PARALLEL}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: max-parallel must be a positive integer"
    exit 1
fi

if [ "${MAX_PARALLEL}" -gt "${RUN_TIMES}" ]; then
    MAX_PARALLEL="${RUN_TIMES}"
fi

if command -v go >/dev/null 2>&1; then
    GO_BIN="$(command -v go)"
elif [ -x /usr/local/go/bin/go ]; then
    GO_BIN="/usr/local/go/bin/go"
else
    echo "Error: go executable was not found in PATH or /usr/local/go/bin"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${SCRIPT_DIR}/${TEST_TARGET}"
DAEMON_SOURCE="${SCRIPT_DIR}/main/${TEST_TARGET}d.go"
DAEMON_BINARY="${SCRIPT_DIR}/main/${TEST_TARGET}d"
LOG_DIR="${SCRIPT_DIR}/debug_log"

if [ ! -d "${TARGET_DIR}" ]; then
    echo "Error: test target directory does not exist: ${TARGET_DIR}"
    exit 1
fi

if [ ! -f "${DAEMON_SOURCE}" ]; then
    echo "Error: daemon source does not exist: ${DAEMON_SOURCE}"
    echo "This runner supports daemon-based targets such as raft1, kvsrv1, and kvraft1."
    exit 1
fi

mkdir -p "${LOG_DIR}"
rm -f -- "${LOG_DIR}"/*.log
echo "====================================="
echo "Test filter pattern: -run ${TEST_FILTER}"
echo "Test target: ${TEST_TARGET}"
echo "Total execution rounds: ${RUN_TIMES}"
echo "Maximum parallel jobs: ${MAX_PARALLEL}"
echo "Log storage directory: ${LOG_DIR}"
echo "====================================="

# Build once because parallel Make invocations would all overwrite the same
# main/${TEST_TARGET}d binary.
echo "Building ${TEST_TARGET} daemon once..."
(
    cd "${SCRIPT_DIR}"
    "${GO_BIN}" build -race -o "${DAEMON_BINARY}" "${DAEMON_SOURCE}"
) || {
    echo "Error: failed to build ${TEST_TARGET} daemon"
    exit 1
}

declare -a ACTIVE_PIDS=()
declare -a ACTIVE_ROUNDS=()
declare -a ACTIVE_LOGS=()
declare -a LOG_FILE_LIST=()
declare -A LOG_STATUS=()
FAILED_ROUNDS=0

wait_for_job() {
    local pid="$1"
    local round="$2"
    local log_file="$3"
    local status

    if wait "${pid}"; then
        status=0
        echo "[Round ${round}] PASS"
    else
        status=$?
        FAILED_ROUNDS=$((FAILED_ROUNDS + 1))
        echo "[Round ${round}] FAIL (exit ${status}), log: ${log_file}"
    fi

    LOG_STATUS["${log_file}"]="${status}"
}

for ((round = 1; round <= RUN_TIMES; round++)); do
    timestamp="$(date +"%Y%m%d_%H%M%S.%N")"
    log_file="${LOG_DIR}/debug_log_round${round}_${timestamp}.log"
    LOG_FILE_LIST+=("${log_file}")

    echo "[Round ${round}] Starting, log: ${log_file}"
    (
        cd "${TARGET_DIR}"
        "${GO_BIN}" test -v -race -run "${TEST_FILTER}"
    ) >"${log_file}" 2>&1 &

    ACTIVE_PIDS+=("$!")
    ACTIVE_ROUNDS+=("${round}")
    ACTIVE_LOGS+=("${log_file}")

    if [ "${#ACTIVE_PIDS[@]}" -ge "${MAX_PARALLEL}" ]; then
        wait_for_job "${ACTIVE_PIDS[0]}" "${ACTIVE_ROUNDS[0]}" "${ACTIVE_LOGS[0]}"
        ACTIVE_PIDS=("${ACTIVE_PIDS[@]:1}")
        ACTIVE_ROUNDS=("${ACTIVE_ROUNDS[@]:1}")
        ACTIVE_LOGS=("${ACTIVE_LOGS[@]:1}")
    fi
done

for i in "${!ACTIVE_PIDS[@]}"; do
    wait_for_job "${ACTIVE_PIDS[$i]}" "${ACTIVE_ROUNDS[$i]}" "${ACTIVE_LOGS[$i]}"
done

echo ""
echo "All test rounds finished. Removing logs from passed rounds..."

for log_file in "${LOG_FILE_LIST[@]}"; do
    if [ "${LOG_STATUS[${log_file}]:-1}" -eq 0 ]; then
        rm -f -- "${log_file}"
        echo "Removed passed log: ${log_file}"
    else
        echo "Reserved failed log: ${log_file}"
    fi
done

echo ""
echo "Passed rounds: $((RUN_TIMES - FAILED_ROUNDS))"
echo "Failed rounds: ${FAILED_ROUNDS}"

if [ "${FAILED_ROUNDS}" -ne 0 ]; then
    exit 1
fi
