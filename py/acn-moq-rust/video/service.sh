#!/usr/bin/env bash
set -euo pipefail

VIDEO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${VIDEO_DIR}/.." && pwd)"
RUN_DIR="${ROOT_DIR}/.video_test"
PID_FILE="${RUN_DIR}/video_test.pid"
LOG_FILE="${RUN_DIR}/video_test.log"

RELAY_BIN="${MOQ_OFFICIAL_RELAY_BIN:-/home/acn/zqm/test/moq-rust/moq/target/release/moq-relay}"
MOQ_CLI_BIN="${MOQ_CLI_BIN:-/home/acn/zqm/test/moq-rust/moq/target/release/moq-cli}"
RELAY_HOST="${RELAY_HOST:-0.0.0.0}"
RELAY_PORT="${RELAY_PORT:-9007}"
WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-9008}"
CERT_HOST="${CERT_HOST:-101.245.78.174}"
SUBSCRIBER="${SUBSCRIBER:-rust-avc3}"
VIDEO_NAME="${VIDEO_NAME:-camera}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"
UVICORN_LOG_LEVEL="${UVICORN_LOG_LEVEL:-warning}"

mkdir -p "${RUN_DIR}"

usage() {
  cat <<EOF
Usage: $0 {start|stop|restart|status|logs}

Environment overrides:
  MOQ_OFFICIAL_RELAY_BIN=${RELAY_BIN}
  MOQ_CLI_BIN=${MOQ_CLI_BIN}
  RELAY_HOST=${RELAY_HOST}
  RELAY_PORT=${RELAY_PORT}
  WEB_HOST=${WEB_HOST}
  WEB_PORT=${WEB_PORT}
  CERT_HOST=${CERT_HOST}
  VIDEO_NAME=${VIDEO_NAME}

Default endpoints:
  relay:  http://${CERT_HOST}:${RELAY_PORT}/
  viewer: https://${CERT_HOST}:${WEB_PORT}/

Examples:
  $0 start
  $0 status
  $0 logs
  $0 stop
EOF
}

is_running() {
  if [[ ! -f "${PID_FILE}" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "${PID_FILE}")"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

require_file() {
  local path="$1"
  local label="$2"
  if [[ ! -x "${path}" ]]; then
    echo "ERROR: ${label} is not executable: ${path}" >&2
    exit 1
  fi
}

check_port_free() {
  local port="$1"
  local label="$2"
  if ss -ltn "( sport = :${port} )" 2>/dev/null | tail -n +2 | grep -q .; then
    echo "ERROR: TCP ${label} port ${port} is already in use" >&2
    ss -ltnp 2>/dev/null | grep ":${port}" || true
    exit 1
  fi
  if ss -lun "( sport = :${port} )" 2>/dev/null | tail -n +2 | grep -q .; then
    echo "ERROR: UDP ${label} port ${port} is already in use" >&2
    ss -lunp 2>/dev/null | grep ":${port}" || true
    exit 1
  fi
}

start_service() {
  if is_running; then
    echo "video test service is already running: pid=$(cat "${PID_FILE}")"
    return 0
  fi

  require_file "${RELAY_BIN}" "moq-relay"
  require_file "${MOQ_CLI_BIN}" "moq-cli"
  check_port_free "${RELAY_PORT}" "relay"
  check_port_free "${WEB_PORT}" "viewer"

  : > "${LOG_FILE}"

  (
    cd "${ROOT_DIR}"
    exec setsid env MOQ_OFFICIAL_RELAY_BIN="${RELAY_BIN}" \
      python3 video/moq_live_video_viewer.py \
        --subscriber "${SUBSCRIBER}" \
        --moq-cli-bin "${MOQ_CLI_BIN}" \
        --relay-host "${RELAY_HOST}" \
        --relay-port "${RELAY_PORT}" \
        --web-host "${WEB_HOST}" \
        --web-port "${WEB_PORT}" \
        --broadcast-name "${VIDEO_NAME}" \
        --cert-host "${CERT_HOST}" \
        --log-level "${LOG_LEVEL}" \
        --uvicorn-log-level "${UVICORN_LOG_LEVEL}"
  ) >> "${LOG_FILE}" 2>&1 &

  local pid=$!
  echo "${pid}" > "${PID_FILE}"

  sleep 1
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "ERROR: service failed to start. Recent logs:" >&2
    tail -80 "${LOG_FILE}" >&2 || true
    rm -f "${PID_FILE}"
    exit 1
  fi

  echo "started video test service: pid=${pid}"
  echo "relay:  http://${CERT_HOST}:${RELAY_PORT}/"
  echo "viewer: https://${CERT_HOST}:${WEB_PORT}/"
  echo "logs:   ${LOG_FILE}"
}

stop_service() {
  if ! is_running; then
    echo "video test service is not running"
    rm -f "${PID_FILE}"
    return 0
  fi

  local pid
  pid="$(cat "${PID_FILE}")"
  echo "stopping video test service: pid=${pid}"
  kill -TERM -- "-${pid}" 2>/dev/null || kill "${pid}" 2>/dev/null || true

  for _ in {1..20}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      rm -f "${PID_FILE}"
      echo "stopped"
      return 0
    fi
    sleep 0.2
  done

  echo "process did not exit after TERM; forcing stop"
  kill -KILL -- "-${pid}" 2>/dev/null || kill -9 "${pid}" 2>/dev/null || true
  rm -f "${PID_FILE}"
  echo "stopped"
}

status_service() {
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "video test service: running pid=${pid}"
    echo "relay:  http://${CERT_HOST}:${RELAY_PORT}/"
    echo "viewer: https://${CERT_HOST}:${WEB_PORT}/"
    echo
    echo "listening ports:"
    ss -ltnp 2>/dev/null | grep -E ":(${RELAY_PORT}|${WEB_PORT}|9001|9002|9003)\\b" || true
    ss -lunp 2>/dev/null | grep -E ":(${RELAY_PORT}|${WEB_PORT}|9001|9002|9003)\\b" || true
    echo
    echo "viewer status:"
    curl -k -s "https://127.0.0.1:${WEB_PORT}/api/status" || true
    echo
  else
    echo "video test service: stopped"
    rm -f "${PID_FILE}"
  fi
}

logs_service() {
  touch "${LOG_FILE}"
  tail -n 120 -f "${LOG_FILE}"
}

case "${1:-}" in
  start)
    start_service
    ;;
  stop)
    stop_service
    ;;
  restart)
    stop_service
    start_service
    ;;
  status)
    status_service
    ;;
  logs)
    logs_service
    ;;
  *)
    usage
    exit 1
    ;;
esac
