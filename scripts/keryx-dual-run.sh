#!/usr/bin/env bash
set -Eeuo pipefail

# Start, stop, and inspect a local Keryx daemon + relay pair without touching
# legacy/AgentAnycast daemons. Runtime state lives under ~/.hermes/.keryx.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

STATE_DIR=${KERYX_DUAL_RUN_STATE_DIR:-"${HOME}/.hermes/.keryx"}
LOG_DIR=${KERYX_DUAL_RUN_LOG_DIR:-"${STATE_DIR}/logs"}
RUN_DIR=${KERYX_DUAL_RUN_RUN_DIR:-"${STATE_DIR}/run"}
DATA_DIR=${HERMES_KERYX_DATA_DIR:-"${STATE_DIR}/data"}
if [[ -n "${HERMES_KERYX_RELAY_CONFIG:-}" ]]; then
  RELAY_CONFIG=${HERMES_KERYX_RELAY_CONFIG}
  RELAY_CONFIG_EXPLICIT=1
else
  RELAY_CONFIG=${STATE_DIR}/relay.json
  RELAY_CONFIG_EXPLICIT=0
fi

DAEMON_ADDR=${HERMES_KERYX_DAEMON_ADDR:-127.0.0.1:50051}
DAEMON_ENDPOINT=${HERMES_KERYX_DAEMON_ENDPOINT:-"http://${DAEMON_ADDR}"}
# Dual-run defaults intentionally avoid common legacy AgentAnycast relay ports
# (for example 4001/libp2p and 50052/health) while staying loopback-only.
RELAY_HEALTH_GRPC_ADDR=${HERMES_KERYX_RELAY_HEALTH_GRPC_ADDR:-127.0.0.1:51052}
RELAY_HTTP_ADDR=${HERMES_KERYX_RELAY_HEALTH_HTTP_ADDR:-127.0.0.1:18081}
RELAY_REGISTRY_ADDR=${HERMES_KERYX_REGISTRY_ENDPOINT:-127.0.0.1:51053}
RELAY_LISTEN_TCP=${HERMES_KERYX_RELAY_LISTEN_TCP:-/ip4/127.0.0.1/tcp/4101}
RELAY_LISTEN_QUIC=${HERMES_KERYX_RELAY_LISTEN_QUIC:-/ip4/127.0.0.1/udp/4101/quic-v1}

DAEMON_PID_FILE=${RUN_DIR}/keryxd.pid
RELAY_PID_FILE=${RUN_DIR}/keryx-relay.pid
DAEMON_LOG=${LOG_DIR}/keryxd.log
RELAY_LOG=${LOG_DIR}/keryx-relay.log
BUILD_LOG=${LOG_DIR}/build.log
STOP_TIMEOUT_SECONDS=${KERYX_DUAL_RUN_STOP_TIMEOUT_SECONDS:-20}
HEALTH_TIMEOUT_SECONDS=${KERYX_DUAL_RUN_HEALTH_TIMEOUT_SECONDS:-45}

usage() {
  cat <<EOF
Usage: $(basename "$0") [--start|--stop|--status|--help]

Default action is --start. Runtime files are kept outside the repo:
  logs: ${LOG_DIR}
  run:  ${RUN_DIR}
  data: ${DATA_DIR}

Environment overrides:
  HERMES_KERYX_DAEMON_ADDR             default: ${DAEMON_ADDR}
  HERMES_KERYX_DAEMON_ENDPOINT         default: ${DAEMON_ENDPOINT}
  HERMES_KERYX_RELAY_HEALTH_GRPC_ADDR  default: ${RELAY_HEALTH_GRPC_ADDR}
  HERMES_KERYX_REGISTRY_ENDPOINT       default: ${RELAY_REGISTRY_ADDR}
  HERMES_KERYX_RELAY_CONFIG            default: ${RELAY_CONFIG}
EOF
}

log() {
  printf '[keryx-dual-run] %s\n' "$*"
}

ensure_dirs() {
  mkdir -p "$LOG_DIR" "$RUN_DIR" "$DATA_DIR" "$(dirname -- "$RELAY_CONFIG")"
}

is_pid_running() {
  local pid=${1:-}
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

pid_from_file() {
  local pid_file=$1
  if [[ -f "$pid_file" ]]; then
    sed -n '1p' "$pid_file" 2>/dev/null || true
  fi
}

service_pid() {
  local pid_file=$1
  local pid
  pid=$(pid_from_file "$pid_file")
  if is_pid_running "$pid"; then
    printf '%s' "$pid"
  fi
}

strip_scheme() {
  local endpoint=$1
  endpoint=${endpoint#http://}
  endpoint=${endpoint#https://}
  endpoint=${endpoint#tcp://}
  printf '%s' "$endpoint"
}

host_port() {
  local addr=$1
  if [[ "$addr" == http://* || "$addr" == https://* || "$addr" == tcp://* ]]; then
    addr=$(strip_scheme "$addr")
  fi
  # This script's defaults are IPv4 loopback host:port. For bracketed IPv6 or
  # unix sockets, skip /dev/tcp probing and let the gRPC health checks decide.
  if [[ "$addr" == *']:'* || "$addr" == unix://* ]]; then
    return 1
  fi
  printf '%s %s\n' "${addr%:*}" "${addr##*:}"
}

tcp_listening() {
  local addr=$1 host port
  if ! read -r host port < <(host_port "$addr"); then
    return 1
  fi
  timeout 1 bash -c "</dev/tcp/${host}/${port}" >/dev/null 2>&1
}

ensure_relay_config() {
  if [[ "$RELAY_CONFIG_EXPLICIT" == 1 && -f "$RELAY_CONFIG" ]]; then
    return 0
  fi
  cat >"$RELAY_CONFIG" <<EOF
{
  "listen_addresses": ["${RELAY_LISTEN_TCP}", "${RELAY_LISTEN_QUIC}"],
  "bootstrap_peers": [],
  "enable_mdns": false,
  "max_circuits": 256,
  "max_reservations": 128,
  "max_reservations_per_peer": 4,
  "connection_timeout_ms": 30000,
  "use_ipv6": false,
  "health_grpc_bind": "${RELAY_HEALTH_GRPC_ADDR}",
  "health_http_bind": "${RELAY_HTTP_ADDR}",
  "registry_grpc_bind": "${RELAY_REGISTRY_ADDR}"
}
EOF
}

ensure_binaries() {
  local daemon_bin=${REPO_ROOT}/target/debug/keryxd
  local relay_bin=${REPO_ROOT}/target/debug/keryx-relay
  local cli_bin=${REPO_ROOT}/target/debug/keryx
  if [[ -x "$daemon_bin" && -x "$relay_bin" && -x "$cli_bin" ]]; then
    return 0
  fi
  log "building keryxd, keryx-relay, and keryx CLI (log: ${BUILD_LOG})"
  (
    cd "$REPO_ROOT"
    cargo build \
      -p keryx-daemon --bin keryxd \
      -p keryx-relay --bin keryx-relay \
      -p keryx-cli --bin keryx
  ) >"$BUILD_LOG" 2>&1
}

python_health() {
  local kind=$1 addr=$2
  PYTHONPATH="${REPO_ROOT}/sdk/python:${REPO_ROOT}/sdk/python/keryx/proto:${PYTHONPATH:-}" \
    KERYX_PROTO_PATH="${REPO_ROOT}/sdk/python/keryx/proto" \
    python3 - "$kind" "$addr" <<'PY'
import json
import os
import sys

kind = sys.argv[1]
addr = sys.argv[2]
addr = addr.removeprefix("http://").removeprefix("https://").removeprefix("tcp://")
sys.path.insert(0, os.environ["KERYX_PROTO_PATH"])

try:
    import grpc
    from hermes.keryx.v1 import daemon_pb2, daemon_pb2_grpc, relay_pb2, relay_pb2_grpc
except Exception as exc:  # pragma: no cover - operator fallback path
    print(f"python gRPC health unavailable: {exc}", file=sys.stderr)
    sys.exit(125)

try:
    with grpc.insecure_channel(addr) as channel:
        grpc.channel_ready_future(channel).result(timeout=3)
        if kind == "daemon":
            stub = daemon_pb2_grpc.KeryxDaemonStub(channel)
            ready = stub.Readiness(daemon_pb2.ReadinessRequest(), timeout=3)
            status = stub.Status(daemon_pb2.StatusRequest(), timeout=3)
            payload = {
                "ready": ready.ready,
                "not_ready_reasons": list(ready.not_ready_reasons),
                "status": status.status,
                "store_ready": status.store_ready,
                "data_dir": status.data_dir,
                "db_path": status.db_path,
            }
            print(json.dumps(payload, sort_keys=True))
            sys.exit(0 if ready.ready else 1)
        if kind == "relay":
            stub = relay_pb2_grpc.KeryxRelayStub(channel)
            health = stub.Health(relay_pb2.HealthRequest(), timeout=3)
            payload = {
                "healthy": health.healthy,
                "connected_peers": health.connected_peers,
                "registry_size": health.registry_size,
                "uptime_seconds": health.uptime_seconds,
                "transport_status": health.transport_status,
                "local_peer_id": health.local_peer_id,
            }
            print(json.dumps(payload, sort_keys=True))
            sys.exit(0 if health.healthy else 1)
        raise ValueError(f"unknown health kind: {kind}")
except Exception as exc:
    print(f"{kind} gRPC health failed at {addr}: {exc}", file=sys.stderr)
    sys.exit(1)
PY
}

grpcurl_health() {
  local kind=$1 addr=$2
  local proto rpc output
  case "$kind" in
    daemon)
      proto=hermes/keryx/v1/daemon.proto
      rpc=hermes.keryx.v1.KeryxDaemon/Readiness
      ;;
    relay)
      proto=hermes/keryx/v1/relay.proto
      rpc=hermes.keryx.v1.KeryxRelay/Health
      ;;
    *)
      return 2
      ;;
  esac
  output=$(grpcurl -plaintext -import-path "${REPO_ROOT}/proto" -proto "$proto" -d '{}' "$addr" "$rpc")
  printf '%s\n' "$output"
  case "$kind" in
    daemon) grep -Eq '"ready"[[:space:]]*:[[:space:]]*true' <<<"$output" ;;
    relay) grep -Eq '"healthy"[[:space:]]*:[[:space:]]*true' <<<"$output" ;;
  esac
}

cli_health() {
  local kind=$1 addr=$2 endpoint output
  endpoint="http://$(strip_scheme "$addr")"
  case "$kind" in
    daemon)
      output=$(HERMES_KERYX_DAEMON_ENDPOINT="$endpoint" "${REPO_ROOT}/target/debug/keryx" status)
      printf '%s\n' "$output"
      grep -Eq '^keryx status: ready$' <<<"$output"
      ;;
    relay)
      output=$(HERMES_KERYX_RELAY_HEALTH_ENDPOINT="$endpoint" "${REPO_ROOT}/target/debug/keryx" relay status)
      printf '%s\n' "$output"
      grep -Eq '^keryx relay status: healthy$' <<<"$output"
      ;;
    *)
      return 2
      ;;
  esac
}

health_check() {
  local kind=$1 addr=$2
  addr=$(strip_scheme "$addr")
  if command -v grpcurl >/dev/null 2>&1; then
    grpcurl_health "$kind" "$addr"
  elif [[ -x "${REPO_ROOT}/target/debug/keryx" ]]; then
    # The Keryx CLI uses gRPC internally (daemon Status RPC, relay Health RPC),
    # so this preserves gRPC validation without requiring grpcurl or Python grpcio.
    cli_health "$kind" "$addr"
  else
    python_health "$kind" "$addr"
  fi
}

wait_for_health() {
  local kind=$1 addr=$2 deadline now output rc
  deadline=$((SECONDS + HEALTH_TIMEOUT_SECONDS))
  rc=1
  while (( SECONDS < deadline )); do
    if output=$(health_check "$kind" "$addr" 2>&1); then
      printf '%s\n' "$output"
      return 0
    else
      rc=$?
    fi
    sleep 1
  done
  log "${kind} failed health check at ${addr} after ${HEALTH_TIMEOUT_SECONDS}s"
  if [[ -n "${output:-}" ]]; then
    printf '%s\n' "$output" >&2
  fi
  return "$rc"
}

start_service() {
  local name=$1 pid_file=$2 log_file=$3 addr=$4
  shift 4
  local pid
  pid=$(service_pid "$pid_file" || true)
  if [[ -n "$pid" ]]; then
    log "${name} already running with pid ${pid}"
    return 0
  fi
  if tcp_listening "$addr"; then
    log "${name} address ${addr} is already accepting TCP connections; not starting a second process"
    return 0
  fi
  : >"$log_file"
  log "starting ${name} (log: ${log_file})"
  (
    cd "$REPO_ROOT"
    exec "$@"
  ) >>"$log_file" 2>&1 &
  pid=$!
  printf '%s\n' "$pid" >"$pid_file"
}

start_all() {
  ensure_dirs
  ensure_relay_config
  ensure_binaries

  start_service \
    keryxd "$DAEMON_PID_FILE" "$DAEMON_LOG" "$DAEMON_ADDR" \
    env HERMES_KERYX_DATA_DIR="$DATA_DIR" \
      HERMES_KERYX_DAEMON_ADDR="$DAEMON_ADDR" \
      HERMES_KERYX_DAEMON_ENDPOINT="$DAEMON_ENDPOINT" \
      "${REPO_ROOT}/target/debug/keryxd"

  wait_for_health daemon "$DAEMON_ADDR" >/dev/null

  start_service \
    keryx-relay "$RELAY_PID_FILE" "$RELAY_LOG" "$RELAY_HEALTH_GRPC_ADDR" \
    env HERMES_KERYX_RELAY_CONFIG="$RELAY_CONFIG" \
      HERMES_KERYX_DAEMON_ENDPOINT="$DAEMON_ENDPOINT" \
      HERMES_KERYX_REGISTRY_ENDPOINT="$RELAY_REGISTRY_ADDR" \
      "${REPO_ROOT}/target/debug/keryx-relay"

  wait_for_health relay "$RELAY_HEALTH_GRPC_ADDR" >/dev/null
  status_all
}

stop_one() {
  local name=$1 pid_file=$2
  local pid waited=0
  pid=$(pid_from_file "$pid_file")
  if ! is_pid_running "$pid"; then
    log "${name}: not running"
    rm -f "$pid_file"
    return 0
  fi

  log "stopping ${name} pid ${pid} with SIGINT"
  kill -INT "$pid" 2>/dev/null || true
  while is_pid_running "$pid" && (( waited < STOP_TIMEOUT_SECONDS )); do
    sleep 1
    waited=$((waited + 1))
  done
  if is_pid_running "$pid"; then
    log "${name}: SIGINT timeout; sending SIGTERM"
    kill -TERM "$pid" 2>/dev/null || true
  fi
  while is_pid_running "$pid" && (( waited < STOP_TIMEOUT_SECONDS + 5 )); do
    sleep 1
    waited=$((waited + 1))
  done
  if is_pid_running "$pid"; then
    log "${name}: still running after SIGTERM"
    return 1
  fi
  rm -f "$pid_file"
  log "${name}: stopped"
}

stop_all() {
  ensure_dirs
  # Stop relay first so it stops accepting/routing before daemon shutdown.
  stop_one keryx-relay "$RELAY_PID_FILE"
  stop_one keryxd "$DAEMON_PID_FILE"
}

status_one() {
  local name=$1 pid_file=$2 kind=$3 addr=$4 log_file=$5
  local pid health_status=unhealthy
  pid=$(service_pid "$pid_file" || true)
  if [[ -n "$pid" ]]; then
    printf '%s: running pid=%s addr=%s log=%s\n' "$name" "$pid" "$addr" "$log_file"
  else
    printf '%s: no pid-file process addr=%s log=%s\n' "$name" "$addr" "$log_file"
  fi
  if health_check "$kind" "$addr" >/tmp/keryx-dual-run-health.$$ 2>&1; then
    health_status=healthy
  fi
  printf '%s health: %s\n' "$name" "$health_status"
  sed 's/^/  /' /tmp/keryx-dual-run-health.$$ || true
  rm -f /tmp/keryx-dual-run-health.$$
}

status_all() {
  ensure_dirs
  status_one keryxd "$DAEMON_PID_FILE" daemon "$DAEMON_ADDR" "$DAEMON_LOG"
  status_one keryx-relay "$RELAY_PID_FILE" relay "$RELAY_HEALTH_GRPC_ADDR" "$RELAY_LOG"
}

main() {
  local action=start
  case "${1:-}" in
    ""|--start) action=start ;;
    --stop) action=stop ;;
    --status) action=status ;;
    -h|--help) usage; return 0 ;;
    *) usage >&2; return 2 ;;
  esac

  case "$action" in
    start) start_all ;;
    stop) stop_all ;;
    status) status_all ;;
  esac
}

main "$@"
